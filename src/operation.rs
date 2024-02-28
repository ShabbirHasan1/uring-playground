use std::{
    io::{Error, Result},
    mem::MaybeUninit,
    os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd},
    task::Poll,
};

use io_uring::{cqueue, opcode::Accept, squeue, types::Fd};
use socket2::SockAddr;

use crate::Reactor;

/// Submit an oneshot operation and wait for it to complete
///
/// # Safety
///
/// Submission parameters must remain valid for the duration of the operation
///
/// # Panics
///
/// If the operation wasn't actually oneshot and would still yield entries
///
/// # Errors
///
/// If submitting the operation fails
pub async unsafe fn oneshot(reactor: &Reactor, entry: squeue::Entry) -> Result<cqueue::Entry> {
    let mut operation = None;
    std::future::poll_fn(move |context| {
        if let Some(handle) = operation {
            let entry = std::task::ready!(reactor.drive_operation(handle, context));
            assert!(!cqueue::more(entry.flags()), "operation assumed as oneshot");

            return Poll::Ready(Ok(entry));
        }

        // SAFETY: caller promises validity
        unsafe { reactor.submit_operation(entry.clone(), context) }.map_or_else(
            |error| Poll::Ready(Err(error)),
            |handle| {
                operation = Some(handle);
                Poll::Pending
            },
        )
    })
    .await
}

/// Accept a request from a socket
///
/// # Errors
///
/// If submitting the operation fails or the operation results in an error
#[allow(clippy::missing_panics_doc)]
pub async fn accept(reactor: &Reactor, socket: BorrowedFd<'_>) -> Result<(OwnedFd, SockAddr)> {
    let mut storage = MaybeUninit::<libc::sockaddr_storage>::uninit();
    let mut length = std::mem::size_of_val(&storage).try_into().unwrap();

    // SAFETY: socket bound to live long enough and the address pointers are valid
    let result = unsafe {
        oneshot(
            reactor,
            Accept::new(
                Fd(socket.as_raw_fd()),
                storage.as_mut_ptr().cast(),
                &mut length,
            )
            .flags(libc::SOCK_NONBLOCK)
            .build(),
        )
        .await?
        .result()
    };

    if result.is_negative() {
        return Err(Error::from_raw_os_error(-result));
    }

    // SAFETY: the kernel should have provided us valid values
    unsafe {
        Ok((
            OwnedFd::from_raw_fd(result),
            SockAddr::new(storage.assume_init(), length),
        ))
    }
}
