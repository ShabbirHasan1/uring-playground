use std::{
    io::{Error, Result},
    mem::MaybeUninit,
    os::fd::{AsRawFd, BorrowedFd, FromRawFd, IntoRawFd, OwnedFd},
    task::Poll,
};

use io_uring::{
    cqueue,
    opcode::{Accept, Close, Read, Shutdown, Write},
    squeue,
    types::Fd,
};
use socket2::SockAddr;
use uring_reactor::Reactor;

/// Submit an oneshot operation and wait for it to complete
///
/// # Safety
///
/// Submission parameters must remain valid for the duration of the
/// operation
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

bitflags::bitflags! {
    /// Socket flags to set to an accepted client
    pub struct AcceptFlags: libc::c_int {
        const NON_BLOCKING = libc::SOCK_NONBLOCK;
        const CLOSE_ON_EXEC = libc::SOCK_CLOEXEC;
    }
}

/// Accept a request from a socket
///
/// # Panics
///
/// If the size of [`libc::sockaddr_storage`] somehow overflows
/// [`libc::socklen_t`]
///
/// # Errors
///
/// If submitting the operation fails or the operation results in an error
pub async fn accept(
    reactor: &Reactor,
    socket: BorrowedFd<'_>,
    flags: AcceptFlags,
) -> Result<(OwnedFd, SockAddr)> {
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
            .flags(flags.bits())
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

/// Read data from a file
///
/// # Panics
///
/// If the buffer's uninitialized capacity overflows the [`u32`] length used
/// by `io_uring`
///
/// # Errors
///
/// If submitting the operation fails or the operation results in an error
pub async fn read(reactor: &Reactor, file: BorrowedFd<'_>, mut buffer: Vec<u8>) -> Result<Vec<u8>> {
    // SAFETY: file bound to live long enough and buffer is owned by us
    let result = unsafe {
        oneshot(
            reactor,
            Read::new(
                Fd(file.as_raw_fd()),
                buffer.as_mut_ptr().add(buffer.len()).cast(),
                u32::try_from(buffer.capacity() - buffer.len()).unwrap(),
            )
            .build(),
        )
        .await?
        .result()
    };

    let amount: usize = result
        .try_into()
        .map_err(|_| Error::from_raw_os_error(-result))?;

    // SAFETY: we trust the kernel to tell us how much was read into the buffer
    unsafe {
        buffer.set_len(buffer.len() + amount);
    }

    Ok(buffer)
}

/// Write data into a file
///
/// # Panics
///
/// If the buffer's length overflows the [`u32`] used by `io_uring`
///
/// # Errors
///
/// If submitting the operation fails or the operation results in an error
pub async fn write(reactor: &Reactor, file: BorrowedFd<'_>, buffer: &[u8]) -> Result<usize> {
    // SAFETY: file and buffer bound to live long enough
    let result = unsafe {
        oneshot(
            reactor,
            Write::new(
                Fd(file.as_raw_fd()),
                buffer.as_ptr(),
                u32::try_from(buffer.len()).unwrap(),
            )
            .build(),
        )
        .await?
        .result()
    };

    result
        .try_into()
        .map_err(|_| Error::from_raw_os_error(-result))
}

/// Which part(s) of the full-duplex connection should be shut down
#[derive(Debug, Clone, Copy)]
#[repr(i32)]
pub enum ShutdownHow {
    Read = libc::SHUT_RD,
    Write = libc::SHUT_WR,
    Both = libc::SHUT_RDWR,
}

/// Shutdown a socket
///
/// # Errors
///
/// If submitting the operation fails or the operation results in an error
pub async fn shutdown(reactor: &Reactor, socket: BorrowedFd<'_>, how: ShutdownHow) -> Result<()> {
    // SAFETY: socket bound to live long enough
    let result = unsafe {
        oneshot(
            reactor,
            Shutdown::new(Fd(socket.as_raw_fd()), how as _).build(),
        )
        .await?
        .result()
    };

    if result.is_negative() {
        return Err(Error::from_raw_os_error(-result));
    }

    Ok(())
}

/// Close a file
///
/// # Errors
///
/// If submitting the operation fails or the operation results in an error
pub async fn close(reactor: &Reactor, file: OwnedFd) -> Result<()> {
    // SAFETY: file bound to live long enough
    let result = unsafe {
        oneshot(reactor, Close::new(Fd(file.into_raw_fd())).build())
            .await?
            .result()
    };

    if result.is_negative() {
        return Err(Error::from_raw_os_error(-result));
    }

    Ok(())
}
