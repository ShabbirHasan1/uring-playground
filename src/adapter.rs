use std::{
    io::{Error, ErrorKind, Result},
    os::fd::{AsRawFd, BorrowedFd},
    pin::Pin,
    task::{Context, Poll},
};

use io_uring::{opcode::PollAdd, types::Fd};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::reactor::{Operation, Reactor};

/// Adapter to implement [`tokio::io`] traits backed by the typical syscalls
/// and polling facilities from `io_uring` when encountering `EAGAIN`
pub struct PollIo<'a> {
    reactor: &'a Reactor,
    file: BorrowedFd<'a>,
    poll: Option<Operation>,
}

impl<'a> PollIo<'a> {
    /// Create the IO adapter backed by normal
    pub const fn new(reactor: &'a Reactor, file: BorrowedFd<'a>) -> Self {
        Self {
            reactor,
            file,
            poll: None,
        }
    }
}

impl<'a> PollIo<'a> {
    /// Register an internal poll operation
    fn register_poll(&mut self, flags: libc::c_short, context: &mut Context) -> Result<Operation> {
        // SAFETY: file bound to live long enough, not technically unsound either
        let handle = unsafe {
            self.reactor.submit_operation(
                PollAdd::new(Fd(self.file.as_raw_fd()), flags.try_into().unwrap()).build(),
                context,
            )?
        };

        self.poll = Some(handle);
        Ok(handle)
    }
}

impl<'a> AsyncRead for PollIo<'a> {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context,
        buffer: &mut ReadBuf,
    ) -> Poll<Result<()>> {
        let this = self.get_mut();

        if let Some(operation) = this.poll {
            std::task::ready!(this.reactor.drive_operation(operation, context));
            this.poll = None;
        }

        // SAFETY: valid pointer with correct length and file bound to live long enough
        let result = usize::try_from(unsafe {
            libc::read(
                this.file.as_raw_fd(),
                buffer.unfilled_mut().as_mut_ptr().cast(),
                buffer.unfilled_mut().len(),
            )
        })
        .map_err(|_| Error::last_os_error());

        match result {
            Ok(amount) => {
                // SAFETY: we trust the kernel not to lie about the amount of bytes it wrote
                unsafe {
                    buffer.assume_init(amount);
                    buffer.advance(amount);
                }

                Poll::Ready(Ok(()))
            }
            Err(error) if error.kind() == ErrorKind::Interrupted => {
                context.waker().wake_by_ref();
                Poll::Pending
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                if let Err(error) = this.register_poll(libc::POLLIN, context) {
                    return Poll::Ready(Err(error));
                }

                Poll::Pending
            }

            Err(error) => Poll::Ready(Err(error)),
        }
    }
}

impl<'a> AsyncWrite for PollIo<'a> {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context,
        buffer: &[u8],
    ) -> Poll<Result<usize>> {
        let this = self.get_mut();

        if let Some(operation) = this.poll {
            let entry = std::task::ready!(this.reactor.drive_operation(operation, context));
            this.poll = None;

            if entry.result().is_negative() {
                return Poll::Ready(Err(Error::from_raw_os_error(-entry.result())));
            }
        }

        // SAFETY: valid pointer with correct length and file bound to live long enough
        let result = unsafe {
            usize::try_from(libc::write(
                this.file.as_raw_fd(),
                buffer.as_ptr().cast(),
                buffer.len(),
            ))
            .map_err(|_| Error::last_os_error())
        };

        match result {
            Ok(amount) => Poll::Ready(Ok(amount)),
            Err(error) if error.kind() == ErrorKind::Interrupted => {
                context.waker().wake_by_ref();
                Poll::Pending
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                if let Err(error) = this.register_poll(libc::POLLOUT, context) {
                    return Poll::Ready(Err(error));
                }

                Poll::Pending
            }
            Err(error) => Poll::Ready(Err(error)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
}
