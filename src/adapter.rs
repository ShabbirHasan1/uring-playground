use std::{
    io::{Error, ErrorKind, Read, Result, Write},
    os::fd::AsRawFd,
    pin::Pin,
    task::{Context, Poll},
};

use futures_io::{AsyncRead, AsyncWrite};
use io_uring::{opcode::PollAdd, types::Fd};

use crate::reactor::{Operation, Reactor};

/// Adapter to implement asynchronous IO traits backed by a standard libary
/// implementation and the polling facilities from `io_uring` when encountering
/// `EAGAIN`
pub struct PollIo<'a, I> {
    reactor: &'a Reactor,
    operation: Option<Operation>,
    io: I,
}

impl<'a, I> PollIo<'a, I> {
    /// Create the asynchronous IO adapter backed by the provided
    /// implementation
    pub const fn new(reactor: &'a Reactor, io: I) -> Self {
        Self {
            reactor,
            operation: None,
            io,
        }
    }
}

impl<'a, I> PollIo<'a, I>
where
    I: AsRawFd + Unpin + 'a,
{
    /// Register an internal poll operation
    fn register_poll(&mut self, flags: libc::c_short, context: &mut Context) -> Result<Operation> {
        let handle = unsafe {
            self.reactor.submit_operation(
                PollAdd::new(Fd(self.io.as_raw_fd()), flags.try_into().unwrap()).build(),
                context,
            )?
        };

        self.operation = Some(handle);
        Ok(handle)
    }
}

impl<'a, I> AsyncRead for PollIo<'a, I>
where
    I: Read + AsRawFd + Unpin + 'a,
{
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context,
        buffer: &mut [u8],
    ) -> Poll<Result<usize>> {
        let this = self.get_mut();

        if let Some(operation) = this.operation {
            std::task::ready!(this.reactor.drive_operation(operation, context));
            this.operation = None;
        }

        match this.io.read(buffer) {
            Ok(amount) => Poll::Ready(Ok(amount)),
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

impl<'a, I> AsyncWrite for PollIo<'a, I>
where
    I: Write + AsRawFd + Unpin + 'a,
{
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context,
        buffer: &[u8],
    ) -> Poll<Result<usize>> {
        let this = self.get_mut();

        if let Some(operation) = this.operation {
            let entry = std::task::ready!(this.reactor.drive_operation(operation, context));
            this.operation = None;

            if entry.result().is_negative() {
                return Poll::Ready(Err(Error::from_raw_os_error(-entry.result())));
            }
        }

        match this.io.write(buffer) {
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

    fn poll_close(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
}
