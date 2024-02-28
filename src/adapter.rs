use std::{
    io::{Error, ErrorKind, Result},
    mem::MaybeUninit,
    os::fd::{AsRawFd, OwnedFd},
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

use io_uring::{
    opcode::{Close, PollAdd, Shutdown, Write},
    types::Fd,
};

use crate::reactor::{Operation, Reactor};

/// Adapter to implement common IO traits backed by `io_uring`
pub struct PollIo {
    reactor: Rc<Reactor>,
    file: OwnedFd,
    operation: Option<Operation>,
}

impl PollIo {
    /// Create the IO adapter backed by normal
    pub const fn new(reactor: Rc<Reactor>, file: OwnedFd) -> Self {
        Self {
            reactor,
            file,
            operation: None,
        }
    }

    /// Attempt to read into the buffer
    ///
    /// # Note
    ///
    /// This method uses a native `read(2)` call with `IORING_OP_POLL_ADD` when
    /// encountering `EAGAIN` due to otherwise resulting in aliasing of the
    /// buffer reference
    pub fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context,
        buffer: &mut [MaybeUninit<u8>],
    ) -> Poll<Result<usize>> {
        let this = self.get_mut();

        if let Some(handle) = this.operation {
            let entry = std::task::ready!(this.reactor.drive_operation(handle, context));
            this.operation = None;

            if entry.result().is_negative() {
                return Poll::Ready(Err(Error::from_raw_os_error(-entry.result())));
            }
        }

        // SAFETY: valid pointer with correct length and file bound to live long enough
        let result = usize::try_from(unsafe {
            libc::read(
                this.file.as_raw_fd(),
                buffer.as_mut_ptr().cast(),
                buffer.len(),
            )
        })
        .map_err(|_| Error::last_os_error());

        match result {
            Ok(amount) => Poll::Ready(Ok(amount)),
            Err(error) if error.kind() == ErrorKind::Interrupted => {
                context.waker().wake_by_ref();
                Poll::Pending
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                // SAFETY: file bound to live long enough
                let result = unsafe {
                    this.reactor.submit_operation(
                        PollAdd::new(Fd(this.file.as_raw_fd()), libc::POLLIN as _).build(),
                        context,
                    )
                };

                match result {
                    Ok(handle) => {
                        this.operation = Some(handle);
                        Poll::Pending
                    }
                    Err(error) => Poll::Ready(Err(error)),
                }
            }
            Err(error) => Poll::Ready(Err(error)),
        }
    }

    /// Attempt to write the buffer's contents
    ///
    /// # Panics
    ///
    /// If the buffer overflows the [`u32`] length field used by `io_uring`
    pub fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context,
        buffer: &[u8],
    ) -> Poll<Result<usize>> {
        let this = self.get_mut();

        if let Some(handle) = this.operation {
            let entry = std::task::ready!(this.reactor.drive_operation(handle, context));
            this.operation = None;

            return Poll::Ready(
                entry
                    .result()
                    .try_into()
                    .map_err(|_| Error::from_raw_os_error(-entry.result())),
            );
        }

        // SAFETY: valid pointer with correct length and file bound to live long enough
        let result = unsafe {
            this.reactor.submit_operation(
                Write::new(
                    Fd(this.file.as_raw_fd()),
                    buffer.as_ptr(),
                    buffer.len().try_into().unwrap(),
                )
                .build(),
                context,
            )
        };

        match result {
            Ok(handle) => {
                this.operation = Some(handle);
                Poll::Pending
            }
            Err(error) => Poll::Ready(Err(error)),
        }
    }

    /// Attempt to shutdown the socket
    pub fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context) -> Poll<Result<()>> {
        let this = self.get_mut();

        if let Some(handle) = this.operation {
            let entry = std::task::ready!(this.reactor.drive_operation(handle, context));
            this.operation = None;

            if entry.result().is_negative() {
                return Poll::Ready(Err(Error::from_raw_os_error(-entry.result())));
            }

            return Poll::Ready(Ok(()));
        }

        // SAFETY: file bound to live long enough
        let result = unsafe {
            this.reactor.submit_operation(
                Shutdown::new(Fd(this.file.as_raw_fd()), libc::SHUT_WR).build(),
                context,
            )
        };

        match result {
            Ok(handle) => {
                this.operation = Some(handle);
                Poll::Pending
            }
            Err(error) => Poll::Ready(Err(error)),
        }
    }

    /// Attempt to close the file
    pub fn poll_close(self: Pin<&mut Self>, context: &mut Context) -> Poll<Result<()>> {
        let this = self.get_mut();

        if let Some(handle) = this.operation {
            let entry = std::task::ready!(this.reactor.drive_operation(handle, context));
            this.operation = None;

            if entry.result().is_negative() {
                return Poll::Ready(Err(Error::from_raw_os_error(-entry.result())));
            }

            return Poll::Ready(Ok(()));
        }

        // SAFETY: file bound to live long enough
        let result = unsafe {
            this.reactor
                .as_ref()
                .submit_operation(Close::new(Fd(this.file.as_raw_fd())).build(), context)
        };

        match result {
            Ok(handle) => {
                this.operation = Some(handle);
                Poll::Pending
            }
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}

#[cfg(feature = "tokio-io")]
mod tokio_io {
    use std::{
        io::Result,
        pin::Pin,
        task::{Context, Poll},
    };

    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

    use crate::adapter::PollIo;

    impl AsyncRead for PollIo {
        fn poll_read(
            self: Pin<&mut Self>,
            context: &mut Context,
            buffer: &mut ReadBuf,
        ) -> Poll<std::io::Result<()>> {
            // SAFETY: doesn't uninitialize memory and correctly returns bytes written
            unsafe {
                self.poll_read(context, buffer.unfilled_mut())
                    .map(|result| {
                        result.map(|amount| {
                            buffer.assume_init(amount);
                            buffer.advance(amount);
                        })
                    })
            }
        }
    }

    impl AsyncWrite for PollIo {
        fn poll_write(
            self: Pin<&mut Self>,
            context: &mut Context,
            buffer: &[u8],
        ) -> Poll<Result<usize>> {
            self.poll_write(context, buffer)
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context) -> Poll<Result<()>> {
            self.poll_shutdown(context)
        }
    }
}

#[cfg(feature = "futures-io")]
mod futures_io {
    use std::{
        io::Result,
        mem::MaybeUninit,
        os::fd::AsRawFd,
        pin::Pin,
        task::{Context, Poll},
    };

    use futures_io::{AsyncRead, AsyncWrite};

    use crate::adapter::PollIo;

    impl AsyncRead for PollIo {
        fn poll_read(
            self: Pin<&mut Self>,
            context: &mut Context,
            buffer: &mut [u8],
        ) -> Poll<Result<usize>> {
            // SAFETY: doesn't uninitialize memory
            self.poll_read(context, unsafe {
                &mut *(std::ptr::from_mut(buffer) as *mut [MaybeUninit<u8>])
            })
        }
    }

    impl AsyncWrite for PollIo {
        fn poll_write(
            self: Pin<&mut Self>,
            context: &mut Context,
            buffer: &[u8],
        ) -> Poll<Result<usize>> {
            self.poll_write(context, buffer)
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<()>> {
            todo!()
        }

        fn poll_close(self: Pin<&mut Self>, context: &mut Context) -> Poll<Result<()>> {
            self.poll_close(context)
        }
    }
}
