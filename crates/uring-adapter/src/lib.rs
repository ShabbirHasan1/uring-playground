use std::{
    io::{Error, ErrorKind, Result},
    mem::MaybeUninit,
    os::fd::{AsRawFd, OwnedFd},
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

use io_uring::{
    opcode::{AsyncCancel, Close, PollAdd, Shutdown, Write},
    types::Fd,
};
use uring_reactor::{Operation, Reactor};

/// Adapter to implement common IO traits backed by `io_uring`
pub struct PollIo {
    reactor: Rc<Reactor>,
    file: OwnedFd,
    read: Option<Operation>,
    write: Option<Operation>,
    shutdown: Option<Operation>,
    close: Option<Operation>,
}

impl PollIo {
    pub const fn new(reactor: Rc<Reactor>, file: OwnedFd) -> Self {
        Self {
            reactor,
            file,
            read: None,
            write: None,
            shutdown: None,
            close: None,
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

        if let Some(handle) = this.read {
            let entry = std::task::ready!(this.reactor.drive_operation(handle, context));
            this.read = None;

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
                unsafe {
                    this.reactor.submit_operation(
                        PollAdd::new(Fd(this.file.as_raw_fd()), libc::POLLIN as _).build(),
                        context,
                    )
                }
                .map_or_else(
                    |error| Poll::Ready(Err(error)),
                    |handle| {
                        this.read = Some(handle);
                        Poll::Pending
                    },
                )
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

        if let Some(handle) = this.write {
            let entry = std::task::ready!(this.reactor.drive_operation(handle, context));
            this.write = None;

            return Poll::Ready(
                entry
                    .result()
                    .try_into()
                    .map_err(|_| Error::from_raw_os_error(-entry.result())),
            );
        }

        // SAFETY: valid pointer with correct length and file bound to live long enough
        unsafe {
            this.reactor.submit_operation(
                Write::new(
                    Fd(this.file.as_raw_fd()),
                    buffer.as_ptr(),
                    buffer.len().try_into().unwrap(),
                )
                .build(),
                context,
            )
        }
        .map_or_else(
            |error| Poll::Ready(Err(error)),
            |handle| {
                this.write = Some(handle);
                Poll::Pending
            },
        )
    }

    /// Attempt to shutdown the socket
    ///
    /// # Panics
    ///
    /// If an internal operation handle overflows user data storage
    ///
    /// # Note
    ///
    /// This method cancels read and write operations if they haven't completed
    pub fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context) -> Poll<Result<()>> {
        let this = self.get_mut();
        let mut dummy = None;
        let remaining = if this.read.is_some() {
            &mut this.read
        } else if this.write.is_none() {
            &mut this.write
        } else {
            &mut dummy
        };

        match (remaining, this.shutdown) {
            // There is a operation that we need to cancel
            (Some(handle), None) => {
                // SAFETY: we don't set any parameters that can get invalidated
                unsafe {
                    this.reactor.submit_operation(
                        AsyncCancel::new(handle.as_raw().try_into().unwrap()).build(),
                        context,
                    )
                }
                .map_or_else(
                    |error| Poll::Ready(Err(error)),
                    |handle| {
                        this.shutdown = Some(handle);
                        Poll::Pending
                    },
                )
            }
            // We're waiting for a cancellation
            (slot @ Some(_), Some(handle)) => {
                let entry = std::task::ready!(this.reactor.drive_operation(handle, context));
                this.shutdown = None;
                *slot = None;

                if entry.result().is_negative() {
                    Poll::Ready(Err(Error::from_raw_os_error(-entry.result())))
                } else {
                    context.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            // Ready to shutdown
            (None, None) => {
                // SAFETY: file bound to live long enough
                unsafe {
                    this.reactor.submit_operation(
                        Shutdown::new(Fd(this.file.as_raw_fd()), libc::SHUT_WR).build(),
                        context,
                    )
                }
                .map_or_else(
                    |error| Poll::Ready(Err(error)),
                    |handle| {
                        this.shutdown = Some(handle);
                        Poll::Pending
                    },
                )
            }
            // We're waiting for the shutdown
            (None, Some(handle)) => {
                let entry = std::task::ready!(this.reactor.drive_operation(handle, context));
                this.shutdown = None;

                if entry.result().is_negative() {
                    Poll::Ready(Err(Error::from_raw_os_error(-entry.result())))
                } else {
                    Poll::Ready(Ok(()))
                }
            }
        }
    }

    /// Attempt to close the file
    pub fn poll_close(self: Pin<&mut Self>, context: &mut Context) -> Poll<Result<()>> {
        let this = self.get_mut();

        if let Some(handle) = this.close {
            let entry = std::task::ready!(this.reactor.drive_operation(handle, context));
            this.close = None;

            if entry.result().is_negative() {
                return Poll::Ready(Err(Error::from_raw_os_error(-entry.result())));
            }

            return Poll::Ready(Ok(()));
        }

        // SAFETY: file bound to live long enough
        unsafe {
            this.reactor
                .submit_operation(Close::new(Fd(this.file.as_raw_fd())).build(), context)
        }
        .map_or_else(
            |error| Poll::Ready(Err(error)),
            |handle| {
                this.close = Some(handle);
                Poll::Pending
            },
        )
    }
}

impl Drop for PollIo {
    fn drop(&mut self) {}
}

#[cfg(feature = "tokio-io")]
mod tokio_io {
    use std::{
        io::Result,
        pin::Pin,
        task::{Context, Poll},
    };

    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

    use crate::PollIo;

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

#[cfg(feature = "hyper-io")]
mod hyper_io {
    use std::{
        io::Result,
        pin::Pin,
        task::{Context, Poll},
    };

    use hyper::rt::{Read, ReadBufCursor, Write};

    use crate::PollIo;

    impl Read for PollIo {
        fn poll_read(
            self: Pin<&mut Self>,
            context: &mut Context,
            mut buffer: ReadBufCursor,
        ) -> Poll<Result<()>> {
            // SAFETY: doesn't uninitialize memory and correctly advances
            unsafe {
                self.poll_read(context, buffer.as_mut())
                    .map(|result| result.map(|amount| buffer.advance(amount)))
            }
        }
    }

    impl Write for PollIo {
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
