use std::{
    future::Future,
    io::{Error, Result},
    os::fd::{AsRawFd, BorrowedFd},
    pin::Pin,
    task::{Context, Poll},
};

use io_uring::{cqueue, opcode, squeue, types};

use crate::{Operation, Reactor};

/// Trait representing an abstract oneshot operation
///
/// # Safety
///
/// Implementations must hold the state that's needed for the entry's
/// parameters to stay valid for the duration of the operation
pub unsafe trait Oneshot {
    type Output;

    /// Build a submission queue entry
    fn build_submission(&mut self) -> squeue::Entry;

    /// Build the output from a completion queue entry
    ///
    /// # Safety
    ///
    /// The entry must originate from the operation made using
    /// [`Oneshot::build_submission`]
    ///
    /// # Errors
    ///
    /// If the operation failed and returned an error code
    unsafe fn process_completion(&mut self, entry: cqueue::Entry) -> Result<Self::Output>;

    /// Create a future to wait for the operation to complete
    fn submit_and_wait(self, reactor: &Reactor) -> Completion<'_, Self>
    where
        Self: Sized,
    {
        Completion::new(reactor, self)
    }
}

#[must_use]
pub struct Write<'a> {
    file: BorrowedFd<'a>,
    buffer: &'a [u8],
}

impl<'a> Write<'a> {
    pub const fn new(file: BorrowedFd<'a>, buffer: &'a [u8]) -> Self {
        Self { file, buffer }
    }
}

/// SAFETY: lifetime bounds guarantee file and buffer validity
unsafe impl<'a> Oneshot for Write<'a> {
    type Output = usize;

    fn build_submission(&mut self) -> squeue::Entry {
        opcode::Write::new(
            types::Fd(self.file.as_raw_fd()),
            self.buffer.as_ptr(),
            self.buffer.len().try_into().unwrap(),
        )
        .build()
    }

    unsafe fn process_completion(&mut self, entry: cqueue::Entry) -> Result<Self::Output> {
        entry
            .result()
            .try_into()
            .map_err(|_| Error::from_raw_os_error(-entry.result()))
    }
}

#[must_use]
pub struct Read<'a> {
    file: BorrowedFd<'a>,
    buffer: Vec<u8>,
}

impl<'a> Read<'a> {
    pub const fn new(file: BorrowedFd<'a>, buffer: Vec<u8>) -> Self {
        Self { file, buffer }
    }
}

/// SAFETY: lifetime bounds guarantee file and buffer validity
unsafe impl<'a> Oneshot for Read<'a> {
    type Output = Vec<u8>;

    fn build_submission(&mut self) -> squeue::Entry {
        // SAFETY: the raw slice correctly points to the vector's uninitialized part
        let (uninitialized_start, remaining_capacity) = unsafe {
            (
                self.buffer.as_ptr().add(self.buffer.len()),
                self.buffer.capacity() - self.buffer.len(),
            )
        };

        opcode::Write::new(
            types::Fd(self.file.as_raw_fd()),
            uninitialized_start,
            remaining_capacity.try_into().unwrap(),
        )
        .build()
    }

    unsafe fn process_completion(&mut self, entry: cqueue::Entry) -> Result<Self::Output> {
        let amount_read: usize = entry
            .result()
            .try_into()
            .map_err(|_| Error::from_raw_os_error(-entry.result()))?;

        let mut buffer = std::mem::take(&mut self.buffer);

        // SAFETY: we only read up to capacity bytes and this amount got initialized
        unsafe { buffer.set_len(buffer.len() + amount_read) };

        Ok(buffer)
    }
}

/// Future to wait for an oneshot operation to complete
pub struct Completion<'a, O> {
    reactor: &'a Reactor,
    operation: O,
    handle: Option<Operation>,
}

impl<'a, O> Completion<'a, O> {
    const fn new(reactor: &'a Reactor, operation: O) -> Self {
        Self {
            reactor,
            operation,
            handle: None,
        }
    }
}

impl<'a, O> Future for Completion<'a, O>
where
    O: Oneshot + Unpin + 'a,
{
    type Output = Result<O::Output>;

    fn poll(self: Pin<&mut Self>, context: &mut Context) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Some(operation) = this.handle {
            let entry = std::task::ready!(this.reactor.drive_operation(operation, context));

            let output = unsafe { this.operation.process_completion(entry) };

            return Poll::Ready(output);
        }

        let result = unsafe {
            this.reactor
                .submit_operation(this.operation.build_submission(), context)
        };

        match result {
            Ok(operation) => {
                this.handle = Some(operation);
                Poll::Pending
            }
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}
