use std::{
    io::{Error, Result},
    os::fd::{IntoRawFd, OwnedFd},
    pin::Pin,
};

use io_uring::{cqueue, opcode, squeue, types::Fd};
use uring_reactor::OperationId;

use crate::operation::Operation;

#[must_use]
pub struct Raw {
    submission: squeue::Entry,
}

impl Raw {
    /// # Safety
    ///
    /// Submission parameters must remain valid for the duration of the
    /// operation
    pub const unsafe fn new(submission: squeue::Entry) -> Self {
        Self { submission }
    }
}

// SAFETY: constructor promises validity
unsafe impl Operation for Raw {
    type Output = cqueue::Entry;

    fn build_submission(self: Pin<&mut Self>) -> squeue::Entry {
        self.submission.clone()
    }

    unsafe fn process_completion(
        self: Pin<&mut Self>,
        entry: cqueue::Entry,
    ) -> Result<Self::Output> {
        Ok(entry)
    }
}

#[must_use]
pub struct Cancel {
    operation: OperationId,
}

impl Cancel {
    pub const fn new(operation: OperationId) -> Self {
        Self { operation }
    }
}

// SAFETY: no parameters that could get invalidated
unsafe impl Operation for Cancel {
    type Output = ();

    fn build_submission(self: Pin<&mut Self>) -> squeue::Entry {
        opcode::AsyncCancel::new(self.operation.as_raw().try_into().unwrap()).build()
    }

    unsafe fn process_completion(
        self: Pin<&mut Self>,
        entry: cqueue::Entry,
    ) -> Result<Self::Output> {
        if entry.result().is_negative() {
            return Err(Error::from_raw_os_error(-entry.result()));
        }

        Ok(())
    }
}

#[must_use]
pub struct Close {
    file: Option<OwnedFd>,
}

impl Close {
    pub const fn new(file: OwnedFd) -> Self {
        Self { file: Some(file) }
    }
}

// SAFETY: file is owned by us
unsafe impl Operation for Close {
    type Output = ();

    fn build_submission(mut self: Pin<&mut Self>) -> squeue::Entry {
        opcode::Close::new(Fd(self.file.take().unwrap().into_raw_fd())).build()
    }

    unsafe fn process_completion(
        self: Pin<&mut Self>,
        entry: cqueue::Entry,
    ) -> Result<Self::Output> {
        if entry.result().is_negative() {
            return Err(Error::from_raw_os_error(-entry.result()));
        }

        Ok(())
    }
}
