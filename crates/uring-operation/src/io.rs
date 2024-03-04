use std::{
    io::{Error, Result},
    mem::MaybeUninit,
    os::fd::{AsRawFd, BorrowedFd},
    pin::Pin,
};

use io_uring::{cqueue, opcode, squeue, types::Fd};

use crate::operation::Operation;

#[must_use]
pub struct Read<'a> {
    file: BorrowedFd<'a>,
    buffer: Vec<u8>,
}

impl<'a> Read<'a> {
    pub const fn new(file: BorrowedFd<'a>, buffer: Vec<u8>) -> Self {
        Self { file, buffer }
    }

    fn uninitialized_section_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        // SAFETY: we're correctly getting a slice of the uninitialzed part
        unsafe {
            std::slice::from_raw_parts_mut(
                self.buffer.as_mut_ptr().add(self.buffer.len()).cast(),
                self.buffer.capacity() - self.buffer.len(),
            )
        }
    }
}

// SAFETY: file bound to live long enough and buffer is owned
unsafe impl<'a> Operation for Read<'a> {
    type Output = Vec<u8>;

    fn build_submission(mut self: Pin<&mut Self>) -> squeue::Entry {
        opcode::Read::new(
            Fd(self.file.as_raw_fd()),
            self.uninitialized_section_mut().as_mut_ptr().cast(),
            u32::try_from(self.uninitialized_section_mut().len()).unwrap(),
        )
        .build()
    }

    unsafe fn process_completion(
        mut self: Pin<&mut Self>,
        entry: cqueue::Entry,
    ) -> Result<Self::Output> {
        let amount: usize = entry
            .result()
            .try_into()
            .map_err(|_| Error::from_raw_os_error(-entry.result()))?;

        // SAFETY: we trust the kernel to tell us how much was read into the buffer
        unsafe {
            let new = self.buffer.len() + amount;
            self.buffer.set_len(new);
        }

        Ok(std::mem::take(&mut self.buffer))
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

// SAFETY: file and buffer bound to live long enough
unsafe impl<'a> Operation for Write<'a> {
    type Output = usize;

    fn build_submission(self: Pin<&mut Self>) -> squeue::Entry {
        opcode::Write::new(
            Fd(self.file.as_raw_fd()),
            self.buffer.as_ptr(),
            u32::try_from(self.buffer.len()).unwrap(),
        )
        .build()
    }

    unsafe fn process_completion(
        self: Pin<&mut Self>,
        entry: cqueue::Entry,
    ) -> Result<Self::Output> {
        entry
            .result()
            .try_into()
            .map_err(|_| Error::from_raw_os_error(-entry.result()))
    }
}

#[must_use]
pub struct Splice<'a> {
    input: BorrowedFd<'a>,
    output: BorrowedFd<'a>,
    amount: u32,
}

impl<'a> Splice<'a> {
    pub const fn new(input: BorrowedFd<'a>, output: BorrowedFd<'a>, amount: u32) -> Self {
        Self {
            input,
            output,
            amount,
        }
    }
}

// SAFETY: files bound to live long enough
unsafe impl<'a> Operation for Splice<'a> {
    type Output = usize;

    fn build_submission(self: Pin<&mut Self>) -> squeue::Entry {
        opcode::Splice::new(
            Fd(self.input.as_raw_fd()),
            -1,
            Fd(self.output.as_raw_fd()),
            -1,
            self.amount,
        )
        .build()
    }

    unsafe fn process_completion(
        self: Pin<&mut Self>,
        entry: cqueue::Entry,
    ) -> Result<Self::Output> {
        entry
            .result()
            .try_into()
            .map_err(|_| Error::from_raw_os_error(-entry.result()))
    }
}
