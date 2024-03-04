use std::{
    io::{Error, Result},
    mem::MaybeUninit,
    os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd},
    pin::Pin,
};

use io_uring::{cqueue, opcode, squeue, types::Fd};
use socket2::SockAddr;

use crate::operation::Operation;

#[must_use]
pub struct Accept<'a> {
    socket: BorrowedFd<'a>,
    flags: libc::c_int,
    storage: MaybeUninit<libc::sockaddr_storage>,
    length: libc::socklen_t,
}

impl<'a> Accept<'a> {
    pub const fn new(socket: BorrowedFd<'a>) -> Self {
        Self {
            socket,
            flags: 0,
            storage: MaybeUninit::uninit(),
            // there's no way the platform's address storage overflows the specific length type
            // that's solely meant for representing it's length
            #[allow(clippy::cast_possible_truncation)]
            length: std::mem::size_of::<libc::sockaddr_storage>() as _,
        }
    }

    pub const fn non_blocking_socket(mut self) -> Self {
        self.flags |= libc::SOCK_NONBLOCK;
        self
    }

    pub const fn close_socket_on_exec(mut self) -> Self {
        self.flags |= libc::SOCK_CLOEXEC;
        self
    }
}

// SAFETY: socket bound to live long enough and the address data is owned
unsafe impl<'a> Operation for Accept<'a> {
    type Output = (OwnedFd, SockAddr);

    fn build_submission(mut self: Pin<&mut Self>) -> squeue::Entry {
        opcode::Accept::new(
            Fd(self.socket.as_raw_fd()),
            self.storage.as_mut_ptr().cast(),
            &mut self.length,
        )
        .flags(self.flags)
        .build()
    }

    unsafe fn process_completion(
        self: Pin<&mut Self>,
        entry: cqueue::Entry,
    ) -> Result<Self::Output> {
        if entry.result().is_negative() {
            return Err(Error::from_raw_os_error(-entry.result()));
        }

        // SAFETY: the kernel should have provided us valid values
        unsafe {
            Ok((
                OwnedFd::from_raw_fd(entry.result()),
                SockAddr::new(self.storage.assume_init(), self.length),
            ))
        }
    }
}

#[must_use]
pub struct Shutdown<'a> {
    socket: BorrowedFd<'a>,
    how: libc::c_int,
}

impl<'a> Shutdown<'a> {
    pub const fn new(socket: BorrowedFd<'a>) -> Self {
        Self {
            socket,
            how: libc::SHUT_RDWR,
        }
    }

    pub const fn read_part_only(mut self) -> Self {
        self.how = libc::SHUT_RD;
        self
    }

    pub const fn write_part_only(mut self) -> Self {
        self.how = libc::SHUT_WR;
        self
    }
}

// SAFETY: file and buffer bound to live long enough
unsafe impl<'a> Operation for Shutdown<'a> {
    type Output = ();

    fn build_submission(self: Pin<&mut Self>) -> squeue::Entry {
        opcode::Shutdown::new(Fd(self.socket.as_raw_fd()), self.how as _).build()
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
