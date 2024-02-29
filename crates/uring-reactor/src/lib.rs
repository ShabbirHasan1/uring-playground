use std::{
    collections::VecDeque,
    io::{Error, ErrorKind::Other, Result},
    task::{Context, Poll, Waker},
};

use io_uring::{cqueue, squeue, IoUring};
use local_danger_cell::DangerCell;
use slab::Slab;

/// Simple IO reactor for making `io_uring` operations
#[must_use]
pub struct Reactor {
    ring: DangerCell<IoUring>,
    operations: DangerCell<Slab<State>>,
}

impl Reactor {
    pub const fn new(ring: IoUring) -> Self {
        Self {
            ring: DangerCell::new(ring),
            operations: DangerCell::new(Slab::new()),
        }
    }

    /// Make an submission
    ///
    /// # Safety
    ///
    /// Submission parameters must remain valid for the duration of the
    /// operation
    ///
    /// # Panics
    ///
    /// If the created operation's index doesn't fit into user data
    ///
    /// # Errors
    ///
    /// If submitting the entry fails
    pub unsafe fn submit_operation(
        &self,
        entry: squeue::Entry,
        context: &mut Context,
    ) -> Result<Operation> {
        let index = self
            .operations
            .assume_unique_access()
            .insert(State::Waiting(context.waker().clone()));

        let entry = entry.user_data(index.try_into().unwrap());

        let mut guard = self.ring.assume_unique_access();
        let (submitter, mut submission, _) = guard.split();

        // SAFETY: the caller guarantees validity
        unsafe {
            submission.push(&entry).or_else(|_| {
                submitter.submit()?;
                submission
                    .push(&entry)
                    .map_err(|error| Error::new(Other, error))
            })?;
        }

        Ok(Operation::from_raw(index))
    }

    /// Poll for the result of an in-flight operation
    ///
    /// # Panics
    ///
    /// If the operation handle is invalid
    pub fn drive_operation(
        &self,
        operation: Operation,
        context: &mut Context,
    ) -> Poll<cqueue::Entry> {
        let mut guard = self.operations.assume_unique_access();
        let slot = guard.get_mut(operation.as_raw()).unwrap();

        match slot {
            State::Waiting(waker) => {
                if !waker.will_wake(context.waker()) {
                    context.waker().clone_into(waker);
                }

                Poll::Pending
            }
            State::Completed(entry) => {
                if !cqueue::more(entry.flags()) {
                    return Poll::Ready(guard.remove(operation.as_raw()).assume_as_completed());
                }

                Poll::Ready(
                    std::mem::replace(slot, State::Waiting(context.waker().clone()))
                        .assume_as_completed(),
                )
            }
            State::Unclaimed(entries) => {
                let Some(entry) = entries.pop_front() else {
                    return Poll::Pending;
                };

                if !entries.is_empty() {
                    context.waker().wake_by_ref();
                    return Poll::Ready(entry);
                }

                if !cqueue::more(entry.flags()) {
                    guard.remove(operation.as_raw());
                    return Poll::Ready(entry);
                }

                *slot = State::Waiting(context.waker().clone());
                Poll::Ready(entry)
            }
        }
    }

    /// Submit entries to the kernel and process completions, in turn waking
    /// up blocked futures
    ///
    /// # Panics
    ///
    /// If the user data value isn't a valid operation index
    ///
    /// # Errors
    ///
    /// If synchronizing with the kernel fails
    pub fn tick(&self) -> Result<()> {
        let mut guard = self.ring.assume_unique_access();
        let (submitter, submission, completion) = guard.split();

        if completion.is_empty() {
            submitter.submit_and_wait(1)?;
        } else if !submission.is_empty() {
            submitter.submit()?;
        }

        for entry in completion {
            let mut guard = self.operations.assume_unique_access();
            let slot = guard
                .get_mut(entry.user_data().try_into().unwrap())
                .unwrap();

            match slot {
                State::Waiting(_) => {
                    std::mem::replace(slot, State::Completed(entry))
                        .assume_as_waiting()
                        .wake();
                }
                State::Completed(_) => {
                    let previous =
                        std::mem::replace(slot, State::Unclaimed(VecDeque::with_capacity(2)));

                    let entries = slot.assume_as_mut_unclaimed();
                    entries.push_back(previous.assume_as_completed());
                    entries.push_back(entry);
                }
                State::Unclaimed(entries) => entries.push_back(entry),
            }
        }

        Ok(())
    }
}

/// Strongly typed index referring to a [`State`] instance
#[derive(Clone, Copy)]
#[must_use]
pub struct Operation(usize);

impl Operation {
    const fn from_raw(index: usize) -> Self {
        Self(index)
    }

    const fn as_raw(self) -> usize {
        self.0
    }
}

/// Internal state of an submitted operation
enum State {
    Waiting(Waker),
    Completed(cqueue::Entry),
    Unclaimed(VecDeque<cqueue::Entry>),
}

impl State {
    fn assume_as_waiting(self) -> Waker {
        if let Self::Waiting(waker) = self {
            return waker;
        }

        panic!("expected to be in the waiting state");
    }

    fn assume_as_completed(self) -> cqueue::Entry {
        if let Self::Completed(entry) = self {
            return entry;
        }

        panic!("expected to be in the completed state");
    }

    fn assume_as_mut_unclaimed(&mut self) -> &mut VecDeque<cqueue::Entry> {
        if let Self::Unclaimed(entries) = self {
            return entries;
        }

        panic!("expected to be in the unclaimed state");
    }
}
