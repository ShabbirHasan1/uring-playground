use std::{
    collections::VecDeque,
    io::{Error, ErrorKind::Other, Result},
    task::{Context, Poll, Waker},
};

use io_uring::{cqueue, squeue, IoUring};
use slab::Slab;

use crate::common::DangerCell;

/// Simple IO reactor for making `io_uring` operations
#[must_use]
pub struct Reactor {
    ring: DangerCell<IoUring>,
    operations: DangerCell<Slab<OperationState>>,
}

impl Reactor {
    /// Create a new reactor with the provided ring instance
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
        waker: Option<Waker>,
    ) -> Result<OperationHandle> {
        let index = self
            .operations
            .assume_unique_access()
            .insert(waker.map_or(OperationState::Submitted, OperationState::Waiting));

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

        Ok(OperationHandle::from_raw(index))
    }

    /// Poll for the result of an in-flight operation
    ///
    /// # Panics
    ///
    /// If the operation handle is invalid
    pub fn drive_operation(
        &self,
        operation: OperationHandle,
        context: &mut Context,
    ) -> Poll<cqueue::Entry> {
        let mut guard = self.operations.assume_unique_access();
        let slot = guard.get_mut(operation.as_raw()).unwrap();

        match slot {
            OperationState::Submitted => {
                *slot = OperationState::Waiting(context.waker().clone());

                Poll::Pending
            }
            OperationState::Waiting(waker) => {
                if !waker.will_wake(context.waker()) {
                    context.waker().clone_into(waker);
                }

                Poll::Pending
            }
            OperationState::Completed(entry) => {
                if !cqueue::more(entry.flags()) {
                    return Poll::Ready(guard.remove(operation.as_raw()).assume_as_completed());
                }

                Poll::Ready(
                    std::mem::replace(slot, OperationState::Waiting(context.waker().clone()))
                        .assume_as_completed(),
                )
            }
            OperationState::Unclaimed(entries) => {
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

                *slot = OperationState::Waiting(context.waker().clone());
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
                OperationState::Submitted => {
                    *slot = OperationState::Completed(entry);
                }
                OperationState::Waiting(_) => {
                    std::mem::replace(slot, OperationState::Completed(entry))
                        .assume_as_waiting()
                        .wake();
                }
                OperationState::Completed(_) => {
                    let previous = std::mem::replace(
                        slot,
                        OperationState::Unclaimed(VecDeque::with_capacity(2)),
                    );

                    let entries = slot.assume_as_mut_unclaimed();
                    entries.push_back(previous.assume_as_completed());
                    entries.push_back(entry);
                }
                OperationState::Unclaimed(entries) => entries.push_back(entry),
            }
        }

        Ok(())
    }
}

/// Strongly typed index referring to an [`OperationState`] instance
#[derive(Clone, Copy)]
#[must_use]
pub struct OperationHandle(usize);

impl OperationHandle {
    const fn from_raw(index: usize) -> Self {
        Self(index)
    }

    const fn as_raw(self) -> usize {
        self.0
    }
}

/// Internal state of an submitted operation
enum OperationState {
    Submitted,
    Waiting(Waker),
    Completed(cqueue::Entry),
    Unclaimed(VecDeque<cqueue::Entry>),
}

impl OperationState {
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
