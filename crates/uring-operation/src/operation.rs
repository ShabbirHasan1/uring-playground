use std::{
    future::Future,
    io::Result,
    pin::Pin,
    task::{ready, Context, Poll},
};

use futures_core::Stream;
use io_uring::{cqueue, squeue};
use uring_reactor::{OperationId, Reactor};

/// An abstract `io_uring` operation that can be submitted and completed
///
/// # Safety
///
/// The implementer must ensure that data used as operation parameters stays
/// valid for the duration of the operation
pub unsafe trait Operation: Sized {
    /// What this operation produces
    type Output;

    // Build an queue entry for submitting this operation
    fn build_submission(self: Pin<&mut Self>) -> squeue::Entry;

    /// Process a queue entry repersenting the operation's completion
    ///
    /// # Safety
    ///
    /// Caller must ensure that this completion corresponds to the
    /// submission from [`Operation::build_submission`]
    ///
    /// # Errors
    ///
    /// If the operation produced an error
    unsafe fn process_completion(
        self: Pin<&mut Self>,
        entry: cqueue::Entry,
    ) -> Result<Self::Output>;

    /// Create oneshot completion future
    fn submit_oneshot(self, reactor: &Reactor) -> Oneshot<'_, Self> {
        Oneshot::new(reactor, self)
    }

    /// Create multishot completion stream
    fn submit_multishot(self, reactor: &Reactor) -> Multishot<'_, Self> {
        Multishot::new(reactor, self)
    }
}

pin_project_lite::pin_project! {
    /// Future to wait for a operation that returns with a single completion
    pub struct Oneshot<'a, O> {
        reactor: &'a Reactor,
        #[pin]
        operation: O,
        handle: Option<OperationId>,
    }
}

impl<'a, O> Oneshot<'a, O> {
    const fn new(reactor: &'a Reactor, operation: O) -> Self {
        Self {
            reactor,
            operation,
            handle: None,
        }
    }

    pub const fn operation_handle(&self) -> Option<OperationId> {
        self.handle
    }
}

impl<'a, O> Future for Oneshot<'a, O>
where
    O: Operation,
{
    type Output = Result<O::Output>;

    fn poll(self: Pin<&mut Self>, context: &mut Context) -> Poll<Self::Output> {
        let this = self.project();

        if let Some(handle) = *this.handle {
            let entry = ready!(this.reactor.drive_operation(handle, context));
            assert!(!cqueue::more(entry.flags()), "operation assumed as oneshot");

            // SAFETY: we control the submission
            return Poll::Ready(unsafe { this.operation.process_completion(entry) });
        }

        let entry = this.operation.build_submission();

        // SAFETY: implementation promises validity
        match unsafe { this.reactor.submit_operation(entry, context) } {
            Ok(operation) => {
                *this.handle = Some(operation);
                Poll::Pending
            }
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}

pin_project_lite::pin_project! {
    /// Future to wait for a operation that returns with a single completion
    pub struct Multishot<'a, O> {
        reactor: &'a Reactor,
        #[pin]
        operation: O,
        handle: Option<OperationId>,
        finished: bool,
    }
}

impl<'a, O> Multishot<'a, O> {
    const fn new(reactor: &'a Reactor, operation: O) -> Self {
        Self {
            reactor,
            operation,
            handle: None,
            finished: false,
        }
    }

    pub const fn operation_handle(&self) -> Option<OperationId> {
        self.handle
    }
}

impl<'a, O> Stream for Multishot<'a, O>
where
    O: Operation,
{
    type Item = Result<O::Output>;

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.finished {
            (0, Some(0))
        } else {
            (1, None)
        }
    }

    fn poll_next(self: Pin<&mut Self>, context: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.project();

        if *this.finished {
            return Poll::Ready(None);
        }

        if let Some(handle) = *this.handle {
            let entry = ready!(this.reactor.drive_operation(handle, context));
            *this.finished = !cqueue::more(entry.flags());

            // SAFETY: we control the submission
            return Poll::Ready(Some(unsafe { this.operation.process_completion(entry) }));
        }

        let entry = this.operation.build_submission();

        // SAFETY: implementation promises validity
        match unsafe { this.reactor.submit_operation(entry, context) } {
            Ok(operation) => {
                *this.handle = Some(operation);
                Poll::Pending
            }
            Err(error) => Poll::Ready(Some(Err(error))),
        }
    }
}
