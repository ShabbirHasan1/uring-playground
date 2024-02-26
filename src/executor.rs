use std::{
    collections::VecDeque,
    future::{Future, IntoFuture},
    task::{Context, Poll},
};

use async_task::{Runnable, Task};

use crate::common::DangerCell;

/// Simple poll loop for driving a future to completion concurretly with a
/// ticker function to act as an event loop
pub fn block_on<F, T, E>(future: F, ticker: T) -> Result<F::Output, E>
where
    F: IntoFuture,
    T: Fn() -> Result<(), E>,
{
    let waker = noop_waker::noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future.into_future());

    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return Ok(output);
        }

        ticker()?;
    }
}

/// Simple executor for spawning an unknown amount of background tasks
#[derive(Default)]
pub struct Executor {
    tasks: DangerCell<VecDeque<Runnable>>,
}

impl Executor {
    /// Create a new single threaded FIFO async executor
    pub fn new() -> Self {
        Self::default()
    }

    /// Spawn a task to be executed in the background
    pub fn spawn<'a, F>(&'a self, future: F) -> Task<F::Output>
    where
        F: IntoFuture,
        F::IntoFuture: 'a,
        F::Output: 'a,
    {
        // SAFETY: the future is tied to outlast the single threaded runtime
        let (runnable, task) = unsafe {
            async_task::spawn_unchecked(future.into_future(), |runnable| {
                self.tasks.assume_unique_access().push_back(runnable);
            })
        };

        runnable.run();
        task
    }

    /// Poll all currently scheduled background tasks
    pub fn tick(&self) {
        loop {
            let item = self.tasks.assume_unique_access().pop_front();
            match item {
                Some(runnable) => _ = runnable.run(),
                None => return,
            }
        }
    }
}
