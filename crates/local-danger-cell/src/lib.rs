use std::{
    cell::{Cell, UnsafeCell},
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

/// An runtime checked [`UnsafeCell`] alternative marked as `!Send` and
/// providing a minimal interface for accesses assumed as unique
#[derive(Default)]
pub struct DangerCell<T> {
    storage: UnsafeCell<T>,
    borrowed: Cell<bool>,
    marker: PhantomData<*mut T>,
}

impl<T> DangerCell<T> {
    pub const fn new(value: T) -> Self {
        Self {
            storage: UnsafeCell::new(value),
            borrowed: Cell::new(false),
            marker: PhantomData,
        }
    }

    /// Take mutable access to the stored value
    ///
    /// # Panics
    ///
    /// If the value is currently borrowed elsewhere
    pub fn assume_unique_access(&self) -> AccessGuard<'_, T> {
        assert!(!self.borrowed.replace(true), "already borrowed");

        AccessGuard(self)
    }
}

/// Scope guard for keeping track of the borrowed state
pub struct AccessGuard<'a, T>(&'a DangerCell<T>);

impl<'a, T> Deref for AccessGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.0.storage.get() }
    }
}

impl<'a, T> DerefMut for AccessGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.0.storage.get() }
    }
}

impl<'a, T> Drop for AccessGuard<'a, T> {
    fn drop(&mut self) {
        self.0.borrowed.set(false);
    }
}
