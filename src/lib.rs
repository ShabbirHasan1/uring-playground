#![doc = include_str!("../README.md")]

mod common;
mod executor;
mod reactor;

pub use crate::{
    executor::{block_on, Executor},
    reactor::{OperationHandle, Reactor},
};
