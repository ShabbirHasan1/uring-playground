#![doc = include_str!("../README.md")]

mod adapter;
mod common;
mod executor;
mod reactor;

pub use crate::{
    adapter::PollAdapter,
    executor::{block_on, Executor},
    reactor::{OperationHandle, Reactor},
};
