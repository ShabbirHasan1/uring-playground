#![doc = include_str!("../README.md")]

mod adapter;
mod common;
mod executor;
mod reactor;

pub use crate::{
    adapter::PollIo,
    executor::{block_on, Executor},
    reactor::{OperationHandle, Reactor},
};
