#![doc = include_str!("../README.md")]

mod adapter;
mod common;
mod executor;
mod operation;
mod reactor;

pub use crate::{
    adapter::PollIo,
    executor::{block_on, Executor},
    operation::{Accept, Completion, Oneshot, Read, Write},
    reactor::{Operation, Reactor},
};
