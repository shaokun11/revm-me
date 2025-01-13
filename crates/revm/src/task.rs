use core::default::Default;

use crate::primitives::{ExecutionResult, EvmState, Env, SpecId};
use crate::journaled_state::ReadWriteSet;
use std::cmp::Ordering;

pub struct Task<I> {
    pub tid: i32,
    pub sid: i32,
    pub gas: u64,
    pub inspector: Option<I>,
    pub spec_id: SpecId,
    pub env: Box<Env>,
}

impl<I> Task<I> {
    pub fn new(env: Box<Env>, tid: i32, sid: i32, spec_id: SpecId, inspector: Option<I>) -> Self {
        Self {
            tid,
            sid,
            gas: env.tx.gas_limit,
            inspector,
            spec_id,
            env,
        }
    }

}

pub struct SidOrderedTask<I>(pub Task<I>);
pub struct TidOrderedTask<I>(pub Task<I>);
pub struct GasOrderedTask<I>(pub Task<I>);

impl<I> Ord for SidOrderedTask<I> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.sid.cmp(&other.0.sid)
            .then_with(|| self.0.tid.cmp(&other.0.tid))
    }
}

impl<I> PartialOrd for SidOrderedTask<I> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<I> PartialEq for SidOrderedTask<I> {
    fn eq(&self, other: &Self) -> bool {
        self.0.sid == other.0.sid && self.0.tid == other.0.tid
    }
}

impl<I> Eq for SidOrderedTask<I> {}

impl<I> Ord for TidOrderedTask<I> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.tid.cmp(&other.0.tid)
    }
}

impl<I> PartialOrd for TidOrderedTask<I> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<I> PartialEq for TidOrderedTask<I> {
    fn eq(&self, other: &Self) -> bool {
        self.0.tid == other.0.tid
    }
}

impl<I> Eq for TidOrderedTask<I> {}

impl<I> Ord for GasOrderedTask<I> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.gas.cmp(&other.0.gas)
            .then_with(|| self.0.tid.cmp(&other.0.tid))
    }
}

impl<I> PartialOrd for GasOrderedTask<I> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<I> PartialEq for GasOrderedTask<I> {
    fn eq(&self, other: &Self) -> bool {
        self.0.gas == other.0.gas && self.0.tid == other.0.tid
    }
}

impl<I> Eq for GasOrderedTask<I> {}

#[derive(Clone)]
pub struct TaskResultItem<I> {
    pub gas: u64,
    pub result: Option<ExecutionResult>,
    pub inspector: Option<I>,
    pub read_write_set: Option<ReadWriteSet>,
    pub state: Option<EvmState>,
}

impl<I> Default for TaskResultItem<I> {
    fn default() -> Self {
        Self {
            gas: 0,
            result: None,
            inspector: None,
            read_write_set: None,
            state: None,
        }
    }
}