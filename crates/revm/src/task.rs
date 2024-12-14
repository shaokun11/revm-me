use crate::primitives::{Env, EvmState, ExecutionResult, SpecId};
use crate::journaled_state::ReadWriteSet;
use std::cmp::Ordering;

#[derive(Clone)]
pub struct Task<I> {
    pub env: Box<Env>,
    pub spec_id: SpecId,
    pub read_write_set: Option<ReadWriteSet>,
    pub tid: i32,
    pub sid: i32,
    pub gas: u64,
    pub state: Option<EvmState>,
    pub result: Option<ExecutionResult>,
    pub inspector: I,
}

impl<I> Task<I> {
    pub fn new(env: Box<Env>, tid: i32, sid: i32, spec_id: SpecId, inspector: I) -> Self {
        Task {
            gas: env.tx.gas_limit,
            read_write_set: None,
            tid,
            sid,
            env,
            spec_id,
            state: None,
            result: None,
            inspector,
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
