use crate::primitives::{Env, EvmState};
use crate::journaled_state::ReadWriteSet;
use std::cmp::Ordering;

pub struct Task {
    pub env: Box<Env>,
    pub read_write_set: Option<ReadWriteSet>,
    pub tid: i32,
    pub sid: i32,
    pub gas: u64,
    pub state: Option<EvmState>,
}

impl Task {
    pub fn new(env: Box<Env>, tid: i32, sid: i32) -> Self {
        Task {
            gas: env.tx.gas_limit,
            read_write_set: None,
            tid,
            sid,
            env,
            state: None,
        }
    }
}

pub struct SidOrderedTask(pub Task);
pub struct TidOrderedTask(pub Task);
pub struct GasOrderedTask(pub Task);

impl Ord for SidOrderedTask {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.sid.cmp(&other.0.sid)
            .then_with(|| self.0.tid.cmp(&other.0.tid))
    }
}

impl PartialOrd for SidOrderedTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for SidOrderedTask {
    fn eq(&self, other: &Self) -> bool {
        self.0.sid == other.0.sid && self.0.tid == other.0.tid
    }
}

impl Eq for SidOrderedTask {}

impl Ord for TidOrderedTask {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.tid.cmp(&other.0.tid)
    }
}

impl PartialOrd for TidOrderedTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for TidOrderedTask {
    fn eq(&self, other: &Self) -> bool {
        self.0.tid == other.0.tid
    }
}

impl Eq for TidOrderedTask {}

impl Ord for GasOrderedTask {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.gas.cmp(&other.0.gas)
            .then_with(|| self.0.tid.cmp(&other.0.tid))
    }
}

impl PartialOrd for GasOrderedTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for GasOrderedTask {
    fn eq(&self, other: &Self) -> bool {
        self.0.gas == other.0.gas && self.0.tid == other.0.tid
    }
}

impl Eq for GasOrderedTask {}
