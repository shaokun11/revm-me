use core::default::Default;

use crate::primitives::{ExecutionResult, EvmState, Env, SpecId};
use crate::journaled_state::ReadWriteSet;

#[derive(Clone)]
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
            gas: 21000,
            inspector,
            spec_id,
            env,
        }
    }

}

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