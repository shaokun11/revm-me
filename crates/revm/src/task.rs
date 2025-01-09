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

pub struct TaskResultItem<I> {
    pub gas: u64,
    pub result: Option<ExecutionResult>,
    
    pub inspector: Option<I>,
}

impl<I> TaskResultItem<I> {
    pub fn new() -> Self {
        Self {
            gas: 0,
            result: None,
            inspector: None,
        }
    }
}

pub struct TaskResultList<I> {
    pub items: Vec<TaskResultItem<I>>,
}

impl<I> TaskResultList<I> {
    pub fn new_with_capacity(capacity: usize) -> Self {
        let mut items = Vec::with_capacity(capacity);

        for _ in 0..capacity {
            items.push(TaskResultItem::new());
        }

        Self {
            items,
        }
    }

    pub fn set(&mut self, tid: i32, item: TaskResultItem<I>) {
        self.items[tid as usize] = item;
    }

    pub fn as_raw_mut_ptr(&mut self) -> *mut TaskResultItem<I> {
        self.items.as_mut_ptr() 
    }
    
    pub fn len(&self) -> usize {
        self.items.len()
    }
}

pub struct TaskState {
    pub read_write_set: Option<ReadWriteSet>,
    pub state: Option<EvmState>,
}

impl TaskState {
    pub fn new() -> Self {
        Self {
            read_write_set: None,
            state: None,
        }
    }
}
