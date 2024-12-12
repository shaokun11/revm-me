use crate::{
    builder::{EvmBuilder, HandlerStage, SetGenericStage},
    db::{Database, DatabaseCommit, EmptyDB},
    handler::Handler,
    interpreter::{
        CallInputs, CreateInputs, EOFCreateInputs, Host, InterpreterAction, SharedMemory,
    },
    primitives::{
        specification::SpecId, BlockEnv, CfgEnv, EVMError, EVMResult, EnvWithHandlerCfg,
        ExecutionResult, HandlerCfg, ResultAndState, TxEnv, TxKind, EOF_MAGIC_BYTES,
    },
    journaled_state::ReadWriteSet,
    Context, ContextWithHandlerCfg, Frame, FrameOrResult, FrameResult,
};
use core::fmt;
use std::{boxed::Box, vec::Vec, sync::{Arc, Mutex}};
use std::thread;

/// EVM call stack limit.
pub const CALL_STACK_LIMIT: u64 = 1024;

/// Parallel EVM instance supporting the execution of multiple transactions concurrently.
/// Contains multiple internal EVM contexts and handlers.
pub struct ParallelEvm<'a, EXT, DB: Database> {
    /// Shared context of execution, containing both EVM and external contexts.
    pub contexts: Vec<Arc<Mutex<Context<EXT, DB>>>>,
    /// Shared handlers that dictate the logic of EVM (or hardfork specification).
    pub handlers: Vec<Arc<Handler<'a, Context<EXT, DB>, EXT, DB>>>,
}

impl<EXT, DB> fmt::Debug for ParallelEvm<'_, EXT, DB>
where
    EXT: fmt::Debug,
    DB: Database + fmt::Debug,
    DB::Error: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParallelEvm")
            .field("contexts", &self.contexts.len())
            .field("handlers", &self.handlers.len())
            .finish_non_exhaustive()
    }
}

impl<'a, EXT, DB> ParallelEvm<'a, EXT, DB>
where
    EXT: fmt::Debug + Send + Sync,
    DB: Database + DatabaseCommit + fmt::Debug + Send + Sync,
    DB::Error: fmt::Debug,
{
    /// Creates a new ParallelEvm with the specified number of contexts and handlers.
    pub fn new(num_instances: usize) -> Self {
        let mut contexts = Vec::with_capacity(num_instances);
        let mut handlers = Vec::with_capacity(num_instances);
        
        for _ in 0..num_instances {
            let context = Arc::new(Mutex::new(Context::<EXT, DB>::new()));
            let handler = Arc::new(Handler::<_, _, _, _>::new());
            contexts.push(context);
            handlers.push(handler);
        }

        ParallelEvm { contexts, handlers }
    }

    /// 并行执行多条交易
    pub fn run_parallel(&self, frames: Vec<Frame>) -> Result<Vec<FrameResult>, EVMError<DB::Error>> {
        let num_threads = self.contexts.len();
        let frames_per_thread = frames.len() / num_threads;
        let frames_split = frames.chunks(frames_per_thread).map(|chunk| chunk.to_vec()).collect::<Vec<_>>();

        let mut handles = Vec::with_capacity(num_threads);
        let results = Arc::new(Mutex::new(Vec::new()));

        for (i, frame_batch) in frames_split.into_iter().enumerate() {
            let contexts = self.contexts.clone();
            let handlers = self.handlers.clone();
            let results = Arc::clone(&results);

            let handle = thread::spawn(move || {
                for frame in frame_batch {
                    let mut context_guard = contexts[i].lock().unwrap();
                    let handler = handlers[i].clone();
                    match handler.execute_frame(&frame, &mut SharedMemory::new(), &mut context_guard) {
                        Ok(result) => {
                            let mut res = results.lock().unwrap();
                            res.push(result);
                        },
                        Err(e) => {
                            // 这里可以根据需求处理错误，例如记录日志或继续
                            // 目前简单地忽略错误
                        },
                    }
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().map_err(|_| EVMError::ThreadJoinError)?;
        }

        let final_results = Arc::try_unwrap(results)
            .map_err(|_| EVMError::MutexPoisoned)?
            .into_inner()
            .map_err(|_| EVMError::MutexPoisoned)?;

        Ok(final_results)
    }
}

impl<'a, EXT, DB> ParallelEvm<'a, EXT, DB>
where
    EXT: fmt::Debug + Send + Sync,
    DB: Database + DatabaseCommit + fmt::Debug + Send + Sync,
    DB::Error: fmt::Debug,
{
    /// 创建新的 ParallelEvm 实例
    pub fn builder() -> ParallelEvmBuilder<'a, EXT, DB> {
        ParallelEvmBuilder::default()
    }
}

/// Builder for ParallelEvm
pub struct ParallelEvmBuilder<'a, EXT, DB: Database> {
    num_instances: usize,
    // Add other builder fields as needed
}

impl<'a, EXT, DB> Default for ParallelEvmBuilder<'a, EXT, DB>
where
    EXT: fmt::Debug,
    DB: Database,
{
    fn default() -> Self {
        Self {
            num_instances: 4, // default to 4 parallel instances
        }
    }
}

impl<'a, EXT, DB> ParallelEvmBuilder<'a, EXT, DB>
where
    EXT: fmt::Debug,
    DB: Database,
{
    /// 设置���行实例的数量
    pub fn with_num_instances(mut self, num: usize) -> Self {
        self.num_instances = num;
        self
    }

    /// 构建 ParallelEvm 实例
    pub fn build(self) -> ParallelEvm<'a, EXT, DB> {
        ParallelEvm::new(self.num_instances)
    }
}
