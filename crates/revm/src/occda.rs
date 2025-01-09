// OCCDA (Optimistic Concurrent Contract Deployment and Analysis) implementation
// This module provides parallel execution of EVM transactions with optimistic concurrency control
use crate::primitives::{ResultAndState, SpecId, Env};
use crate::access_tracker::AccessTracker;
use crate::journaled_state::AccessType;
use crate::task::{Task, TaskState, TaskResultItem};
use crate::dag::TaskDag;
use crate::evm::Evm;
use crate::db::{Database, DatabaseCommit, DatabaseRef, WrapDatabaseRef};
use crate::inspector::GetInspector;
use crate::inspector_handle_register;
use crate::handler::register::HandleRegister;
use std::sync::Arc;
use rayon::ThreadPool;
use rayon::prelude::*;
use parking_lot::RwLock;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use once_cell::sync::OnceCell;

// Main struct for handling parallel execution of EVM transactions
pub struct Occda {
    _dag: TaskDag,           // Dependency graph for tasks
    num_threads: usize,
}

// Global thread pool
static THREAD_POOL: OnceCell<ThreadPool> = OnceCell::new();

impl Occda {
    // Create a new OCCDA instance with specified number of threads
    pub fn new(num_threads: usize) -> Self {
        THREAD_POOL.get_or_init(|| {
            rayon::ThreadPoolBuilder::new()
                .num_threads(num_threads)
                .build()
                .unwrap()
        });

        Occda {
            _dag: TaskDag::new(),
            num_threads,
        }
    }

    // Initialize tasks with their dependencies from the graph
    // Returns a vector of tasks with updated sequence IDs (sid)
    pub fn init<I>(&mut self, tasks: Vec<Task<I>>, graph: Option<&TaskDag>) -> Vec<Task<I>> {
        let mut vec = Vec::with_capacity(tasks.len());
        for mut task in tasks {
            if let Some(g) = graph {
                // Find the maximum sid among dependencies
                let sid_max = g.get_dependencies(&task)
                    .into_iter()
                    .map(|node| g.get_task_tid(node).unwrap_or(-1))
                    .max()
                    .unwrap_or(-1);
                task.sid = sid_max;
            } else {
                task.sid = -1;
            }
            vec.push(task);
        }
        vec
    }

    // Build an EVM instance with the given configuration
    #[inline]
    fn build_evm<'a, DB: Database + DatabaseRef, I>(
        &self,
        db: &'a DB,
        inspector: I,
        spec_id: SpecId,
        e: &Box<Env>,
        register_handles_fn: HandleRegister<I, WrapDatabaseRef<&'a DB>>,
    ) -> Evm<'a, I, WrapDatabaseRef<&'a DB>> {
        Evm::builder()
            .with_ref_db(db)
            .modify_env(|env| env.clone_from(e))
            .with_external_context(inspector)
            .with_spec_id(spec_id)
            .append_handler_register(register_handles_fn)
            .build()
    }

    // Main execution function that processes transactions in parallel
    // Returns a vector of task results or an error
    pub fn main_with_db<'a, DB, I>(
        &mut self,
        h_tx: &'a mut Vec<Task<I>>,
        db: Arc<RwLock<DB>>,
        result_store: &mut Vec<TaskResultItem<I>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        DB: DatabaseRef + Database + DatabaseCommit + Send + Sync,
        I: Send + Sync + Clone + 'static + for<'db> GetInspector<WrapDatabaseRef<&'db DB>>,
    {
        // Initialize tracking variables
        let tx_size = h_tx.len();
        let mut exec_size = 0;
        let mut h_ready: Vec<usize> = Vec::new();          // Tasks ready for execution
        let mut h_exec: BinaryHeap<Reverse<usize>> = BinaryHeap::new(); // Tasks executing
        let mut h_commit: BinaryHeap<Reverse<usize>> = BinaryHeap::new(); // Tasks ready for commit
        let len = h_tx.len();
        let mut next = 0;

        // Create access tracker for conflict detection
        let mut access_tracker = AccessTracker::new();
        
        // Initialize result and state storage
        let mut state_store = Vec::with_capacity(len);
        for i in 0..len {
            state_store.push(TaskState::new());
            h_exec.push(Reverse(i));
        }

        let db_shared = db.clone();

        let store_ptr_usize = state_store.as_mut_ptr() as usize;
        let result_ptr = result_store.as_mut_ptr() as usize;

        // Main processing loop
        while next < len {
            // Find tasks ready for execution based on sequence ID
            while let Some(Reverse(task_idx)) = h_exec.pop() {
                if h_tx[task_idx].sid <= (next as i32 - 1) {
                    h_ready.push(task_idx);
                } else {
                    h_exec.push(Reverse(task_idx));
                    break;
                }
            }
            if h_ready.is_empty() {
                break;
            }

            // Sort ready tasks by gas (higher gas first)
            exec_size += h_ready.len();
            let ready_tasks = std::mem::take(&mut h_ready);
            
            if ready_tasks.len() == 1 {
                let task = &h_tx[ready_tasks[0]];
                let db_ref = db_shared.read();
                let inspector = task.inspector.clone().unwrap();
                let mut evm = self.build_evm(
                    &*db_ref,
                    inspector,
                    task.spec_id,
                    &task.env,
                    inspector_handle_register,
                );
                let mut task_result = TaskResultItem::new();    
                task_result.inspector = Some(evm.context.external.clone());
                task_result.gas = task.gas;
                
                let result = evm.transact();
                match result {
                    Ok(result_and_state) => {
                        let ResultAndState { state, result } = result_and_state;
                        state_store[task.tid as usize].state = Some(state);
                        task_result.result = Some(result);
                    }
                    Err(_) => {
                        task_result.gas = 0;
                    }
                }

                result_store[task.tid as usize] = task_result;
                h_commit.push(Reverse(ready_tasks[0]));
            } else {
                h_commit.extend(ready_tasks.iter().map(|&idx| Reverse(idx)));

                let chunk_size = ready_tasks.len() / self.num_threads + (ready_tasks.len() % self.num_threads > 0) as usize;
                // Execute tasks in parallel using thread pool
                THREAD_POOL.get().unwrap().install(|| {
                    ready_tasks
                        .par_chunks(chunk_size)
                        .for_each(|indexes| {

                            let db_ref = db_shared.read();
                            // Setup and execute individual task
                            for idx in indexes {
                                let task = &h_tx[*idx];
                                
                                let inspector = task.inspector.clone().unwrap();
                                let mut evm = Evm::builder()
                                .with_ref_db(&*db_ref)
                                .modify_env(|env| env.clone_from(&task.env))
                                .with_external_context(inspector)
                                .with_spec_id(task.spec_id)
                                .append_handler_register(inspector_handle_register)
                                .build();

                                let result = evm.transact();

                                // Process execution results
                                let mut task_result = TaskResultItem::new();
                                let mut task_state = TaskState::new();
                                task_result.inspector = Some(evm.context.external.clone());
                                task_result.gas = task.gas;

                                // Track read-write access
                                let mut read_write_set = evm.get_read_write_set();
                                read_write_set.add_write(task.env.tx.caller, AccessType::AccountInfo);
                                task_state.read_write_set = Some(read_write_set);

                                // Handle execution result
                                match result {
                                    Ok(result_and_state) => {
                                        let ResultAndState { state, result } = result_and_state;
                                        task_state.state = Some(state);
                                        task_result.result = Some(result);
                                    }
                                    Err(_) => {
                                        task_state.state = None;
                                        task_result.gas = 0;
                                    }
                                }

                                let store_raw_ptr = store_ptr_usize as *mut TaskState;
                                let result_raw_ptr = result_ptr as *mut TaskResultItem<I>;
                                unsafe {
                                    *store_raw_ptr.add(*idx) = task_state;
                                    *result_raw_ptr.add(*idx) = task_result;
                                }
                            }
                            
                        });
                });

            }

            // Begin commit phase
            // let start = std::time::Instant::now();
            let mut db_mut = db.write();
            
            // Process commits in order
            loop {
                let Some(Reverse(task_idx)) = h_commit.pop() else {
                    break;
                };

                // Ensure sequential commit order
                if h_tx[task_idx].tid != next as i32 {
                    h_commit.push(Reverse(task_idx));
                    break;
                }

                let task_state = &mut state_store[task_idx as usize];

                // Check for conflicts
                let read_write_set = task_state.read_write_set.as_ref().unwrap();
                let conflict = access_tracker.check_conflict_in_range(
                    &read_write_set.read_set,
                    h_tx[task_idx].sid + 1,
                    h_tx[task_idx].tid,
                );

                // Handle conflicts or commit changes
                if conflict.is_some() {
                    // Conflict detected: update sid and retry
                    h_tx[task_idx].sid = h_tx[task_idx].tid - 1;
                    // h_tx.push(value);
                    h_exec.push(Reverse(task_idx));
                } else {
                    // No conflict: commit changes and update access tracker
                    if let Some(state) = task_state.state.take() {
                        db_mut.commit(state);
                    }

                    access_tracker.record_write_set(
                        h_tx[task_idx].tid,
                        &read_write_set.write_set
                    );
                    next += 1;
                }
            }
        }

        // Calculate and log execution statistics
        let conflict_rate = ((exec_size - tx_size) as f64) / (tx_size as f64) * 100.0;
        println!(
            "finished execute tasks size: {} with conflict rate: {:.2}%",
            result_store.len(),
            conflict_rate
        );

        // Clean up resources asynchronously
        std::thread::spawn(move || {
            drop(access_tracker);
            drop(state_store);
            drop(h_exec);
            drop(h_commit);
            drop(h_ready);
        });

        Ok(())
    }
}
