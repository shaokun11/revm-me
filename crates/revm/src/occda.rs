// OCCDA (Optimistic Concurrent Contract Deployment and Analysis) implementation
// This module provides parallel execution of EVM transactions with optimistic concurrency control
use crate::primitives::ResultAndState;
use crate::access_tracker::AccessTracker;
use crate::journaled_state::AccessType;
use crate::task::{Task, TaskResultItem};
use crate::dag::TaskDag;
use crate::evm::Evm;
use crate::db::{Database, DatabaseCommit, DatabaseRef, WrapDatabaseRef};
use crate::inspector::GetInspector;
use crate::inspector_handle_register;
use std::sync::Arc;
use rayon::ThreadPool;
use rayon::prelude::*;
use parking_lot::RwLock;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::Duration;

// Main struct for handling parallel execution of EVM transactions
pub struct Occda {
    _dag: TaskDag,           // Dependency graph for tasks
    num_threads: usize,
    thread_pool: ThreadPool,
}


impl Occda {
    // Create a new OCCDA instance with specified number of threads
    pub fn new(num_threads: usize) -> Self {
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .unwrap();

        Occda {
            _dag: TaskDag::new(),
            num_threads,
            thread_pool,
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

        let mut perpare_time = Duration::from_secs(0);
        let mut commit_time = Duration::from_secs(0);
        let mut parallel_time = Duration::from_secs(0);
        let mut seq_time = Duration::from_secs(0);

        // Initialize tracking variables
        let tx_size = h_tx.len();
        let mut exec_size = 0;
        let mut h_ready: Vec<usize> = Vec::new();          // Tasks ready for execution
        let mut h_exec: BinaryHeap<Reverse<(usize, usize)>> = BinaryHeap::new(); // Tasks executing
        let mut h_commit: BinaryHeap<Reverse<usize>> = BinaryHeap::new(); // Tasks ready for commit
        let len = h_tx.len();
        let mut next = 0;

        for i in 0..len {
            h_exec.push(Reverse((h_tx[i].sid as usize, h_tx[i].tid as usize)));
        }

        // Create access tracker for conflict detection
        let mut access_tracker = AccessTracker::new();
        
        // Initialize result and state storage

        let db_shared = db.clone();

        let result_ptr = result_store.as_mut_ptr() as usize;

        // Main processing loop
        while next < len {
            let perpare_start = std::time::Instant::now();
            // Find tasks ready for execution based on sequence ID
            while let Some(Reverse((sid, tid))) = h_exec.pop() {
                if h_tx[tid as usize].sid <= (next as i32 - 1) {
                    h_ready.push(tid as usize);
                } else {
                    h_exec.push(Reverse((sid, tid)));
                    break;
                }
            }
            if h_ready.is_empty() {
                break;
            }
            let perpare_end = std::time::Instant::now();
            perpare_time += perpare_end - perpare_start;

            // Sort ready tasks by gas (higher gas first)
            exec_size += h_ready.len();
            let ready_tasks = std::mem::take(&mut h_ready);
            
            if ready_tasks.len() == 1 {
                let seq_start = std::time::Instant::now();
                let idx = ready_tasks[0];
                let task = &h_tx[idx];
                let db_ref = db_shared.read();
                let inspector = task.inspector.clone().unwrap();
                let mut evm = Evm::builder()
                                .with_ref_db(&*db_ref)
                                .modify_env(|env| env.clone_from(&task.env))
                                .with_external_context(inspector)
                                .with_spec_id(task.spec_id)
                                .append_handler_register(inspector_handle_register)
                                .build();
                let result = evm.transact();

                let mut task_result = TaskResultItem::default();    
                task_result.inspector = Some(evm.context.external.clone());
                task_result.gas = task.gas;

                let mut read_write_set = evm.get_read_write_set();
                read_write_set.add_write(task.env.tx.caller, AccessType::AccountInfo);
                task_result.read_write_set = Some(read_write_set);
                
                
                match result {
                    Ok(result_and_state) => {
                        let ResultAndState { state, result } = result_and_state;
                        task_result.state = Some(state);
                        task_result.result = Some(result);
                    }
                    Err(_) => {
                        task_result.state = None;
                        task_result.gas = 0;
                    }
                }
                
                result_store[idx] = task_result;
                let seq_end = std::time::Instant::now();
                seq_time += seq_end - seq_start;
                
            } else {
                let parallel_start = std::time::Instant::now();
                let total_gas: u64 = ready_tasks.iter().map(|&idx| h_tx[idx].gas).sum();
                let target_gas_per_thread = total_gas / self.num_threads as u64;
                
                let mut chunks: Vec<Vec<usize>> = vec![Vec::new(); self.num_threads];
                let mut current_thread = 0;
                let mut current_gas = 0u64;
                
                let mut sorted_tasks: Vec<_> = ready_tasks.iter().map(|&idx| (idx, h_tx[idx].gas)).collect();
                sorted_tasks.sort_by_key(|&(_, gas)| std::cmp::Reverse(gas));
                
                for (idx, gas) in sorted_tasks {
                    if current_gas >= target_gas_per_thread && current_thread < self.num_threads - 1 {
                        current_thread += 1;
                        current_gas = 0;
                    }
                    chunks[current_thread].push(idx);
                    current_gas += gas;
                }
                
                println!("Gas distribution:");
                for (i, chunk) in chunks.iter().enumerate() {
                    let chunk_gas: u64 = chunk.iter().map(|&idx| h_tx[idx].gas).sum();
                    println!("Thread {}: tasks={}, total_gas={}", i, chunk.len(), chunk_gas);
                }

                let thread_times: Arc<parking_lot::RwLock<Vec<(Duration, Duration, Duration, Duration)>>> = 
                    Arc::new(parking_lot::RwLock::new(vec![(Duration::from_secs(0), Duration::from_secs(0), 
                    Duration::from_secs(0), Duration::from_secs(0)); self.num_threads]));

                self.thread_pool.install(|| {
                    chunks
                        .into_par_iter()
                        .enumerate()
                        .for_each(|(thread_id, indexes)| {
                            let db_read_start = std::time::Instant::now();
                            let db_ref = &*db_shared.read();
                            let db_read_end = std::time::Instant::now();
                            let db_read_time = db_read_end - db_read_start;
                            
                            let mut init_time = Duration::from_secs(0);
                            let mut transact_time = Duration::from_secs(0);
                            let mut write_result_time = Duration::from_secs(0);
                            let mut transact_times = Vec::with_capacity(indexes.len());

                            println!("task_size: {:?} thread_id: {}", indexes.len(), thread_id);

                            for idx in indexes {
                                let task = &h_tx[idx];
                                
                                let init_start = std::time::Instant::now();
                                let inspector = task.inspector.clone().unwrap();
                                let mut evm = Evm::builder()
                                    .with_ref_db(db_ref)
                                    .modify_env(|env| env.clone_from(&task.env))
                                    .with_external_context(inspector)
                                    .with_spec_id(task.spec_id)
                                    .append_handler_register(inspector_handle_register)
                                    .build();
                                let init_end = std::time::Instant::now();
                                init_time += init_end - init_start;

                                let transact_start = std::time::Instant::now();
                                let result = evm.transact();
                                let transact_end = std::time::Instant::now();
                                let this_transact_time = transact_end - transact_start;
                                transact_time += this_transact_time;
                                transact_times.push(this_transact_time);

                                let write_start = std::time::Instant::now();
                                // Process execution results
                                let mut task_result = TaskResultItem::default();
                                task_result.inspector = Some(evm.context.external.clone());
                                task_result.gas = task.gas;

                                // Track read-write access
                                let mut read_write_set = evm.get_read_write_set();
                                read_write_set.add_write(task.env.tx.caller, AccessType::AccountInfo);
                                task_result.read_write_set = Some(read_write_set);

                                // Handle execution result
                                match result {
                                    Ok(result_and_state) => {
                                        let ResultAndState { state, result } = result_and_state;
                                        task_result.state = Some(state);
                                        task_result.result = Some(result);
                                    }
                                    Err(_) => {
                                        task_result.state = None;
                                        task_result.gas = 0;
                                    }
                                }

                                let result_raw_ptr = result_ptr as *mut TaskResultItem<I>;
                                unsafe {
                                    *result_raw_ptr.add(idx) = task_result;
                                }
                                let write_end = std::time::Instant::now();
                                write_result_time += write_end - write_start;
                            }
                            thread_times.write()[thread_id] = (db_read_time, init_time, transact_time, write_result_time);
                            
                            if !transact_times.is_empty() {
                                let avg = transact_times.iter().sum::<Duration>() / transact_times.len() as u32;
                                let min = transact_times.iter().min().unwrap();
                                let max = transact_times.iter().max().unwrap();
                                println!("Thread {} transact times - avg: {:?}, min: {:?}, max: {:?}", 
                                    thread_id, avg, min, max);
                            }
                        });
                });
                let parallel_end = std::time::Instant::now();
                parallel_time += parallel_end - parallel_start;
                // 打印每个线程的执行时间
                println!("Thread execution times:");
                for (i, (db_time, init_time, transact_time, write_time)) in thread_times.read().iter().enumerate() {
                    println!("Thread {}: db_read={:?}, init={:?}, transact={:?}, write={:?}", 
                        i, db_time, init_time, transact_time, write_time);
                }
            }

            h_commit.extend(ready_tasks.iter().map(|&idx| Reverse(idx)));

            // Begin commit phase
            // let start = std::time::Instant::now();
            let commit_start = std::time::Instant::now();
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

                let task_result = &mut result_store[task_idx as usize];

                // Check for conflicts
                let read_write_set = task_result.read_write_set.as_ref().unwrap();
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
                    h_exec.push(Reverse((h_tx[task_idx].sid as usize, h_tx[task_idx].tid as usize)));
                } else {
                    // No conflict: commit changes and update access tracker
                    if let Some(state) = task_result.state.clone() {
                        db_mut.commit(state);
                    }

                    access_tracker.record_write_set(
                        h_tx[task_idx].tid,
                        &read_write_set.write_set
                    );
                    next += 1;
                }
            }
            let commit_end = std::time::Instant::now();
            commit_time += commit_end - commit_start;
        }

        // Calculate and log execution statistics
        let conflict_rate = ((exec_size - tx_size) as f64) / (tx_size as f64) * 100.0;
        println!(
            "finished execute tasks size: {} with conflict rate: {:.2}%",
            result_store.len(),
            conflict_rate
        );
        println!("perpare_time: {:?}", perpare_time);
        println!("parallel_time: {:?}", parallel_time);
        println!("seq_time: {:?}", seq_time);
        println!("commit_time: {:?}", commit_time); 

        // Clean up resources asynchronously
        std::thread::spawn(move || {
            drop(access_tracker);
            drop(h_exec);
            drop(h_commit);
            drop(h_ready);
        });

        Ok(())
    }
}
