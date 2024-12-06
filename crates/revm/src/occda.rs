use core::cell::UnsafeCell;
use std::collections::BinaryHeap;
use std::cmp::Reverse;
use std::time::Instant;
use crate::primitives::{ResultAndState, SpecId};
use crate::access_tracker::AccessTracker;
use crate::journaled_state::AccessType;
use crate::task::{SidOrderedTask, Task, TidOrderedTask};
use crate::dag::TaskDag;
use crate::evm::Evm;
use crate::profiler;
use crate::db::{DatabaseCommit, Database, DatabaseRef};
use std::sync::Arc;
use serde_json::{Map, Value};
use rayon::prelude::*;

struct SyncState<'a, DB> {
    inner: UnsafeCell<&'a mut DB>
}
unsafe impl<DB> Send for SyncState<'_, DB> {}
unsafe impl<DB> Sync for SyncState<'_, DB> {}

pub struct Occda {
    _dag: TaskDag,
}

impl Occda
{
    pub fn new(num_threads: usize) -> Self {
        // Initialize global thread pool during struct creation
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build_global()
            .unwrap();

        Occda {
            _dag: TaskDag::new(),
        }
    }

    // pub fn get_cached_state(&self) -> CacheState {
    //     let state = self.state.read();
    //     state.cache.clone()
    // }

    pub fn init(&mut self, tasks: Vec<Task>, graph: Option<&TaskDag>) -> BinaryHeap<Reverse<SidOrderedTask>> {
        let mut heap = BinaryHeap::new();
        
        for mut task in tasks {
            if let Some(g) = graph {
                let sid_max = g.get_dependencies(&task)
                    .into_iter()
                    .map(|node| g.get_task_tid(node).unwrap_or(-1))
                    .max()
                    .unwrap_or(-1);
                task.sid = sid_max;
            } else {
                task.sid = -1;
            }
            
            heap.push(Reverse(SidOrderedTask(task)));
        }
        
        heap
    }

    pub async fn main_with_db<DB: Database + DatabaseCommit + 'static>(
        &mut self,
        mut h_tx: BinaryHeap<Reverse<SidOrderedTask>>,
        db_mut: &mut DB
    ) -> Result<Vec<ResultAndState>, Box<dyn std::error::Error + Send + Sync>> {
        let mut h_ready = BinaryHeap::<Reverse<TidOrderedTask>>::new();
        // let mut h_threads = BinaryHeap::<Reverse<GasOrderedTask>>::new();
        let mut h_commit = BinaryHeap::<Reverse<TidOrderedTask>>::new();
        let mut next = 0;
        let len = h_tx.len();
        // let mut total_gas_usage = 0u64;
        let mut access_tracker = AccessTracker::new();
        let db_shared = Arc::new(SyncState {
            inner: UnsafeCell::new(db_mut)
        });

        let mut results_states: Vec<ResultAndState> = Vec::new();
        while next < len {
            // Schedule tasks
            // Move tasks from h_tx to h_ready
            profiler::start("schedule");
            while let Some(Reverse(SidOrderedTask(task))) = h_tx.pop() {
                if task.sid <= next as i32 - 1 {
                    h_ready.push(Reverse(TidOrderedTask(task)));
                } else {
                    h_tx.push(Reverse(SidOrderedTask(task)));
                    break;
                }
            }
            profiler::end("schedule");

            // while !h_ready.is_empty() {
            //     if let Some(Reverse(TidOrderedTask(task))) = h_ready.pop() {
            //         h_threads.push(Reverse(GasOrderedTask(task)));
            //     }
            // }

            // Execute tasks
            let mut tasks: Vec<_> = h_ready.drain().collect();
            tasks.sort_by(|a, b| {
                // Sort by gas in descending order (high to low)
                let Reverse(TidOrderedTask(task_a)) = a;
                let Reverse(TidOrderedTask(task_b)) = b;
                task_b.gas.cmp(&task_a.gas)
            });
            let results: Vec<_> = tasks.into_par_iter()
            .map({
                let db_shared = Arc::clone(&db_shared);
                move |Reverse(TidOrderedTask(mut task))| {
                    let task_name = task.tid.to_string();
                    profiler::start(&task_name);

                    let genesis = profiler::get_genesis();
                    let duration_u64 = || (
                        Instant::now().duration_since(genesis).as_nanos() as u64
                    ).into();
                    let mut description = Map::new();
                    description.insert("type".to_string(), Value::String("transaction".to_string()));

                    description.insert("evm_build::start".to_string(), duration_u64());
                    // let static_data: &'static mut State<DB> = unsafe { transmute(db_mut_clone) };
                    unsafe {
                        let db_ref = &mut **db_shared.inner.get();
                        let mut evm = Evm::builder()
                        .with_db(db_ref)
                        .modify_env(|e| e.clone_from(&task.env))
                        .with_spec_id(SpecId::CANCUN)
                        .build();
                        description.insert("evm_build::end".to_string(), duration_u64());

                        description.insert("transact::start".to_string(), duration_u64());
                        let result = evm.transact();
                        description.insert("transact::end".to_string(), duration_u64());

                        description.insert("get_rwset::start".to_string(), duration_u64());
                        let mut read_write_set = evm.get_read_write_set();
                        read_write_set.add_write(task.env.tx.caller, AccessType::AccountInfo);
                        task.read_write_set = Some(read_write_set);
                        description.insert("get_rwset::end".to_string(), duration_u64());
                        
                        description.insert("process_result::start".to_string(), duration_u64());
                        let status = match result {
                            Ok(result_and_state) => {
                                let ResultAndState { state, result } = result_and_state;
                                task.state = Some(state);
                                task.result = Some(result);
                                if task.result.as_ref().unwrap().is_success() {
                                    "success"
                                } else {
                                    "revert"
                                }
                            },
                            Err(_) => {
                                task.state = None;
                                task.gas = 0;
                                "abort"
                            },
                        };
                        description.insert("process_result::end".to_string(), duration_u64());
                        description.insert("status".to_string(), Value::String(status.to_string()));
                        profiler::notes(&task_name, &mut description);
                        profiler::end(&task_name);
                        task
                    }
                }
            })
            .collect();

            // Wait for at least one task to complete
            profiler::start("collect_gas");
            for task in results {
                results_states.push(ResultAndState { 
                    state: task.state.clone().unwrap(), 
                    result: task.result.clone().unwrap() 
                });
                h_commit.push(Reverse(TidOrderedTask(task)));
            }
            profiler::end("collect_gas");
            // Commit tasks
            // Commit or abort tasks

            while let Some(Reverse(TidOrderedTask(mut task))) = h_commit.pop() {
                if task.tid != next as i32 {
                    h_commit.push(Reverse(TidOrderedTask(task)));
                    break;
                }
                
                profiler::start("commit");

                let genesis = profiler::get_genesis();
                let duration_u64 = || (
                    Instant::now().duration_since(genesis).as_nanos() as u64
                ).into();
                let mut description = Map::new();
                description.insert("type".to_string(), Value::String("commit".to_string()));
                description.insert("tx".to_string(), Value::String(task.tid.to_string()));

                // Check conflicts with all tasks between sid+1 and tid-1
                description.insert("check_conflict::start".to_string(), duration_u64());
                let conflict = access_tracker.check_conflict_in_range(
                    &task.read_write_set.as_ref().unwrap().read_set,
                    task.sid + 1,
                    task.tid
                );
                description.insert("check_conflict::end".to_string(), duration_u64());
                if conflict.is_some() {
                    task.sid = task.tid - 1;
                    h_tx.push(Reverse(SidOrderedTask(task)));
                } else {
                    // Commit the changes serially
                    description.insert("db_mut::start".to_string(), duration_u64());
                    // let db_mut: &'static mut State<DB> = unsafe { transmute(db) };

                    description.insert("db_mut::end".to_string(), duration_u64());

                    description.insert("take_state::start".to_string(), duration_u64());
                    let state_to_commit = task.state.ok_or_else(|| {
                        eprintln!("Task state is None, returning error");
                        Box::<dyn std::error::Error + Send + Sync>::from("Task state is None")
                    }).unwrap();
                    description.insert("take_state::end".to_string(), duration_u64());

                    description.insert("db_commit::start".to_string(), duration_u64());
                    unsafe {
                        let db_ref = &mut **db_shared.inner.get();
                        db_ref.commit(state_to_commit);
                    }
                    // db_mut.commit(state_to_commit);
                    description.insert("db_commit::end".to_string(), duration_u64());
                    
                    description.insert("record_write::start".to_string(), duration_u64());
                    access_tracker.record_write_set(
                        task.tid,
                        &task.read_write_set.as_ref().unwrap().write_set
                    );
                    description.insert("record_write::end".to_string(), duration_u64());
                    next += 1;
                }

                profiler::notes("commit", &mut description);
                profiler::end("commit");

            }
        }

        Ok(results_states)
    }

    pub async fn main_with_db_ref<DB: DatabaseRef + DatabaseCommit + Sync + 'static>(
        &mut self,
        mut h_tx: BinaryHeap<Reverse<SidOrderedTask>>,
        db_mut: &mut DB
    ) -> Result<Vec<ResultAndState>, Box<dyn std::error::Error + Send + Sync>> {
        let mut h_ready = BinaryHeap::<Reverse<TidOrderedTask>>::new();
        let mut h_commit = BinaryHeap::<Reverse<TidOrderedTask>>::new();
        let mut next = 0;
        let len = h_tx.len();
        let mut access_tracker = AccessTracker::new();

        let mut results_states: Vec<ResultAndState> = Vec::new();
        while next < len {
            // 调度任务
            profiler::start("schedule");
            while let Some(Reverse(SidOrderedTask(task))) = h_tx.pop() {
                if task.sid <= next as i32 - 1 {
                    h_ready.push(Reverse(TidOrderedTask(task)));
                } else {
                    h_tx.push(Reverse(SidOrderedTask(task)));
                    break;
                }
            }
            profiler::end("schedule");

            // 执行任务
            let mut tasks: Vec<_> = h_ready.drain().collect();
            tasks.sort_by(|a, b| {
                let Reverse(TidOrderedTask(task_a)) = a;
                let Reverse(TidOrderedTask(task_b)) = b;
                task_b.gas.cmp(&task_a.gas)
            });
            let db_ref = &*db_mut;
            let results: Vec<_> = tasks.into_par_iter()
            .map({
                move |Reverse(TidOrderedTask(mut task))| {
                    let task_name = task.tid.to_string();
                    profiler::start(&task_name);

                    let genesis = profiler::get_genesis();
                    let duration_u64 = || (
                        Instant::now().duration_since(genesis).as_nanos() as u64
                    ).into();
                    let mut description = Map::new();
                    description.insert("type".to_string(), Value::String("transaction".to_string()));

                    description.insert("evm_build::start".to_string(), duration_u64());
                    let mut evm = Evm::builder()
                            .with_ref_db(db_ref)
                            .modify_env(|e| e.clone_from(&task.env))
                            .with_spec_id(SpecId::CANCUN)
                            .build();
                        description.insert("evm_build::end".to_string(), duration_u64());

                        description.insert("transact::start".to_string(), duration_u64());
                        let result = evm.transact();
                        description.insert("transact::end".to_string(), duration_u64());

                        description.insert("get_rwset::start".to_string(), duration_u64());
                        let mut read_write_set = evm.get_read_write_set();
                        read_write_set.add_write(task.env.tx.caller, AccessType::AccountInfo);
                        task.read_write_set = Some(read_write_set);
                        description.insert("get_rwset::end".to_string(), duration_u64());
                        
                        description.insert("process_result::start".to_string(), duration_u64());
                        let status = match result {
                            Ok(result_and_state) => {
                                let ResultAndState { state, result } = result_and_state;
                                task.state = Some(state);
                                task.result = Some(result);
                                if task.result.as_ref().unwrap().is_success() {
                                    "success"
                                } else {
                                    "revert"
                                }
                            },
                            Err(_) => {
                                task.state = None;
                                task.gas = 0;
                                "abort"
                            },
                        };
                    description.insert("process_result::end".to_string(), duration_u64());
                    description.insert("status".to_string(), Value::String(status.to_string()));
                    profiler::notes(&task_name, &mut description);
                    profiler::end(&task_name);
                    task
                }
            })
            .collect();

            // 收集结果
            profiler::start("collect_gas");
            for task in results {
                results_states.push(ResultAndState { 
                    state: task.state.clone().unwrap(), 
                    result: task.result.clone().unwrap() 
                });
                h_commit.push(Reverse(TidOrderedTask(task)));
            }
            profiler::end("collect_gas");

            // 提交任务
            while let Some(Reverse(TidOrderedTask(mut task))) = h_commit.pop() {
                if task.tid != next as i32 {
                    h_commit.push(Reverse(TidOrderedTask(task)));
                    break;
                }

                profiler::start("commit");

                let genesis = profiler::get_genesis();
                let duration_u64 = || (
                    Instant::now().duration_since(genesis).as_nanos() as u64
                ).into();
                let mut description = Map::new();
                description.insert("type".to_string(), Value::String("commit".to_string()));
                description.insert("tx".to_string(), Value::String(task.tid.to_string()));

                description.insert("check_conflict::start".to_string(), duration_u64());
                let conflict = access_tracker.check_conflict_in_range(
                    &task.read_write_set.as_ref().unwrap().read_set,
                    task.sid + 1,
                    task.tid
                );
                description.insert("check_conflict::end".to_string(), duration_u64());
                if conflict.is_some() {
                    task.sid = task.tid - 1;
                    h_tx.push(Reverse(SidOrderedTask(task)));
                } else {
                    description.insert("db_commit::start".to_string(), duration_u64());
                    db_mut.commit(task.state.ok_or_else(|| {
                        eprintln!("Task state is None, returning error");
                        Box::<dyn std::error::Error + Send + Sync>::from("Task state is None")
                    }).unwrap());
                    description.insert("db_commit::end".to_string(), duration_u64());

                    description.insert("record_write::start".to_string(), duration_u64());
                    access_tracker.record_write_set(
                        task.tid,
                        &task.read_write_set.as_ref().unwrap().write_set
                    );
                    description.insert("record_write::end".to_string(), duration_u64());
                    next += 1;
                }

                profiler::notes("commit", &mut description);
                profiler::end("commit");
            }
        }

        Ok(results_states)
    }
}