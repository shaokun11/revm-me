use std::collections::BinaryHeap;
use std::cmp::Reverse;
use crate::primitives::{ResultAndState, SpecId, Env};
use crate::access_tracker::AccessTracker;
use crate::journaled_state::AccessType;
use crate::task::{SidOrderedTask, Task, TidOrderedTask};
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


pub struct Occda {
    _dag: TaskDag,
    thread_pool: ThreadPool
}

impl Occda
{
    pub fn new(num_threads: usize) -> Self {
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            // .stack_size(8 * 1024 * 1024) 
            .build()
            .unwrap();

        thread_pool.install(|| {
            (0..num_threads).into_par_iter().for_each(|_| {
                std::thread::sleep(std::time::Duration::from_millis(1));
            });
        });
        
        Occda {
            _dag: TaskDag::new(),
            thread_pool,
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

    fn build_evm<'a, DB, I>(&self, db: &'a DB, inspector: I, spec_id: SpecId, e: &Box<Env>
    , register_handles_fn: HandleRegister<I, WrapDatabaseRef<&'a DB>>) -> Evm<'a, I, WrapDatabaseRef<&'a DB>>
        where
            DB: Database + DatabaseRef,
        {
            Evm::builder()
                .with_ref_db(db)
                .modify_env(|env| env.clone_from(e))
                .with_external_context(inspector)
                .with_spec_id(spec_id)
                .append_handler_register(register_handles_fn)
                .build()
        }

    pub async fn main_with_db<DB: DatabaseRef + Database + DatabaseCommit + Send + Sync, I>(
        &mut self,
        mut h_tx: BinaryHeap<Reverse<SidOrderedTask>>,
        db: Arc<RwLock<DB>>,
        inspector: Arc<I>
    ) -> Result<Vec<Task>, Box<dyn std::error::Error + Send + Sync>> 
    where
        DB: DatabaseRef + Database + DatabaseCommit + Send + Sync,
        I: Send + Sync + Clone + for<'db> GetInspector<WrapDatabaseRef<&'db DB>>,
    {
        let mut h_ready = BinaryHeap::<Reverse<TidOrderedTask>>::new();
        let mut h_commit = BinaryHeap::<Reverse<TidOrderedTask>>::new();
        let mut next = 0;
        let len = h_tx.len();
        let mut access_tracker = AccessTracker::new();

        let mut task_list: Vec<Task> = Vec::new();
        while next < len {
            // Schedule tasks
            while let Some(Reverse(SidOrderedTask(task))) = h_tx.pop() {
                if task.sid <= next as i32 - 1 {
                    h_ready.push(Reverse(TidOrderedTask(task)));
                } else {
                    h_tx.push(Reverse(SidOrderedTask(task)));
                    break;
                }
            }

            let mut tasks: Vec<_> = h_ready.drain().collect();
            tasks.sort_by(|a, b| {
                // Sort by gas in descending order (high to low)
                let Reverse(TidOrderedTask(task_a)) = a;
                let Reverse(TidOrderedTask(task_b)) = b;
                task_b.gas.cmp(&task_a.gas)
            });

            let this = &*self;
            let db_shared = Arc::clone(&db);
            let inspector_shared = Arc::clone(&inspector);
            let results: Vec<_> = self.thread_pool.install(|| {
                tasks.into_par_iter()
                .map({
                    let this = this;
                    let db_shared = Arc::clone(&db_shared);
                    move |Reverse(TidOrderedTask(mut task))| {
                        let db_ref = db_shared.read();
                        {
                            let inspector = Arc::clone(&inspector_shared);
                            let db_ref_mut: &DB = &*db_ref;
                            let mut evm = this.build_evm(db_ref_mut, (*inspector).clone(), task.spec_id, &task.env, inspector_handle_register);

                            let result = evm.transact();

                            let mut read_write_set = evm.get_read_write_set();
                            read_write_set.add_write(task.env.tx.caller, AccessType::AccountInfo);
                            task.read_write_set = Some(read_write_set);

                            match result {
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
                        } 
                        

                        task
                    }
                })
                .collect()
            });

            // task_list.extend(results);
            // task_list.extend(results.iter().map(|t| t.clone()));
            for task in results {
                h_commit.push(Reverse(TidOrderedTask(task)));
            }

            // 
            // Commit tasks
            // Commit or abort tasks
            while let Some(Reverse(TidOrderedTask(mut task))) = h_commit.pop() {
                if task.tid != next as i32 {
                    h_commit.push(Reverse(TidOrderedTask(task)));
                    break;
                }

                // Check conflicts with all tasks between sid+1 and tid-1
                let conflict = access_tracker.check_conflict_in_range(
                    &task.read_write_set.as_ref().unwrap().read_set,
                    task.sid + 1,
                    task.tid
                );
                if conflict.is_some() {
                    task.sid = task.tid - 1;
                    h_tx.push(Reverse(SidOrderedTask(task)));
                } else {
                    let state_to_commit = task.state.clone().ok_or_else(|| {
                        eprintln!("Task state is None, returning error");
                        Box::<dyn std::error::Error + Send + Sync>::from("Task state is None")
                    }).unwrap();

                    db.write().commit(state_to_commit.clone());
                    

                    access_tracker.record_write_set(
                        task.tid,
                        &task.read_write_set.as_ref().unwrap().write_set
                    );
                    task_list.push(task);
                    next += 1;
                }

            }
        }

        println!("finished execute tasks size: {}", task_list.len());
        Ok(task_list)
    }

}