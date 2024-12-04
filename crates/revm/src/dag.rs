use daggy::{Dag, NodeIndex, Walker};
use std::collections::HashMap;
use crate::task::Task;

pub struct TaskDag {
    dag: Dag<(), ()>,
    task_to_node: HashMap<i32, NodeIndex>,
}

impl TaskDag {
    pub fn new() -> Self {
        TaskDag {
            dag: Dag::new(),
            task_to_node: HashMap::new(),
        }
    }

    pub fn add_task(&mut self, task: &Task) -> NodeIndex {
        let node = self.dag.add_node(());
        self.task_to_node.insert(task.tid, node);
        node
    }

    pub fn add_dependency(&mut self, dependent: &Task, dependency: &Task) {
        if let (Some(&dep_node), Some(&task_node)) = (
            self.task_to_node.get(&dependency.tid),
            self.task_to_node.get(&dependent.tid),
        ) {
            let _ = self.dag.add_edge(dep_node, task_node, ());
        }
    }

    pub fn get_dependencies(&self, task: &Task) -> Vec<NodeIndex> {
        if let Some(&node) = self.task_to_node.get(&task.tid) {
            self.dag.parents(node).iter(&self.dag).map(|(_, n)| n).collect()
        } else {
            Vec::new()
        }
    }

    pub fn get_task_tid(&self, node: NodeIndex) -> Option<i32> {
        self.task_to_node.iter()
            .find_map(|(&tid, &n)| if n == node { Some(tid) } else { None })
    }
}
