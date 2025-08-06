use crate::task_engine::model::TaskDetail;
use std::collections::HashMap;
use crate::task_engine::model::Task;

pub struct InnerState {
    pub taskdetails: Vec<TaskDetail>,
    pub tasks: HashMap<i32, Task>,
} 