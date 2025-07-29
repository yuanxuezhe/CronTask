use crate::task::TaskDetail;
use std::collections::HashMap;
use crate::task::Task;

pub struct InnerState {
    pub taskdetails: Vec<TaskDetail>,
    pub tasks: HashMap<i32, Task>,
} 