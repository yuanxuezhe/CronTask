use crate::task_engine::model::{Task, TaskDetail};

/// 内部状态结构
pub struct InnerState {
    /// 任务详情列表
    pub taskdetails: Vec<TaskDetail>,
    /// 任务映射，键为taskid，值为Task对象
    pub tasks: std::collections::HashMap<i32, Task>,
}