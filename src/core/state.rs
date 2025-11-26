use crate::task_engine::model::{Task, TaskDetail};

/// 内部状态结构
pub struct InnerState {
    /// 任务详情映射，键为(taskid, timepoint)元组，值为TaskDetail对象
    pub taskdetails: std::collections::HashMap<(i32, String), TaskDetail>,
    /// 任务映射，键为taskid，值为Task对象
    pub tasks: std::collections::HashMap<i32, Task>,
}
