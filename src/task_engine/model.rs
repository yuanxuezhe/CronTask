use dbcore::{Database, ResultSet};
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct TaskDetail {
    pub taskid: i32,
    pub timepoint: String,
    pub current_trigger_count: i32,
    pub status: i32,
    pub tag: i32,
}

#[derive(Debug, Default, Clone, macros::SqlCRUD, Serialize)]
#[table_name = "task"]
pub struct Task {
    #[primary_key]
    pub taskid: i32,
    pub taskname: String,
    pub start_date: String,
    pub end_date: String,
    pub cycle_type: String,
    pub period: String, 
    pub time_point: String,
    pub retry_type: String,
    pub retry_interval: i32,
    pub retry_count: i32,
    pub status: String,
    pub discribe: String,
}

impl TaskDetail {
    /// 生成并返回任务键
    pub fn task_key(&self) -> String {
        format!("{}{}{}", self.taskid, crate::common::consts::TASK_KEY_SEPARATOR, self.timepoint)
    }
}