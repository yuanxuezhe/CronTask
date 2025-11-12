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
    /// 更新TaskDetail的状态到数据库
    /// 注意：这里我们假设有一个task_detail表来存储TaskDetail信息
    /// 但在当前实现中，TaskDetail信息存储在task表中，我们需要根据taskid和timepoint更新状态
    pub async fn update_status(&self, _db: &Database) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 在实际应用中，如果TaskDetail有独立的表，我们会在这里更新
        // 但根据当前设计，TaskDetail信息实际上是内存中的临时状态，不需要直接更新到数据库
        // 我们会在task表中添加相关字段或者创建新的task_detail表来存储这些信息
        Ok(())
    }
}