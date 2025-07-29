// 全局通用工具函数
// 例如：任务 key 生成、通用时间格式化等

/// 生成任务唯一 key
pub fn gen_task_key(taskid: i32, timepoint: &str) -> String {
    format!("{}::{}", taskid, timepoint)
} 