// 全局通用工具函数

/// 通过组合任务ID和时间点生成唯一标识符
/// 
/// # 参数
/// * `taskid` - 任务ID
/// * `timepoint` - 时间点字符串
/// 
/// # 返回值
/// 返回格式为"{taskid}::{timepoint}"的字符串
pub fn gen_task_key(taskid: i32, timepoint: &str) -> String {
    format!("{}::{}", taskid, timepoint)
}