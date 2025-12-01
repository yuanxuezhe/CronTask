// 标准库导入
use std::sync::Arc;
use std::time::Duration;

// 从当前目录重新导出类型
pub use super::{MessageBus, TimeBus, TaskScheduler};

/// 创建新的消息总线
pub fn create_message_bus(channel_buffer_size: usize) -> Arc<MessageBus> {
    MessageBus::new(channel_buffer_size)
}

/// 创建新的时间总线
pub fn create_time_bus() -> Arc<TimeBus> {
    TimeBus::new()
}

/// 创建新的任务调度器
pub fn create_task_scheduler(
    tick_duration: Duration,
    total_slots: usize,
    message_bus: Arc<MessageBus>,
) -> TaskScheduler {
    TaskScheduler::new(tick_duration, total_slots, message_bus)
}


