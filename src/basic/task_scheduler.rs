use std::sync::Arc;

// 外部 crate 导入
use chrono::NaiveDateTime;
use std::time::Duration;

// 内部模块导入
use crate::common::error::CronTaskError;
use super::message_bus::MessageBus;
use super::time_wheel::TimeWheel;

/// 任务调度器
pub struct TaskScheduler {
    /// 时间轮实例
    time_wheel: Arc<TimeWheel>,
}

impl TaskScheduler {
    /// 创建新的任务调度器
    ///
    /// # 参数
    /// * `tick_duration` - 时间轮滴答间隔
    /// * `total_slots` - 时间轮总槽数
    /// * `message_bus` - 消息总线，用于发送任务执行事件
    ///
    /// # 返回值
    /// 返回新的任务调度器实例
    pub fn new(tick_duration: Duration, total_slots: usize, message_bus: Arc<MessageBus>) -> Self {
        let time_wheel = Arc::new(TimeWheel::new(tick_duration, total_slots, message_bus));

        Self { time_wheel }
    }

    /// 将任务添加到时间轮中进行调度（直接调用时间轮方法，避免通道开销）
    ///
    /// # 参数
    /// * `timestamp` - 任务触发时间
    /// * `delay` - 延迟时间
    /// * `key` - 任务唯一标识符
    ///
    /// # 返回值
    /// 返回操作结果，包含任务唯一标识符
    pub async fn add<K>(
        &self,
        timestamp: NaiveDateTime,
        delay: Duration,
        key: K,
    ) -> Result<String, CronTaskError>
    where
        K: Into<String>,
    {
        // 直接调用时间轮方法，避免通道开销
        self.time_wheel.add_task(timestamp, delay, key.into()).await
    }

    /// 取消已添加的任务（直接调用时间轮方法，避免通道开销）
    ///
    /// # 参数
    /// * `timestamp` - 任务原定触发时间
    /// * `delay` - 原定延迟时间
    /// * `key` - 任务唯一标识符
    ///
    /// # 返回值
    /// 返回操作结果
    pub async fn cancel<K>(
        &self,
        timestamp: NaiveDateTime,
        delay: Duration,
        key: K,
    ) -> Result<String, CronTaskError>
    where
        K: Into<String>,
    {
        // 直接调用时间轮方法，避免通道开销
        self.time_wheel.del_task(timestamp, delay, key.into()).await
    }

    /// 获取时间轮实例
    ///
    /// # 返回值
    /// 返回时间轮实例的Arc引用
    pub fn time_wheel(&self) -> Arc<TimeWheel> {
        Arc::clone(&self.time_wheel)
    }
}
