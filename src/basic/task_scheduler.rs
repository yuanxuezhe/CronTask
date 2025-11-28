use std::sync::Arc;

// 外部 crate 导入
use chrono::NaiveDateTime;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

// 内部模块导入
use crate::common::error::CronTaskError;
use super::message_bus::MessageBus;
use super::time_wheel::TimeWheel;

/// 任务请求类型
#[derive(Debug)]
pub enum TaskRequest {
    /// 添加任务请求
    Add {
        /// 任务触发时间
        timestamp: NaiveDateTime,
        /// 延迟时间
        delay: Duration,
        /// 任务唯一标识符
        key: String,
        /// 响应通道，用于返回操作结果
        response: oneshot::Sender<Result<String, CronTaskError>>,
    },
    /// 取消任务请求
    Cancel {
        /// 任务原定触发时间
        timestamp: NaiveDateTime,
        /// 原定延迟时间
        delay: Duration,
        /// 任务唯一标识符
        key: String,
        /// 响应通道，用于返回操作结果
        response: oneshot::Sender<Result<String, CronTaskError>>,
    },
}

/// 通道缓冲区大小
const CHANNEL_BUFFER_SIZE: usize = 1000;

/// 任务调度器
pub struct TaskScheduler {
    /// 任务请求发送通道
    pub sender: mpsc::Sender<TaskRequest>,
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
        let (sender, receiver) = mpsc::channel::<TaskRequest>(CHANNEL_BUFFER_SIZE);
        let time_wheel = Arc::new(TimeWheel::new(tick_duration, total_slots, message_bus));
        let tw_clone: Arc<TimeWheel> = Arc::clone(&time_wheel);

        // 启动请求处理器
        start_request_processor(receiver, tw_clone);

        Self { sender, time_wheel }
    }

    /// 将任务添加到时间轮中进行调度
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
        let (response_tx, response_rx) = oneshot::channel();
        let request = TaskRequest::Add {
            timestamp,
            delay,
            key: key.into(),
            response: response_tx,
        };

        // 发送请求到通道
        self.sender
            .send(request)
            .await
            .map_err(|_| CronTaskError::TaskSendError)?;

        // 等待响应
        response_rx
            .await
            .map_err(|_| CronTaskError::TaskRecvError)?
    }

    /// 取消已添加的任务
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
        let (response_tx, response_rx) = oneshot::channel();
        let request = TaskRequest::Cancel {
            timestamp,
            delay,
            key: key.into(),
            response: response_tx,
        };

        // 发送请求到通道
        self.sender
            .send(request)
            .await
            .map_err(|_| CronTaskError::TaskSendError)?;

        // 等待响应
        response_rx
            .await
            .map_err(|_| CronTaskError::TaskRecvError)?
    }

    /// 获取时间轮实例
    ///
    /// # 返回值
    /// 返回时间轮实例的Arc引用
    pub fn time_wheel(&self) -> Arc<TimeWheel> {
        Arc::clone(&self.time_wheel)
    }
}

// 私有辅助函数
impl TaskScheduler {
    /// 处理任务请求
    async fn process_requests(
        mut receiver: mpsc::Receiver<TaskRequest>,
        time_wheel: Arc<TimeWheel>,
    ) {
        // 持续接收请求并处理
        while let Some(request) = receiver.recv().await {
            match request {
                TaskRequest::Add {
                    timestamp,
                    delay,
                    key,
                    response,
                } => {
                    let result = time_wheel.add_task(timestamp, delay, key).await;
                    // 忽略发送响应的错误，因为客户端可能已经取消了请求
                    let _ = response.send(result);
                }
                TaskRequest::Cancel {
                    timestamp,
                    delay,
                    key,
                    response,
                } => {
                    let result = time_wheel.del_task(timestamp, delay, key).await;
                    // 忽略发送响应的错误，因为客户端可能已经取消了请求
                    let _ = response.send(result);
                }
            }
        }
    }
}

/// 启动请求处理器
fn start_request_processor(receiver: mpsc::Receiver<TaskRequest>, time_wheel: Arc<TimeWheel>) {
    tokio::spawn(async move {
        TaskScheduler::process_requests(receiver, time_wheel).await;
    });
}
