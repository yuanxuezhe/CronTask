use tokio::sync::mpsc;
use chrono::NaiveDateTime;
use std::time::Duration;
use tokio::sync::oneshot;
use super::request::TaskRequest;
use std::sync::Arc;
use crate::taskscheduler::timewheel::TimeWheel;

const CHANNEL_BUFFER_SIZE: usize = 1000;

#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    #[error("发送任务请求失败")]
    SendError,
    #[error("接收响应失败")]
    RecvError,
    #[error("时间轮错误: {0}")]
    TimeWheelError(String),
}

pub struct TaskScheduler {
    /// 任务请求发送通道
    pub sender: mpsc::Sender<TaskRequest>,
}

impl TaskScheduler {
    /// 创建新的任务调度器
    /// 
    /// # 参数
    /// * `tick_duration` - 时间轮滴答间隔
    /// * `total_slots` - 时间轮总槽数
    /// * `high_precision` - 是否使用高精度模式
    /// 
    /// # 返回值
    /// 返回新的任务调度器实例
    pub fn new(tick_duration: Duration, total_slots: usize, high_precision: bool) -> Self {
        let (sender, receiver) = mpsc::channel::<TaskRequest>(CHANNEL_BUFFER_SIZE);
        let time_wheel = Arc::new(TimeWheel::new(tick_duration, total_slots));
        let tw_clone: Arc<TimeWheel> = Arc::clone(&time_wheel);
        tokio::spawn(async move {
            Self::process_requests(receiver, tw_clone).await;
        });
        let tw_driver: Arc<TimeWheel> = Arc::clone(&time_wheel);
        tokio::spawn(async move {
            if high_precision {
                tw_driver.run_highprecision().await;
            } else {
                tw_driver.run().await;
            }
        });
        Self { sender }
    }
    
    /// 处理来自通道的任务添加和取消请求
    /// 
    /// # 参数
    /// * `receiver` - 任务请求接收通道
    /// * `time_wheel` - 时间轮实例
    async fn process_requests(mut receiver: mpsc::Receiver<TaskRequest>, time_wheel: Arc<TimeWheel>) {
        while let Some(request) = receiver.recv().await {
            match request {
                TaskRequest::Add { time, interval, key, arg, task, resp } => {
                    let result = time_wheel.add_task(time, interval, key, arg, task).await
                        .map_err(|e| SchedulerError::TimeWheelError(e.to_string()));
                    let _ = resp.send(result);
                }
                TaskRequest::Cancel { time, interval, key, resp } => {
                    let result = time_wheel.del_task(time, interval, key).await
                        .map_err(|e| SchedulerError::TimeWheelError(e.to_string()));
                    let _ = resp.send(result);
                }
            }
        }
    }

    /// 将任务添加到时间轮中进行调度
    /// 
    /// # 参数
    /// * `timestamp` - 任务触发时间
    /// * `delay` - 延迟时间
    /// * `key` - 任务唯一标识符
    /// * `arg` - 任务参数
    /// * `task` - 任务执行函数
    /// 
    /// # 返回值
    /// 成功时返回任务key，失败时返回错误信息
    pub async fn schedule<F, K>(
        &self, 
        timestamp: NaiveDateTime, 
        delay: Duration, 
        key: K,
        arg: String, 
        task: F
    ) -> Result<String, SchedulerError>
    where
        F: Fn(String, String) + Send + Sync + 'static,
        K: ToString,
    {
        let (resp_tx, resp_rx) = oneshot::channel();
        let arc_task = Arc::new(task);
        let req = TaskRequest::Add {
            time: timestamp,
            interval: delay,
            key: key.to_string(),
            arg,
            task: arc_task,
            resp: resp_tx,
        };
        self.sender.send(req).await.map_err(|_| SchedulerError::SendError)?;
        resp_rx.await.map_err(|_| SchedulerError::RecvError)?
    }
    
    /// 从时间轮中移除指定任务
    /// 
    /// # 参数
    /// * `timestamp` - 任务原定触发时间
    /// * `delay` - 原定延迟时间
    /// * `key` - 任务唯一标识符
    /// 
    /// # 返回值
    /// 成功时返回操作结果信息，失败时返回错误信息
    pub async fn cancel<K>(
        &self,
        timestamp: NaiveDateTime,
        delay: Duration,
        key: K,
    ) -> Result<String, SchedulerError>
    where
        K: ToString,
    {
        let (resp_tx, resp_rx) = oneshot::channel();
        let req = TaskRequest::Cancel {
            time: timestamp,
            interval: delay,
            key: key.to_string(),
            resp: resp_tx,
        };
        self.sender.send(req).await.map_err(|_| SchedulerError::SendError)?;
        resp_rx.await.map_err(|_| SchedulerError::RecvError)?
    }
}