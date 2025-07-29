use tokio::sync::mpsc;
use chrono::NaiveDateTime;
use std::time::Duration;
use tokio::sync::oneshot;
use super::request::TaskRequest;
use std::sync::Arc;
use crate::taskscheduler::timewheel::TimeWheel;

const CHANNEL_BUFFER_SIZE: usize = 1000;

pub struct TaskScheduler {
    pub sender: mpsc::Sender<TaskRequest>,
}

impl TaskScheduler {
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
    
    async fn process_requests(mut receiver: mpsc::Receiver<TaskRequest>, time_wheel: Arc<TimeWheel>) {
        while let Some(request) = receiver.recv().await {
            match request {
                TaskRequest::Add { time, interval, key, arg, task, resp } => {
                    let result = time_wheel.add_task(time, interval, key, arg, task).await;
                    let _ = resp.send(result);
                }
                TaskRequest::Cancel { time, interval, key, resp } => {
                    let result = time_wheel.del_task(time, interval, key).await;
                    let _ = resp.send(result);
                }
            }
        }
    }

    pub async fn schedule<F, K>(
        &self, 
        timestamp: NaiveDateTime, 
        delay: Duration, 
        key: K,
        arg: String, 
        task: F
    ) -> Result<String, String>
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
        self.sender.send(req).await.map_err(|_| "发送失败".to_string())?;
        resp_rx.await.unwrap_or_else(|_| Err("接收失败".to_string()))
    }
    pub async fn cancel<K>(
        &self,
        timestamp: NaiveDateTime,
        delay: Duration,
        key: K,
    ) -> Result<String, String>
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
        self.sender.send(req).await.map_err(|_| "发送失败".to_string())?;
        resp_rx.await.unwrap_or_else(|_| Err("接收失败".to_string()))
    }
}