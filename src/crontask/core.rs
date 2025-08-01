use std::sync::Arc;
use tokio::sync::Mutex;
use crate::taskscheduler::TaskScheduler;
use crate::crontask::state::InnerState;
use crate::bus::{message_bus::MessageBus, time_bus::TimeBus};
use crate::bus::message_bus::CronMessage;
use dbcore::Database;
use std::collections::HashMap;
use crate::task::TaskDetail;
use crate::comm::consts::RELOAD_TASK_NAME;
use log::info;

pub struct CronTask {
    /// 任务调度器
    pub taskscheduler: Arc<TaskScheduler>,
    /// 内部状态，包含任务和任务详情
    pub inner: Arc<Mutex<InnerState>>,
    /// 重新加载任务的时间间隔（毫秒）
    pub reload_interval: u64,
    /// 数据库连接
    pub db: Database,
    /// 消息总线
    pub message_bus: Arc<MessageBus>,
    /// 时间总线
    pub time_bus: Arc<TimeBus>,
}

impl CronTask {
    /// 创建新的CronTask实例
    /// 
    /// # 参数
    /// * `reload_millis` - 重新加载任务的时间间隔（毫秒）
    /// * `tick_mills` - 时间轮滴答间隔（毫秒）
    /// * `total_slots` - 时间轮总槽数
    /// * `high_precision` - 是否使用高精度模式
    /// * `db` - 数据库连接
    /// 
    /// # 返回值
    /// 返回一个Arc包装的CronTask实例
    pub fn new(reload_millis: u64, tick_mills: u64, total_slots: usize, high_precision: bool, db: Database) -> Arc<Self> {
        let task_scheduler = Arc::new(TaskScheduler::new(
            std::time::Duration::from_millis(tick_mills),
            total_slots,
            high_precision
        ));
        
        let message_bus = MessageBus::new();
        let time_bus = TimeBus::new();
        
        let instance = Arc::new(Self {
            taskscheduler: task_scheduler,
            inner: Arc::new(Mutex::new(InnerState {
                taskdetails: Vec::new(),
                tasks: HashMap::new(),
            })),
            reload_interval: reload_millis,
            db,
            message_bus: message_bus.clone(),
            time_bus: time_bus.clone(),
        });

        // 启动时间轮
        let time_wheel = instance.taskscheduler.time_wheel();
        let time_bus_for_wheel = time_bus.clone();
        tokio::spawn(async move {
            time_bus_for_wheel.register_callback(0b000010, move |_pulse| {
                let time_wheel_clone = time_wheel.clone();
                tokio::spawn(async move {
                    time_wheel_clone.run(|_timestamp| async move {
                        // 时间轮滴答回调，可以在这里添加日志或其他处理
                    }).await;
                });
            }).await;
        });

        // 启动消息处理器
        let instance_message_handler = instance.clone();
        tokio::spawn(async move {
            instance_message_handler.handle_messages().await;
        });
        
        // 发送初始重新加载任务消息
        let reload_name = RELOAD_TASK_NAME.to_string();
        tokio::task::spawn(async move {
            let _ = message_bus.send(CronMessage::ScheduleTask {
                timestamp: chrono::Local::now().naive_local(),
                delay_ms: reload_millis,
                key: reload_name,
                arg: "__reload_tasks__".to_string(),
            });
        });

        instance
    }
    
    /// 处理消息总线中的消息
    async fn handle_messages(self: &Arc<Self>) {
        let mut receiver = self.message_bus.subscribe();
        
        while let Ok(message) = receiver.recv().await {
            match message {
                CronMessage::ScheduleTask { timestamp, delay_ms, key, arg } => {
                    self.handle_schedule_task(timestamp, delay_ms, key, arg).await;
                },
                CronMessage::CancelTask { timestamp, delay_ms, key } => {
                    self.handle_cancel_task(timestamp, delay_ms, key).await;
                },
                CronMessage::ReloadTasks => {
                    self.reload_tasks().await;
                },
                CronMessage::ExecuteTask { key, eventdata } => {
                    let _ = self.on_call_back_inner(key, eventdata).await;
                },
            }
        }
    }
    
    /// 处理任务调度消息
    async fn handle_schedule_task(&self, timestamp: chrono::NaiveDateTime, delay_ms: u64, key: String, arg: String) {
        println!("crontask::schedule 调度任务: {} at {} + {}ms", key, timestamp, delay_ms);
        let result = self.taskscheduler.schedule(
            timestamp,
            std::time::Duration::from_millis(delay_ms),
            key.clone(),
            arg,
            {
                let message_bus = self.message_bus.clone();
                move |key, eventdata| {
                    let _ = message_bus.send(CronMessage::ExecuteTask { key, eventdata });
                }
            },
        ).await;
        
        if let Err(e) = result {
            eprintln!("任务调度失败: {} - {}", key, e);
        }
    }
    
    /// 处理任务取消消息
    async fn handle_cancel_task(&self, timestamp: chrono::NaiveDateTime, delay_ms: u64, key: String) {
        println!("crontask::cancel 取消任务: {} at {} + {}ms", key, timestamp, delay_ms);
        let result = self.taskscheduler.cancel(
            timestamp,
            std::time::Duration::from_millis(delay_ms),
            key.clone(),
        ).await;
        
        if let Err(e) = result {
            eprintln!("任务取消失败: {} - {}", key, e);
        }
    }
    
    /// 获取活跃任务数量
    /// 
    /// # 返回值
    /// 返回当前监控中的任务数量
    pub async fn active_task_count(&self) -> usize {
        let guard = self.inner.lock().await;
        guard.taskdetails
            .iter()
            .filter(|detail| detail.status == crate::comm::consts::TASK_STATUS_MONITORING)
            .count()
    }
    
    /// 获取所有任务详情
    /// 
    /// # 返回值
    /// 返回所有任务详情的副本
    pub async fn get_task_details(&self) -> Vec<TaskDetail> {
        let guard = self.inner.lock().await;
        guard.taskdetails.clone()
    }
    
    /// 清空所有任务
    pub async fn clear_all_tasks(&self) {
        let mut guard = self.inner.lock().await;
        guard.taskdetails.clear();
        guard.tasks.clear();
        info!("所有任务已清空");
    }
}