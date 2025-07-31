use std::sync::Arc;
use tokio::sync::Mutex;
use crate::taskscheduler::TaskScheduler;
use crate::crontask::state::InnerState;
use crate::crontask::message_bus::{MessageBus, CronMessage};
use crate::crontask::time_bus::{TimeBus, TimeEvent};
use dbcore::Database;
use std::collections::HashMap;
use crate::task::TaskDetail;
use crate::comm::consts::RELOAD_TASK_NAME;
use log::info;
use chrono::Timelike;

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

        // 启动消息处理器
        let instance_message_handler = instance.clone();
        tokio::spawn(async move {
            instance_message_handler.handle_messages().await;
        });
        
        // 启动时间事件处理器
        let instance_time_handler = instance.clone();
        tokio::spawn(async move {
            instance_time_handler.handle_time_events().await;
        });
        
        // 启动时间生成器
        let time_bus_clone = time_bus.clone();
        tokio::spawn(async move {
            Self::run_time_generator(time_bus_clone).await;
        });
        
        // 发送初始重新加载任务消息
        let message_bus_clone = message_bus.clone();
        let reload_name = RELOAD_TASK_NAME.to_string();
        tokio::task::spawn(async move {
            let _ = message_bus_clone.send(CronMessage::ScheduleTask {
                timestamp: chrono::Local::now().naive_local(),
                delay_ms: reload_millis,
                key: reload_name,
                arg: "__reload_tasks__".to_string(),
            });
        });

        instance
    }
    
    /// 运行时间生成器，定期生成时间事件
    async fn run_time_generator(time_bus: Arc<TimeBus>) {
        let mut second_interval = tokio::time::interval(std::time::Duration::from_secs(1));
        
        // 记录上一次触发的时间，避免重复触发
        let mut last_minute = None;
        let mut last_hour = None;
        let mut last_day = None;
        
        loop {
            second_interval.tick().await;
            
            let now = TimeBus::now();
            
            // 总是发送秒事件
            let _ = time_bus.send(TimeEvent::Second(now));
            
            // 检查是否需要触发分钟事件
            if last_minute != Some(now.minute()) {
                last_minute = Some(now.minute());
                let _ = time_bus.send(TimeEvent::Minute(now));
            }
            
            // 检查是否需要触发小时事件
            if last_hour != Some(now.hour()) {
                last_hour = Some(now.hour());
                let _ = time_bus.send(TimeEvent::Hour(now));
            }
            
            // 检查是否需要触发天事件
            if last_day != Some(now.date_naive()) {
                last_day = Some(now.date_naive());
                let _ = time_bus.send(TimeEvent::Day(now));
            }
        }
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
                    self.on_call_back_inner(key, eventdata).await;
                },
            }
        }
    }
    
    /// 处理时间事件
    async fn handle_time_events(self: &Arc<Self>) {
        let mut receiver = self.time_bus.subscribe();
        
        while let Ok(event) = receiver.recv().await {
            match event {
                TimeEvent::Second(_) => {
                    // 每秒可以处理的逻辑
                },
                TimeEvent::Minute(_) => {
                    // 每分钟重新调度任务
                    self.reschedule_all().await;
                },
                TimeEvent::Hour(_) => {
                    // 每小时可以处理的逻辑
                },
                TimeEvent::Day(_) => {
                    // 每天可以处理的逻辑
                },
                TimeEvent::Custom(_, _) => {
                    // 自定义时间事件
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