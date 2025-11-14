// 标准库导入
use std::sync::Arc;
use std::collections::HashMap;

// 外部 crate 导入
use tokio::sync::Mutex;
use chrono::Local;

// 内部模块导入
use crate::scheduler::task_scheduler::TaskScheduler;
use crate::core::state::InnerState;
use crate::message::message_bus::{MessageBus, CronMessage};
use crate::message::time_bus::TimeBus;
use crate::common::consts::RELOAD_TASK_NAME;
use crate::task_engine::model::TaskDetail;

// 外部 crate 使用声明
use dbcore::Database;

// 导入日志宏
use crate::info_log;

/// 核心任务调度管理器
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
    /// 创建新的 CronTask 实例
    /// 
    /// # 参数
    /// * `reload_millis` - 重新加载任务的时间间隔（毫秒）
    /// * `tick_mills` - 时间轮滴答间隔（毫秒）
    /// * `total_slots` - 时间轮总槽数
    /// * `db` - 数据库连接
    /// 
    /// # 返回值
    /// 返回一个 Arc 包装的 CronTask 实例
    pub fn new(
        reload_millis: u64, 
        tick_mills: u64, 
        total_slots: usize, 
        db: Database
    ) -> Arc<Self> {
        let task_scheduler = Arc::new(TaskScheduler::new(
            std::time::Duration::from_millis(tick_mills),
            total_slots
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

        // 设置全局 CronTask 实例，以便日志宏可以访问消息总线
        crate::common::log::set_cron_task(&instance);

        // 启动时间轮
        Self::start_time_wheel(&instance, time_bus);
        
        // 启动消息处理器
        Self::start_message_handler(instance.clone());

        instance
    }
    
    /// 初始化加载任务
    /// 在 CronTask 构造完成后调用此方法来触发初始任务加载
    pub async fn init_load_tasks(self: &Arc<Self>) {
        // 发送初始重新加载任务消息
        let reload_name = RELOAD_TASK_NAME.to_string();
        let reload_interval = self.reload_interval;
        let message_bus = self.message_bus.clone();
        
        tokio::task::spawn(async move {
            let _ = message_bus.send(CronMessage::ScheduleTask {
                timestamp: Local::now().naive_local(),
                delay_ms: reload_interval,
                key: reload_name,
                arg: "__reload_tasks__".to_string(),
            });
        });
    }
    
    /// 处理消息总线中的消息
    async fn handle_messages(self: &Arc<Self>) {
        crate::core::handlers::message_handler::MessageHandler::handle_messages(self).await;
    }
}

// 私有辅助函数实现
impl CronTask {
    /// 启动时间轮
    fn start_time_wheel(instance: &Arc<CronTask>, time_bus: Arc<TimeBus>) {
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
    }
    
    /// 启动消息处理器
    fn start_message_handler(instance: Arc<Self>) {
        tokio::spawn(async move {
            instance.handle_messages().await;
        });
    }
    

}