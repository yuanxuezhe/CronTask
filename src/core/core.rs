// 标准库导入
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

// 外部 crate 导入
use chrono::Local;
use tokio::sync::RwLock;

// 内部模块导入
use crate::common::consts::RELOAD_TASK_NAME;
use crate::core::state::InnerState;
use crate::core::message_handler::MessageHandler;
use crate::basic::{MessageBus, TimeBus, TaskScheduler};

// 外部 crate 使用声明
use dbcore::Database;

// 导入日志宏

/// 核心任务调度管理器
pub struct CronTask {
    /// 任务调度器
    pub task_scheduler: Arc<TaskScheduler>,
    /// 内部状态，包含任务和任务详情，使用RwLock优化读多写少场景
    pub inner: Arc<RwLock<InnerState>>,
    /// 重新加载任务的时间间隔（毫秒）
    pub reload_interval: u64,
    /// 数据库连接
    pub db: Database,
    /// 消息总线
    pub message_bus: Arc<MessageBus>,
    
    /// 关闭标志，用于优雅地终止任务
    pub shutdown_flag: Arc<AtomicBool>,
}

impl CronTask {
    /// 创建新的 CronTask 实例
    ///
    /// # 参数
    /// * `reload_millis` - 重新加载任务的时间间隔（毫秒）
    /// * `tick_mills` - 时间轮滴答间隔（毫秒）
    /// * `total_slots` - 时间轮总槽数
    /// * `channel_buffer_size` - 消息通道缓冲区大小
    /// * `db` - 数据库连接
    ///
    /// # 返回值
    /// 返回一个 Arc 包装的 CronTask 实例
    pub fn new(
        reload_millis: u64,
        tick_mills: u64,
        total_slots: usize,
        channel_buffer_size: usize,
        db: Database,
    ) -> Arc<Self> {
        let message_bus = crate::basic::create_message_bus(channel_buffer_size);
        let time_bus = crate::basic::create_time_bus();

        let task_scheduler = Arc::new(crate::basic::create_task_scheduler(
            std::time::Duration::from_millis(tick_mills),
            total_slots,
            message_bus.clone(),
        ));

        let shutdown_flag = Arc::new(AtomicBool::new(false));

        let instance = Arc::new(Self {
            task_scheduler,
            inner: Arc::new(RwLock::new(InnerState {
                taskdetails: HashMap::new(),
                tasks: HashMap::new(),
            })),
            reload_interval: reload_millis,
            db,
            message_bus: message_bus.clone(),
            shutdown_flag: shutdown_flag.clone(),
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
        // 立即执行初始任务加载，而不是通过消息队列
        CronTask::reload_tasks(self).await;

        // 然后设置定期重新加载
        let reload_name = RELOAD_TASK_NAME.to_string();
        let reload_interval = self.reload_interval;
        let message_bus = self.message_bus.clone();

        tokio::task::spawn(async move { let _ = message_bus.send_schedule_task(
                Local::now().naive_local(),
                reload_interval,
                reload_name,
            );
        });
    }

    /// 处理消息总线中的消息
    async fn handle_messages(self: &Arc<Self>) {
        MessageHandler::handle_messages(self).await;
    }

    /// 优雅关闭系统
    fn _shutdown(&self) {
        // 设置关闭标志，通知所有任务退出
        self.shutdown_flag.store(true, Ordering::Relaxed);
    }
}

// 私有辅助函数实现
impl CronTask {
    /// 启动时间轮
    fn start_time_wheel(instance: &Arc<CronTask>, time_bus: Arc<TimeBus>) {
        let time_wheel = instance.task_scheduler.time_wheel();
        let time_bus_for_wheel = time_bus.clone();
        let shutdown_flag = instance.shutdown_flag.clone();

        // 创建一个持久的订阅者来防止通道关闭，并支持优雅终止
        let time_bus_subscriber = time_bus.clone();
        let shutdown_flag_sub = shutdown_flag.clone();
        tokio::spawn(async move {
            // 订阅时间总线但不处理消息，仅保持通道活跃
            let mut receiver = time_bus_subscriber.subscribe();

            // 定期检查关闭标志
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // 检查是否需要关闭
                        if shutdown_flag_sub.load(Ordering::Relaxed) {
                            break;
                        }
                    },
                    result = receiver.recv() => {
                        // 即使接收到消息也不做处理，只是为了保持通道活跃
                        if result.is_err() {
                            // 通道关闭，可以退出
                            break;
                        }
                    },
                }
            }
        });

        // 注册时间回调来驱动时间轮
        let shutdown_flag_callback = shutdown_flag.clone();
        tokio::spawn(async move {
            // 注册秒级回调
            time_bus_for_wheel
                .register_callback(0b000010, move |_pulse| {
                    let time_wheel_clone = time_wheel.clone();
                    tokio::spawn(async move {
                        time_wheel_clone
                            .run(|_timestamp| async move {
                                // 时间轮滴答回调
                            })
                            .await;
                    });
                })
                .await;

            // 定期检查关闭标志
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
            loop {
                interval.tick().await;
                if shutdown_flag_callback.load(Ordering::Relaxed) {
                    break;
                }
            }
        });
    }

    /// 启动消息处理器
    fn start_message_handler(instance: Arc<Self>) {
        let shutdown_flag = instance.shutdown_flag.clone();

        tokio::spawn(async move {
            // 克隆实例以满足'static生命周期要求
            let instance_clone = instance.clone();

            // 启动消息处理任务
            let message_handler_task =
                tokio::spawn(async move { instance_clone.handle_messages().await });

            // 在处理消息的同时定期检查关闭标志
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));

            tokio::select! {
                _ = interval.tick() => {
                    // 检查是否需要关闭
                    if shutdown_flag.load(Ordering::Relaxed) {
                        // 这里可以添加关闭逻辑，但由于我们使用的是Arc，任务会自然结束
                    }
                },
                result = message_handler_task => {
                    // 消息处理完成或出错退出
                    if result.is_err() {
                        // 任务被中止或出错
                    }
                },
            }
        });
    }
}
