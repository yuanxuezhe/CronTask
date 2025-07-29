use std::sync::Arc;
use tokio::sync::Mutex;
use crate::taskscheduler::TaskScheduler;
use crate::crontask::state::InnerState;
use dbcore::Database;
use std::collections::HashMap;
use crate::task::TaskDetail;
use log::{info, error}; // 添加日志导入

pub struct CronTask {
    /// 任务调度器
    pub taskscheduler: Arc<TaskScheduler>,
    /// 内部状态，包含任务和任务详情
    pub inner: Arc<Mutex<InnerState>>,
    /// 重新加载任务的时间间隔（毫秒）
    pub reload_interval: u64,
    /// 数据库连接
    pub db: Database,
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
        let instance = Arc::new(Self {
            taskscheduler: Arc::new(TaskScheduler::new(
                std::time::Duration::from_millis(tick_mills),
                total_slots,
                high_precision
            )),
            inner: Arc::new(Mutex::new(InnerState {
                taskdetails: Vec::new(),
                tasks: HashMap::new(),
            })),
            reload_interval: reload_millis,
            db,
        });

        let instance_clone = instance.clone();
        let reload_name = crate::consts::RELOAD_TASK_NAME.to_string();
        
        // 使用更精确的调度间隔
        let reload_interval = instance.reload_interval;
        
        tokio::spawn(async move {
            loop {
                match instance_clone.schedule(
                    chrono::Local::now().naive_local(), 
                    reload_interval, 
                    reload_name.clone(),
                    reload_name.clone(),
                ).await {
                    Ok(_) => {
                        info!("CronTask initialized successfully");
                        break;
                    },
                    Err(e) => {
                        error!("Failed to initialize CronTask: {}. Retrying in 5 seconds...", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    }
                }
            }
        });
        
        instance
    }

    /// 获取当前活跃的任务数量
    pub async fn active_task_count(&self) -> usize {
        let inner = self.inner.lock().await;
        inner.tasks.len()
    }

    /// 获取任务详情
    pub async fn get_task_details(&self) -> Vec<TaskDetail> {
        let inner = self.inner.lock().await;
        inner.taskdetails.clone()
    }

    /// 清除所有任务
    pub async fn clear_all_tasks(&self) {
        let mut inner = self.inner.lock().await;
        inner.tasks.clear();
        inner.taskdetails.clear();
        info!("All tasks cleared");
    }
}