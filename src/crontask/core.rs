use std::sync::Arc;
use tokio::sync::Mutex;
use crate::taskscheduler::TaskScheduler;
use crate::crontask::state::InnerState;
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
        let reload_name = RELOAD_TASK_NAME.to_string();
        
        // 使用更精确的调度间隔
        tokio::task::spawn(async move {
            let _ = instance_clone.schedule(
                chrono::Local::now().naive_local(), 
                reload_millis, 
                reload_name, 
                "__reload_tasks__".to_string(),
            ).await;
        });

        instance
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