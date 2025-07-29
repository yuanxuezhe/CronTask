use std::sync::Arc;
use tokio::sync::Mutex;
use crate::taskscheduler::TaskScheduler;
use crate::crontask::state::InnerState;
use dbcore::Database;
use std::collections::HashMap;

pub struct CronTask {
    pub taskscheduler: Arc<TaskScheduler>,
    pub inner: Arc<Mutex<InnerState>>,
    pub reload_interval: u64,
    pub db: Database,
}

impl CronTask {
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
        
        tokio::spawn(async move {
            let _ = instance_clone.schedule(
                chrono::Local::now().naive_local(), 
                2000, 
                reload_name.clone(), 
                reload_name.clone()
            ).await;
            println!("CronTask initialized");
        });
        
        instance
    }
} 