// src/crontask.rs
use chrono::{NaiveDateTime, Utc};
use std::time::Duration;
use crate::taskscheduler::TaskScheduler;
use chrono_tz::Asia::Shanghai;
use std::sync::Arc;
use crate::task::Task;
use tokio::sync::Mutex;
use dbcore::Database;

pub struct CronTask {
    taskscheduler: Arc<TaskScheduler>,
    tasks: Arc<Mutex<Vec<Task>>>,
    reload_interval: u64,  // 时间间隔 微妙
    db: Database,
}

impl CronTask {
    pub fn new(reload_millis: u64, tick_mills: u64, total_slots: usize, high_precision: bool, db:Database) -> Arc<Self> {
        let instance = Arc::new(Self {
            // 正确初始化 TaskScheduler
            taskscheduler: Arc::new(TaskScheduler::new(Duration::from_millis(tick_mills),total_slots,high_precision)),
            tasks: Arc::new(Mutex::new(Vec::new())),
            reload_interval: reload_millis,
            db: db,
        });

        // 启动后台任务加载线程
        instance.clone().start_reloader();
        instance
    }

    /// 启动后台重载线程
    fn start_reloader(self: &Arc<Self>) {
        let self_clone = Arc::clone(self);
        // 使用 tokio::spawn 启动异步任务
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(self_clone.reload_interval));
            
            loop {
                interval.tick().await;
                self_clone.reload_tasks().await;
            }
        });
    }

    /// 重新加载任务
    async fn reload_tasks(self: &Arc<Self>) {
        //print!(".");
        // 2. 更新任务列表（作用域内持有锁）
        let mut guard = self.tasks.lock().await;
        // 1. 从数据库或其他存储加载新任务
        let new_tasks = self.load_tasks_from_db().await;
        let new_tasks = new_tasks.expect("Failed to load tasks from DB");

        // 2. 更新任务列表
        // 只加载taskid在self.tasks中不存在的任务
        let mut ext_tasks = Vec::new();
        for task in new_tasks.iter() {
            if !guard.iter().any(|t| t.taskid == task.taskid) {
                ext_tasks.push(task.clone());
            }
        }
        guard.extend(ext_tasks.clone());
        // 如果任务列表不为空，列出新加的任务的taskid和taskname
        if !ext_tasks.is_empty() {
            println!("新加载的任务:");
            for task in ext_tasks.iter() {
                println!("        taskid: {}, taskname: {}", task.taskid, task.taskname);
            }
        }

        // 3. 重新调度所有任务
        self.clone().reschedule_all(&ext_tasks).await;
    }

    /// 从数据库加载任务（示例实现）
    async fn load_tasks_from_db(&self) -> Result<Vec<Task>,Box<dyn std::error::Error + Send + Sync>> {
        let rs = self.db.open("select * from task where taskid >= ?").set_param(0).query(&self.db).await?;
        // 2. 遍历结果集
        let mut tasks = Vec::new();
        for row_data in rs.iter() {
            tasks.push(Task {
                taskid: row_data.get("taskid")?,
                taskname: row_data.get("taskname")?,
                start_date: row_data.get("start_date")?,
                end_date: row_data.get("end_date")?,
                cycle_type: row_data.get("cycle_type")?,
                period: row_data.get("period")?,
                time_point: row_data.get("time_point")?,
                retry_type: row_data.get("retry_type")?,
                retry_interval: row_data.get("retry_interval")?,
                retry_count: row_data.get("retry_count")?,
                current_trigger_count: 0,
                status: row_data.get("status")?,
                discribe: row_data.get("discribe")?,
            });
        }

        Ok(tasks)
    }

    /// 重新调度所有任务
    async fn reschedule_all(self: &Arc<Self>, ext_tasks: &Vec<Task>) {
        //let tasks = self.tasks.lock().await;

        for task in ext_tasks.iter() {
            self.add_task(task.clone());
        }
    }

    pub fn add_task(
        self: &Arc<Self>,
        _task: Task
    ) {
        let timepoint = _task.next_n_schedules(10);
        println!("最近10天内的触发时间点: {:?}", timepoint);

        // 循环添加任务
        for i in 0..timepoint.len() {
            //println!("触发时间点: {}", timepoint[i]);
            self.schedule(
                timepoint[i],
                (_task.retry_interval * _task.current_trigger_count).try_into().unwrap(),
                format!("{}|{}", _task.taskid, timepoint[i]),
            );
        }
    }

    fn schedule(
        self: &Arc<Self>,
        timestamp: NaiveDateTime, 
        millis: u64, 
        arg: String, 
    ) {
        // 克隆 Arc 引用
        let self_clone = self.clone();

        match self.taskscheduler.schedule(
            timestamp,
            Duration::from_millis(millis),
            arg,
            move |name| self_clone.on_call_back(name)
         ).recv() {
            Ok(msg) => println!("Result: {}", msg), // 直接打印消息
            Err(e) => println!("Channel error: {}", e),
        }
    }

    fn on_call_back(self: &Arc<Self>, name: String) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            this.on_call_back_inner(name).await;
        });
    }

    async fn on_call_back_inner(self: &Arc<Self>,name: String) {
        let now = Utc::now().with_timezone(&Shanghai);
        println!("[{}] 执行任务xxxx: {}", now, name);
        
        // 1. 解析任务ID和时间点
        let parts: Vec<&str> = name.split('|').collect();
        if parts.len() != 2 {
            println!("任务名称格式错误: {}", name);
            return;
        }
        let task_id: i32 = match parts[0].parse() {
            Ok(id) => id,
            Err(_) => {
                println!("任务ID解析失败: {}", parts[0]);
                return;
            }
        };

        let time_point: NaiveDateTime = match NaiveDateTime::parse_from_str(parts[1], "%Y-%m-%d %H:%M:%S") {
            Ok(tp) => tp,
            Err(_) => {
                println!("时间点解析失败: {}", parts[1]);
                return;
            }
        };

        // 通过name定位task并重新订阅
        let mut tasks = self.tasks.lock().await;
        for task in tasks.iter_mut() {
            if task.taskid == task_id {
                if task.current_trigger_count >= task.retry_count {
                    println!("任务 {} 达到最大重试次数，停止调度", task.taskname);
                    return;
                }
                // 重新订阅
                task.current_trigger_count += 1;

                self.schedule(
                    time_point,
                    (task.retry_interval * task.current_trigger_count).try_into().unwrap(),
                    name.clone(),
                );
            }
        }
    }
}