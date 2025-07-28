use chrono::{Local, NaiveDateTime, Utc};
use std::time::Duration;
use crate::taskscheduler::TaskScheduler;
use chrono_tz::Asia::Shanghai;
use std::sync::Arc;
use crate::task::{Task, TaskDetail};
use dbcore::Database;
use std::collections::{HashMap, HashSet};
use std::mem::drop;
use tokio::sync::Mutex;
use crate::consts::*;

pub struct InnerState {
    pub taskdetails: Vec<TaskDetail>,
    pub tasks: HashMap<i32, Task>,
}

pub struct CronTask {
    taskscheduler: Arc<TaskScheduler>,
    inner: Arc<Mutex<InnerState>>,
    reload_interval: u64,
    db: Database,
}

impl CronTask {
    pub fn new(reload_millis: u64, tick_mills: u64, total_slots: usize, high_precision: bool, db: Database) -> Arc<Self> {
        let instance = Arc::new(Self {
            taskscheduler: Arc::new(TaskScheduler::new(
                Duration::from_millis(tick_mills),
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
        
        tokio::spawn(async move {
            let _ = instance_clone.schedule(
                Local::now().naive_local(), 
                2000, 
                reload_name.clone(), 
                reload_name.clone()
            ).await;
            println!("CronTask initialized");
        });
        
        instance
    }

    /// 重新加载任务
    async fn reload_tasks(self: &Arc<Self>) {
        // 从数据库加载新任务
        let new_tasks = match self.load_tasks_from_db().await {
            Ok(tasks) => tasks,
            Err(e) => {
                eprintln!("Failed to load tasks from DB: {}", e);
                return;
            }
        };

        // 预先计算所有新任务的时间点，减少锁内计算
        let mut new_task_details = Vec::new();
        let mut new_keys = HashSet::new();

        for task in new_tasks.values() {
            let timepoints = task.next_n_schedules(10);
            if timepoints.is_empty() {
                continue;
            }

            for tp in timepoints {
                let key = format!("{}{}{}", task.taskid, TASK_KEY_SEPARATOR, tp);
                new_keys.insert(key.clone());

                new_task_details.push(TaskDetail {
                    taskid: task.taskid,
                    timepoint: tp.to_string(),
                    current_trigger_count: 0,
                    status: TASK_STATUS_UNMONITORED,
                    tag: TASK_TAG_NEW,
                });
            }
        }

        // 获取锁并更新状态
        let mut guard = self.inner.lock().await;
        
        // 创建旧任务明细键映射（优化：使用更高效的数据结构）
        let old_keys: HashSet<String> = guard.taskdetails
            .iter()
            .map(|detail| format!("{}{}{}", detail.taskid, TASK_KEY_SEPARATOR, detail.timepoint))
            .collect();

        // 标记需要保留的任务
        for detail in guard.taskdetails.iter_mut() {
            let key = format!("{}{}{}", detail.taskid, TASK_KEY_SEPARATOR, detail.timepoint);
            if new_keys.contains(&key) && detail.status == TASK_STATUS_MONITORING {
                detail.tag = TASK_TAG_KEEP;
            } else {
                detail.tag = TASK_TAG_DELETE;
            }
        }

        // 添加新任务（优化：只添加真正的新任务）
        for detail in new_task_details {
            let key = format!("{}{}{}", detail.taskid, TASK_KEY_SEPARATOR, detail.timepoint);
            if !old_keys.contains(&key) {
                guard.taskdetails.push(detail);
            }
        }

        // 更新任务映射
        guard.tasks = new_tasks;

        drop(guard);

        // 重新调度所有任务
        self.clone().reschedule_all().await;
    }

    /// 从数据库加载任务
    pub async fn load_tasks_from_db(&self) -> Result<HashMap<i32, Task>, Box<dyn std::error::Error>> {
        let rs = self.db.open("select * from task where taskid >= ?").set_param(0).query(&self.db).await?;
        let mut tasks = HashMap::new();

        for row_data in rs.iter() {
            let task = Task {
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
                status: row_data.get("status")?,
                discribe: row_data.get("discribe")?,
            };

            tasks.insert(task.taskid, task);
        }

        Ok(tasks)
    }

    /// 从缓存中获取所有任务
    pub async fn get_all_tasks_from_cache(&self) -> HashMap<i32, Task> {
        let guard = self.inner.lock().await;
        guard.tasks.clone()
    }

    /// 重新调度所有任务
    async fn reschedule_all(self: &Arc<Self>) {
        // 收集需要处理的任务信息
        let mut to_cancel = Vec::new();
        let mut to_schedule = Vec::new();
        let mut taskdetail_updates = Vec::new();

        {
            let mut guard = self.inner.lock().await;
            let InnerState { taskdetails, tasks } = &mut *guard;

            for taskdetail in taskdetails.iter_mut() {
                let task = match tasks.get(&taskdetail.taskid) {
                    Some(task) => task,
                    None => {
                        eprintln!("任务ID {} 不存在", taskdetail.taskid);
                        continue;
                    }
                };

                let ndt = match NaiveDateTime::parse_from_str(&taskdetail.timepoint, DATETIME_FORMAT) {
                    Ok(dt) => dt,
                    Err(e) => {
                        eprintln!("时间点解析失败: {} - {}", taskdetail.timepoint, e);
                        continue;
                    }
                };

                let task_key = format!("{}{}{}", task.taskid, TASK_KEY_SEPARATOR, taskdetail.timepoint);
                let delay_ms = (task.retry_interval * taskdetail.current_trigger_count) as u64;

                // 处理需要取消的任务
                if taskdetail.tag == TASK_TAG_DELETE {
                    if taskdetail.status == TASK_STATUS_MONITORING {
                        to_cancel.push((ndt, delay_ms, task_key));
                    }
                    continue;
                }

                // 跳过已在监控中的任务
                if taskdetail.status != TASK_STATUS_UNMONITORED {
                    continue;
                }
                
                // 收集需要调度的任务
                to_schedule.push((
                    ndt,
                    delay_ms,
                    task_key.clone(),
                    self.build_task_message(task.discribe.clone(), taskdetail.current_trigger_count),
                    taskdetail.taskid,
                    taskdetail.timepoint.clone(),
                ));

                // 记录需要更新的任务明细
                taskdetail_updates.push((taskdetail.taskid, taskdetail.timepoint.clone()));
            }
        }

        // 批量取消任务
        for (ndt, delay_ms, task_key) in to_cancel {
            match self.cancel(ndt, delay_ms, task_key.clone()).await {
                Ok(_) => println!("任务取消成功: {}", task_key),
                Err(e) => eprintln!("任务取消失败: {} - {}", task_key, e),
            }
        }

        // 批量调度任务
        for (ndt, delay_ms, task_key, message, taskid, timepoint) in to_schedule {
            match self.schedule(ndt, delay_ms, task_key.clone(), message).await {
                Ok(_) => {
                    // 更新任务状态
                    let mut guard = self.inner.lock().await;
                    if let Some(detail) = guard.taskdetails
                        .iter_mut()
                        .find(|d| d.taskid == taskid && d.timepoint == timepoint) {
                        detail.status = TASK_STATUS_MONITORING;
                    }
                    println!("任务调度成功: {}", task_key);
                },
                Err(e) => {
                    eprintln!("任务调度失败: {} - {}", task_key, e);
                },
            }
        }

        // 清理已删除的任务
        let mut guard = self.inner.lock().await;
        guard.taskdetails.retain(|detail| detail.tag != TASK_TAG_DELETE || detail.status != TASK_STATUS_UNMONITORED);
    }

    fn build_task_message(&self, discribe: String, current_trigger_count: i32) -> String {
        if current_trigger_count == 0 {
            discribe
        } else {
            format!("{}(重复提醒第{}次)", discribe, current_trigger_count)
        }
    }

    async fn schedule(
        self: &Arc<Self>,
        timestamp: NaiveDateTime, 
        millis: u64, 
        key: String,
        arg: String, 
    ) -> Result<String, String> {
        println!("crontask::schedule 调度任务: {} at {} + {}ms", key, timestamp, millis);
        
        let self_clone = self.clone();
        self.taskscheduler.schedule(
            timestamp,
            Duration::from_millis(millis),
            key,
            arg,
            move |key, eventdata| self_clone.on_call_back(key, eventdata),
        ).await
    }

    async fn cancel(
        self: &Arc<Self>,
        timestamp: NaiveDateTime, 
        millis: u64, 
        key: String,
    ) -> Result<String, String> {
        println!("crontask::cancel 取消任务: {} at {} + {}ms", key, timestamp, millis);
        self.taskscheduler.cancel(
            timestamp,
            Duration::from_millis(millis),
            key,
        ).await
    }

    fn on_call_back(self: &Arc<Self>, key: String, eventdata: String) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            this.on_call_back_inner(key, eventdata).await;
        });
    }

    async fn on_call_back_inner(self: &Arc<Self>, key: String, eventdata: String) {
        let now = Utc::now().with_timezone(&Shanghai);
        println!("[{}] 执行任务[{}]: {}", now, key, eventdata);

        if key == RELOAD_TASK_NAME {
            // 重新调度下一次 reload
            let _ = self.schedule(
                Local::now().naive_local(), 
                self.reload_interval, 
                key.clone(), 
                eventdata.clone()
            ).await;
    
            // 调用 reload_tasks
            self.reload_tasks().await;
            return;
        }
        
        // 解析任务ID和时间点
        let parts: Vec<&str> = key.split(TASK_KEY_SEPARATOR).collect();
        if parts.len() != 2 {
            eprintln!("任务名称格式错误: {}", key);
            return;
        }
        
        let task_id: i32 = match parts[0].parse() {
            Ok(id) => id,
            Err(_) => {
                eprintln!("任务ID解析失败: {}", parts[0]);
                return;
            }
        };

        let time_point: NaiveDateTime = match NaiveDateTime::parse_from_str(parts[1], DATETIME_FORMAT) {
            Ok(tp) => tp,
            Err(_) => {
                eprintln!("时间点解析失败: {}", parts[1]);
                return;
            }
        };

        // 优化：减少锁持有时间，先获取必要数据
        let (task, taskdetail) = {
            let guard = self.inner.lock().await;
            
            let task = match guard.tasks.get(&task_id) {
                Some(task) => task.clone(),
                None => {
                    eprintln!("任务ID {} 不存在", task_id);
                    return;
                }
            };

            let taskdetail = guard.taskdetails
                .iter()
                .find(|detail| format!("{}{}{}", detail.taskid, TASK_KEY_SEPARATOR, detail.timepoint) == key)
                .cloned();

            (task, taskdetail)
        };

        let mut taskdetail = match taskdetail {
            Some(detail) => detail,
            None => {
                eprintln!("任务明细不存在: {}", key);
                return;
            }
        };

        if taskdetail.current_trigger_count >= task.retry_count {
            println!("任务 {} 达到最大重试次数，停止调度", task.taskname);
            taskdetail.status = TASK_STATUS_UNMONITORED;
            
            // 更新状态
            let mut guard = self.inner.lock().await;
            if let Some(detail) = guard.taskdetails
                .iter_mut()
                .find(|d| format!("{}{}{}", d.taskid, TASK_KEY_SEPARATOR, d.timepoint) == key) {
                *detail = taskdetail;
            }
            return;
        }
        
        // 重新订阅
        taskdetail.current_trigger_count += 1;

        match self.schedule(
            time_point,
            (task.retry_interval * taskdetail.current_trigger_count).try_into().unwrap(),
            key.clone(),
            self.build_task_message(task.discribe.clone(), taskdetail.current_trigger_count),
        ).await {
            Ok(_) => {
                taskdetail.status = TASK_STATUS_RETRY;
                println!("任务重试调度成功: {}", key);
            },
            Err(e) => {
                taskdetail.status = TASK_STATUS_UNMONITORED;
                eprintln!("任务重试调度失败: {} - {}", key, e);
            },
        }

        // 更新状态
        let mut guard = self.inner.lock().await;
        if let Some(detail) = guard.taskdetails
            .iter_mut()
            .find(|d| format!("{}{}{}", d.taskid, TASK_KEY_SEPARATOR, d.timepoint) == key) {
            *detail = taskdetail;
        }
    }
}