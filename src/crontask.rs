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

// 常量定义
const RELOAD_TASK_NAME: &str = "__reload_tasks__";
const DATETIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S";
const TASK_KEY_SEPARATOR: &str = "|";

// 任务状态常量
const TASK_STATUS_UNMONITORED: i32 = 0;
const TASK_STATUS_MONITORING: i32 = 1;
const TASK_STATUS_RETRY: i32 = 2;

// 任务标签常量
const TASK_TAG_DELETE: i32 = 0;
const TASK_TAG_KEEP: i32 = 1;
const TASK_TAG_NEW: i32 = 2;

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

        let mut guard = self.inner.lock().await;
        guard.tasks = new_tasks;

        // 生成新任务明细
        let mut task_details_new = Vec::new();
        let mut new_keys = HashSet::new();

        for task in guard.tasks.values() {
            let timepoints = task.next_n_schedules(10);
            if timepoints.is_empty() {
                continue;
            }
            println!("最近10天内的触发时间点: {:?}", timepoints);

            for tp in timepoints {
                let key = format!("{}{}{}", task.taskid, TASK_KEY_SEPARATOR, tp);
                new_keys.insert(key.clone());

                task_details_new.push(TaskDetail {
                    taskid: task.taskid,
                    timepoint: tp.to_string(),
                    current_trigger_count: 0,
                    status: TASK_STATUS_UNMONITORED,
                    tag: TASK_TAG_NEW,
                });
            }
        }

        // 创建旧任务明细键映射
        let mut old_map: HashMap<String, usize> = HashMap::new();
        for (idx, detail) in guard.taskdetails.iter().enumerate() {
            let key = format!("{}{}{}", detail.taskid, TASK_KEY_SEPARATOR, detail.timepoint);
            old_map.insert(key, idx);
        }

        // 标记和修改原始 taskdetails
        for detail in guard.taskdetails.iter_mut() {
            let key = format!("{}{}{}", detail.taskid, TASK_KEY_SEPARATOR, detail.timepoint);
            if new_keys.contains(&key) && detail.status == TASK_STATUS_MONITORING {
                detail.tag = TASK_TAG_KEEP;
            } else {
                detail.tag = TASK_TAG_DELETE;
            }
        }

        // 添加新任务
        for detail in task_details_new {
            let key = format!("{}{}{}", detail.taskid, TASK_KEY_SEPARATOR, detail.timepoint);
            if !old_map.contains_key(&key) {
                let mut new_detail = detail;
                new_detail.tag = TASK_TAG_NEW;
                guard.taskdetails.push(new_detail);
            }
        }

        drop(guard);

        // 重新调度所有任务
        self.clone().reschedule_all().await;
    }

    /// 从数据库加载任务
    async fn load_tasks_from_db(&self) -> Result<HashMap<i32, Task>, Box<dyn std::error::Error + Send + Sync>> {
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

    /// 重新调度所有任务
    async fn reschedule_all(self: &Arc<Self>) {
        let mut guard = self.inner.lock().await;
        let InnerState { taskdetails, tasks } = &mut *guard;

        for taskdetail in taskdetails.iter_mut() {
            let task = match tasks.get_mut(&taskdetail.taskid) {
                Some(task) => task,
                None => {
                    println!("任务ID {} 不存在", taskdetail.taskid);
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

            // 处理需要取消的任务
            if taskdetail.tag == TASK_TAG_DELETE {
                println!("TaskID:{}({}|{})已完成", task.taskid, task.taskname, taskdetail.timepoint);
                
                if taskdetail.status == TASK_STATUS_MONITORING {
                    match self.cancel(
                        ndt,
                        (task.retry_interval * taskdetail.current_trigger_count).try_into().unwrap(),
                        format!("{}{}{}", task.taskid, TASK_KEY_SEPARATOR, taskdetail.timepoint),
                    ).await {
                        Ok(_) => {
                            taskdetail.status = TASK_STATUS_UNMONITORED;
                            println!("TaskID:{}({}|{})时间轮中取消成功", task.taskid, task.taskname, taskdetail.timepoint);
                        },
                        Err(e) => {
                            eprintln!("TaskID:{}({}|{})时间轮中取消失败: {}", task.taskid, task.taskname, taskdetail.timepoint, e);
                        },
                    }
                }
                continue;
            }

            // 跳过已在监控中的任务
            if taskdetail.status != TASK_STATUS_UNMONITORED {
                println!("TaskID:{}({}|{})已在监控中，跳过调度", task.taskid, task.taskname, taskdetail.timepoint);
                continue;
            }
            
            // 调度新任务
            match self.schedule(
                ndt,
                (task.retry_interval * taskdetail.current_trigger_count).try_into().unwrap(),
                format!("{}{}{}", task.taskid, TASK_KEY_SEPARATOR, taskdetail.timepoint),
                self.build_task_message(task.discribe.clone(), taskdetail.current_trigger_count),
            ).await {
                Ok(msg) => {
                    taskdetail.status = TASK_STATUS_MONITORING;
                    println!("TaskID:{}({}|{})添加任务成功： {}", 
                        task.taskid, task.taskname, 
                        ndt + Duration::from_millis((task.retry_interval * taskdetail.current_trigger_count).try_into().unwrap()), 
                        msg);
                },
                Err(e) => {
                    eprintln!("TaskID:{}({}|{})添加任务失败： {}", 
                        task.taskid, task.taskname, 
                        ndt + Duration::from_millis((task.retry_interval * taskdetail.current_trigger_count).try_into().unwrap()), 
                        e);
                },
            }
        }

        // 清理已删除的任务
        taskdetails.retain(|detail| detail.tag != TASK_TAG_DELETE || detail.status != TASK_STATUS_UNMONITORED);
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

        let mut guard = self.inner.lock().await;

        let task = match guard.tasks.get(&task_id) {
            Some(task) => task.clone(),
            None => {
                eprintln!("任务ID {} 不存在", task_id);
                return;
            }
        };

        for taskdetail in guard.taskdetails.iter_mut() {
            if format!("{}{}{}", taskdetail.taskid, TASK_KEY_SEPARATOR, taskdetail.timepoint) == key {
                if taskdetail.current_trigger_count >= task.retry_count {
                    println!("任务 {} 达到最大重试次数，停止调度", task.taskname);
                    taskdetail.status = TASK_STATUS_UNMONITORED;
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
                    Ok(msg) => {
                        taskdetail.status = TASK_STATUS_RETRY;
                        println!("TaskID:{}({}|{})添加任务成功： {}", 
                            task.taskid, task.taskname, 
                            time_point + Duration::from_millis((task.retry_interval * taskdetail.current_trigger_count).try_into().unwrap()), 
                            msg);
                    },
                    Err(e) => {
                        taskdetail.status = TASK_STATUS_UNMONITORED;
                        eprintln!("TaskID:{}({}|{})添加任务失败： {}", 
                            task.taskid, task.taskname, 
                            time_point + Duration::from_millis((task.retry_interval * taskdetail.current_trigger_count).try_into().unwrap()), 
                            e);
                    },
                }
            }
        }
    }
}