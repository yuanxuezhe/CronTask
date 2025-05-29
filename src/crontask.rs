// src/crontask.rs
use chrono::{Local, NaiveDateTime, Utc};
use std::time::Duration;
use crate::taskscheduler::TaskScheduler;
use chrono_tz::Asia::Shanghai;
use std::sync::{Arc};
use crate::task::{Task,TaskDetail};
use dbcore::Database;
use std::collections::{HashMap, HashSet};
use std::mem::drop;
use tokio::sync::Mutex;

pub struct InnerState {
    pub taskdetails: Vec<TaskDetail>,
    pub tasks: HashMap<i32, Task>,
}

pub struct CronTask {
    taskscheduler: Arc<TaskScheduler>,
    /// 内部共享数据，集成了读写锁
    inner: Arc<Mutex<InnerState>>,
    reload_interval: u64,  // 时间间隔 微妙
    db: Database,
}

impl CronTask {
    pub fn new(reload_millis: u64, tick_mills: u64, total_slots: usize, high_precision: bool, db:Database) -> Arc<Self> {
        let instance = Arc::new(Self {
            // 正确初始化 TaskScheduler
            taskscheduler: Arc::new(TaskScheduler::new(Duration::from_millis(tick_mills),total_slots,high_precision)),
            inner: Arc::new(Mutex::new(InnerState {
                taskdetails: Vec::new(),
                tasks: HashMap::new(),
            })),
            reload_interval: reload_millis,
            db: db,
        });

        let instance_clone = instance.clone(); // 克隆 Arc 引用
        //let now = Local::now().naive_local();
        let reload_name = "__reload_tasks__".to_string();
        tokio::spawn(async move {
            let _ = instance_clone.schedule(Local::now().naive_local(), 2000, reload_name.clone(), reload_name.clone()).await;
            println!("crontask::new");
        });
        instance
    }

    /// 重新加载任务
    async fn reload_tasks(self: &Arc<Self>) {
        // 1. 从数据库或其他存储加载新任务
        let new_tasks = self.load_tasks_from_db().await;
        let new_tasks = new_tasks.expect("Failed to load tasks from DB");

        let mut guard = self.inner.lock().await; // 获取写锁
        guard.tasks = new_tasks;

        // 1. 生成新任务明细
        let mut task_details_new = Vec::new();
        let mut new_keys = HashSet::new();

        for task in guard.tasks.values() {
            let timepoint = task.next_n_schedules(10);
            if timepoint.is_empty() {
                continue;
            }
            println!("最近10天内的触发时间点: {:?}", timepoint);

            for tp in timepoint {
                let key = format!("{}|{}", task.taskid, tp);
                new_keys.insert(key.clone());

                task_details_new.push(TaskDetail {
                    taskid: task.taskid,
                    timepoint: tp.to_string(),
                    current_trigger_count: 0,
                    status:0, // 0: 未监控，1: 监控中
                    tag: 2, // 0: 删除，1: 保留，2: 新增
                });
            }
        }

        // 2. 创建旧任务明细键映射（taskid + timepoint -> index）
        let mut old_map: HashMap<String, usize> = HashMap::new();
        for (idx, detail) in guard.taskdetails.iter().enumerate() {
            let key = format!("{}|{}", detail.taskid, detail.timepoint);
            old_map.insert(key, idx);
        }

        // 3. 标记和修改原始 taskdetails
        for detail in guard.taskdetails.iter_mut() {
            let key = format!("{}|{}", detail.taskid, detail.timepoint);
            if new_keys.contains(&key) && detail.status == 1 {
                detail.tag = 1; // 保留
            } else {
                detail.tag = 0; // 已删除
            }
        }

        // 4. 将新数据中不存在于原始数据的添加进去，tag = 2
        for detail in task_details_new {
            let key = format!("{}|{}", detail.taskid, detail.timepoint);
            if !old_map.contains_key(&key) {
                let mut new_detail = detail;
                new_detail.tag = 2;
                guard.taskdetails.push(new_detail);
            }
        }

        drop(guard); // 手动释放写锁

        // 3. 重新调度所有任务
        self.clone().reschedule_all().await;
    }

    /// 从数据库加载任务（示例实现）
    async fn load_tasks_from_db(&self) -> Result<HashMap<i32, Task>,Box<dyn std::error::Error + Send + Sync>> {
        let rs = self.db.open("select * from task where taskid >= ?").set_param(0).query(&self.db).await?;
        // 2. 遍历结果集
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

            tasks.insert(task.taskid.clone(), task);
        }

        Ok(tasks)
    }

    /// 重新调度所有任务
    async fn reschedule_all(self: &Arc<Self>) {
        let mut guard = self.inner.lock().await; // 获取写锁
        // 获取guard.taskdetails引用，后续能直接修改其中的值
        let InnerState { taskdetails: task_details_new, tasks } = &mut *guard;

        for _taskdetail in task_details_new.iter_mut() {
            let _task = match tasks.get_mut(&_taskdetail.taskid) {
                Some(_task) => _task,
                None => {
                    println!("任务ID {} 不存在", _taskdetail.taskid);
                    return;
                }
            };
            let ndt = NaiveDateTime::parse_from_str(&_taskdetail.timepoint, "%Y-%m-%d %H:%M:%S").unwrap();
            // 0说明需要取消任务
            if _taskdetail.tag == 0 {
                println!("TaskID:{}({}|{})已完成", _task.taskid, _task.taskname, _taskdetail.timepoint);
                // 监控中，则取消任务
                if _taskdetail.status == 1 {
                    match self.cancel(
                        ndt,
                        (_task.retry_interval * _taskdetail.current_trigger_count).try_into().unwrap(),
                        format!("{}|{}", _task.taskid, _taskdetail.timepoint),
                    ).await {
                        Ok(_msg) => {
                            _taskdetail.status = 0; // 设置终止
                            // 取消任务逻辑
                            println!("TaskID:{}({}|{})时间轮中取消成功", _task.taskid, _task.taskname, _taskdetail.timepoint);
                        },
                        Err(_e) =>{
                            println!("TaskID:{}({}|{})时间轮中取消失败", _task.taskid, _task.taskname, _taskdetail.timepoint);
                        },
                    }
                }
                continue; // 跳过已达到最大重试次数的任务
            }

            if _taskdetail.status != 0 {
                // 监控中，跳过调度
                println!("TaskID:{}({}|{})已在监控中，跳过调度", _task.taskid, _task.taskname, _taskdetail.timepoint);
                continue; // 跳过已在监控中的任务
            }
            
            match self.schedule(
                ndt,
                (_task.retry_interval * _taskdetail.current_trigger_count).try_into().unwrap(),
                format!("{}|{}", _task.taskid, _taskdetail.timepoint),
                self.build_task_message(_task.discribe.clone(), _taskdetail.current_trigger_count),  // ← 闭包返回 String
            ).await {
                Ok(msg) => {
                    _taskdetail.status = 1; // 设置为监控中
                    println!("TaskID:{}({}|{})添加任务成功： {}", _task.taskid, _task.taskname, ndt + Duration::from_millis((_task.retry_interval * _taskdetail.current_trigger_count).try_into().unwrap()), msg); // 直接打印消息
                },
                Err(e) =>{
                    println!("TaskID:{}({}|{})添加任务失败： {}", _task.taskid, _task.taskname, ndt + Duration::from_millis((_task.retry_interval * _taskdetail.current_trigger_count).try_into().unwrap()), e);
                },
            }
        }

        // 删掉 task_details_new 中 tag = 0的记录
        task_details_new.retain(|detail| detail.tag != 0 || detail.status != 0);
    }

    fn build_task_message(self: &Arc<Self>, discribe: String, current_trigger_count: i32) -> String {
        if current_trigger_count == 0 {
            format!("{}", discribe)
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
        // 克隆 Arc 引用
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
        println!("crontask::cancel 调度任务: {} at {} + {}ms", key, timestamp, millis);
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

    async fn on_call_back_inner(self: &Arc<Self>,key: String, eventdata: String) {
        let now = Utc::now().with_timezone(&Shanghai);
        println!("[{}] 执行任务[{}]:  {}", now, key, eventdata);

        if key == "__reload_tasks__" {
            // 重新调度下一次 reload
            let _ = self.schedule(Local::now().naive_local(), self.reload_interval, key.clone(), eventdata.clone()).await;
    
            // 调用 reload_tasks
            self.reload_tasks().await;
            return;
        }
        
        // 1. 解析任务ID和时间点
        let parts: Vec<&str> = key.split('|').collect();
        if parts.len() != 2 {
            println!("任务名称格式错误: {}", key);
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

        let mut guard = self.inner.lock().await; // 获取写锁

        let _task = match guard.tasks.get(&task_id) {
            Some(_task) => _task.clone(),
            None => {
                println!("任务ID {} 不存在", task_id);
                return;
            }
        };

        for _taskdetail in guard.taskdetails.iter_mut() {
            if format!("{}|{}", _taskdetail.taskid, _taskdetail.timepoint) == key {
                if _taskdetail.current_trigger_count >= _task.retry_count {
                    println!("任务 {} 达到最大重试次数，停止调度", _task.taskname);
                    _taskdetail.status = 0; // 未监控
                    return;
                }
                // 重新订阅
                _taskdetail.current_trigger_count += 1;

                match self.schedule(
                    time_point,
                    (_task.retry_interval * _taskdetail.current_trigger_count).try_into().unwrap(),
                    key.clone(),
                    self.build_task_message(_task.discribe.clone(), _taskdetail.current_trigger_count),  // ← 闭包返回 String
                ).await {
                    Ok(msg) => {
                        _taskdetail.status = 2; // 重复提醒
                        println!("TaskID:{}({}|{})添加任务成功： {}", _task.taskid, _task.taskname, time_point + Duration::from_millis((_task.retry_interval * _taskdetail.current_trigger_count).try_into().unwrap()), msg); // 直接打印消息
                    },
                    Err(e) =>{
                        _taskdetail.status = 0; // 未监控
                        println!("TaskID:{}({}|{})添加任务失败： {}", _task.taskid, _task.taskname, time_point + Duration::from_millis((_task.retry_interval * _taskdetail.current_trigger_count).try_into().unwrap()), e);
                    },
                }
            }
        }
    }
}