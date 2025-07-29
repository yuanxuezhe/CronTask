use std::sync::Arc;
use chrono::NaiveDateTime;
use crate::crontask::state::InnerState;
use crate::consts::*;
use crate::utils::gen_task_key;
use std::collections::HashSet;
use crate::task::TaskDetail;

impl crate::crontask::core::CronTask {
    /// 重新调度所有任务
    pub async fn reschedule_all(self: &Arc<Self>) {
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
                let task_key = gen_task_key(task.taskid, &taskdetail.timepoint);
                let delay_ms = (task.retry_interval * taskdetail.current_trigger_count) as u64;
                if taskdetail.tag == TASK_TAG_DELETE {
                    if taskdetail.status == TASK_STATUS_MONITORING {
                        to_cancel.push((ndt, delay_ms, task_key));
                    }
                    continue;
                }
                if taskdetail.status != TASK_STATUS_UNMONITORED {
                    continue;
                }
                to_schedule.push((
                    ndt,
                    delay_ms,
                    task_key.clone(),
                    self.build_task_message(task.discribe.clone(), taskdetail.current_trigger_count),
                    taskdetail.taskid,
                    taskdetail.timepoint.clone(),
                ));
                taskdetail_updates.push((taskdetail.taskid, taskdetail.timepoint.clone()));
            }
        }
        for (ndt, delay_ms, task_key) in to_cancel {
            match self.cancel(ndt, delay_ms, task_key.clone()).await {
                Ok(_) => println!("任务取消成功: {}", task_key),
                Err(e) => eprintln!("任务取消失败: {} - {}", task_key, e),
            }
        }
        for (ndt, delay_ms, task_key, message, taskid, timepoint) in to_schedule {
            match self.schedule(ndt, delay_ms, task_key.clone(), message).await {
                Ok(_) => {
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
        let mut guard = self.inner.lock().await;
        guard.taskdetails.retain(|detail| detail.tag != TASK_TAG_DELETE || detail.status != TASK_STATUS_UNMONITORED);
    }
    /// 重新加载任务（从数据库加载并刷新缓存、调度）
    pub async fn reload_tasks(self: &Arc<Self>) {
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
                let key = gen_task_key(task.taskid, &tp.to_string());
                new_keys.insert(key.clone());
                new_task_details.push(TaskDetail {
                    taskid: task.taskid,
                    timepoint: tp.to_string(),
                    current_trigger_count: 0,
                    status: crate::consts::TASK_STATUS_UNMONITORED,
                    tag: crate::consts::TASK_TAG_NEW,
                });
            }
        }
        // 获取锁并更新状态
        let mut guard = self.inner.lock().await;
        // 创建旧任务明细键映射
        let old_keys: HashSet<String> = guard.taskdetails
            .iter()
            .map(|detail| gen_task_key(detail.taskid, &detail.timepoint))
            .collect();
        // 标记需要保留的任务
        for detail in guard.taskdetails.iter_mut() {
            let key = gen_task_key(detail.taskid, &detail.timepoint);
            if new_keys.contains(&key) && detail.status == crate::consts::TASK_STATUS_MONITORING {
                detail.tag = crate::consts::TASK_TAG_KEEP;
            } else {
                detail.tag = crate::consts::TASK_TAG_DELETE;
            }
        }
        // 添加新任务（只添加真正的新任务）
        for detail in new_task_details {
            let key = gen_task_key(detail.taskid, &detail.timepoint);
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
    pub fn build_task_message(&self, discribe: String, current_trigger_count: i32) -> String {
        if current_trigger_count == 0 {
            discribe
        } else {
            format!("{}(重复提醒第{}次)", discribe, current_trigger_count)
        }
    }
    pub async fn schedule(
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
            std::time::Duration::from_millis(millis),
            key,
            arg,
            move |key, eventdata| self_clone.on_call_back(key, eventdata),
        ).await
    }
    pub async fn cancel(
        self: &Arc<Self>,
        timestamp: NaiveDateTime, 
        millis: u64, 
        key: String,
    ) -> Result<String, String> {
        println!("crontask::cancel 取消任务: {} at {} + {}ms", key, timestamp, millis);
        self.taskscheduler.cancel(
            timestamp,
            std::time::Duration::from_millis(millis),
            key,
        ).await
    }
} 