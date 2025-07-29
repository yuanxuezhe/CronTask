use std::sync::Arc;
use chrono::NaiveDateTime;
use crate::crontask::state::InnerState;
use crate::consts::*;
use crate::utils::*;
use std::collections::HashSet;
use crate::task::TaskDetail;

impl crate::crontask::core::CronTask {
    /// 检查所有任务的状态并根据需要进行调度或取消调度
    pub async fn reschedule_all(self: &Arc<Self>) {
        let mut to_cancel = Vec::new();
        let mut to_schedule = Vec::new();
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
            }
        }
        for (ndt, delay_ms, task_key) in to_cancel {
            match self.cancel(ndt, delay_ms, task_key.clone()).await {
                Ok(_) => println!("任务取消成功: {}", task_key),
                Err(e) => eprintln!("任务取消失败: {} - {}", task_key, e),
            }
        }
        
        // 先收集所有需要更新状态的任务信息
        let mut status_updates = Vec::new();
        for (ndt, delay_ms, task_key, message, taskid, timepoint) in to_schedule {
            match self.schedule(ndt, delay_ms, task_key.clone(), message).await {
                Ok(_) => {
                    status_updates.push((taskid, timepoint, TASK_STATUS_MONITORING));
                    println!("任务调度成功: {}", task_key);
                },
                Err(e) => {
                    eprintln!("任务调度失败: {} - {}", task_key, e);
                },
            }
        }
        
        // 批量更新状态以减少锁竞争
        if !status_updates.is_empty() {
            let mut guard = self.inner.lock().await;
            for (taskid, timepoint, new_status) in status_updates {
                if let Some(detail) = guard.taskdetails
                    .iter_mut()
                    .find(|d| d.taskid == taskid && d.timepoint == timepoint) {
                    detail.status = new_status;
                }
            }
        }
        
        // 清理已删除的任务
        let mut guard = self.inner.lock().await;
        guard.taskdetails.retain(|detail| detail.tag != TASK_TAG_DELETE || detail.status != TASK_STATUS_UNMONITORED);
    }
    
    /// 从数据库重新加载所有任务，并更新内部状态和调度
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
                    status: TASK_STATUS_UNMONITORED,
                    tag: TASK_TAG_NEW,
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
            if new_keys.contains(&key) && detail.status == TASK_STATUS_MONITORING {
                detail.tag = TASK_TAG_KEEP;
            } else {
                detail.tag = TASK_TAG_DELETE;
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
    
    /// 根据任务描述和当前触发次数构建任务消息
    /// 
    /// # 参数
    /// * `description` - 任务描述（修复了字段名拼写错误）
    /// * `current_trigger_count` - 当前触发次数
    /// 
    /// # 返回值
    /// 返回构建好的任务消息字符串
    pub fn build_task_message(&self, description: String, current_trigger_count: i32) -> String {
        if current_trigger_count == 0 {
            description
        } else {
            format!("{}（重复提醒第{}次）", description, current_trigger_count)
        }
    }
    
    /// 将任务添加到时间轮中进行调度
    /// 
    /// # 参数
    /// * `timestamp` - 任务触发时间（NaiveDateTime类型）
    /// * `millis` - 延迟毫秒数
    /// * `key` - 任务唯一标识符
    /// * `arg` - 任务参数（字符串类型）
    /// 
    /// # 返回值
    /// 成功时返回任务 key，失败时返回错误信息（Result<String, String> 类型）
    pub async fn schedule(
        self: &Arc<Self>,
        timestamp: NaiveDateTime, 
        millis: u64, 
        key: String,
        arg: String, 
    ) -> Result<String, String> {
        println!("crontask::schedule 调度任务: {} at {} + {}ms", key, timestamp, millis);
        let self_clone = self.clone();
        let result = self.taskscheduler.schedule(
            timestamp,
            std::time::Duration::from_millis(millis),
            key,
            arg,
            move |key, eventdata| self_clone.on_call_back(key, eventdata),
        ).await;
        
        match result {
            Ok(key) => Ok(key),
            Err(e) => Err(e.to_string()),
        }
    }
    
    /// 从时间轮中移除指定任务
    /// 
    /// # 参数
    /// * `timestamp` - 任务原定触发时间（NaiveDateTime 类型）
    /// * `millis` - 原定延迟毫秒数
    /// * `key` - 任务唯一标识符
    /// 
    /// # 返回值
    /// 成功时返回操作结果信息，失败时返回错误信息（Result<String, String> 类型）
    pub async fn cancel(
        self: &Arc<Self>,
        timestamp: NaiveDateTime, 
        millis: u64, 
        key: String,
    ) -> Result<String, String> {
        println!("crontask::cancel 取消任务: {} at {} + {}ms", key, timestamp, millis);
        let result = self.taskscheduler.cancel(
            timestamp,
            std::time::Duration::from_millis(millis),
            key,
        ).await;
        
        match result {
            Ok(result) => Ok(result),
            Err(e) => Err(e.to_string()),
        }
    }
}