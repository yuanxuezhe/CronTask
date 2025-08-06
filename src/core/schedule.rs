// 标准库导入
use std::collections::HashSet;
use std::sync::Arc;

// 外部 crate 导入
use chrono::NaiveDateTime;

// 内部模块导入
use crate::common::consts::*;
use crate::common::utils::gen_task_key;
use crate::core::cron_task::CronTask;
use crate::message::message_bus::CronMessage;
use crate::task_engine::model::TaskDetail;

/// 任务调度实现
impl CronTask {
    /// 检查所有任务的状态并根据需要进行调度或取消调度
    pub async fn reschedule_all(self: &Arc<Self>) {
        let mut to_cancel = Vec::new();
        let mut to_schedule = Vec::new();
        
        {
            let mut guard = self.inner.lock().await;
            let crate::core::state::InnerState { taskdetails, tasks } = &mut *guard;
            
            for taskdetail in taskdetails.iter_mut() {
                // 获取任务信息
                let task = match tasks.get(&taskdetail.taskid) {
                    Some(task) => task,
                    None => {
                        crate::error_log!("任务ID {} 不存在", taskdetail.taskid);
                        continue;
                    }
                };
                
                // 解析时间点
                let ndt = match NaiveDateTime::parse_from_str(&taskdetail.timepoint, DATETIME_FORMAT) {
                    Ok(dt) => dt,
                    Err(e) => {
                        crate::error_log!("时间点解析失败: {} - {}", taskdetail.timepoint, e);
                        continue;
                    }
                };
                
                let task_key = gen_task_key(task.taskid, &taskdetail.timepoint);
                let delay_ms = (task.retry_interval * taskdetail.current_trigger_count) as u64;
                
                // 处理需要删除的任务
                if taskdetail.tag == TASK_TAG_DELETE {
                    if taskdetail.status == TASK_STATUS_MONITORING {
                        to_cancel.push((ndt, delay_ms, task_key));
                    }
                    continue;
                }
                
                // 跳过已监控但不需要重新调度的任务
                if taskdetail.status != TASK_STATUS_UNMONITORED {
                    continue;
                }
                
                // 准备需要调度的任务
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
        
        // 取消需要删除的任务
        self.cancel_tasks(to_cancel).await;
        
        // 调度新任务
        self.schedule_tasks(to_schedule).await;
        
        // 清理已删除的任务
        self.cleanup_deleted_tasks().await;
    }
    
    /// 从数据库重新加载所有任务，并更新内部状态和调度
    pub async fn reload_tasks(self: &Arc<Self>) {
        // 从数据库加载新任务
        let new_tasks = match self.load_tasks_from_db().await {
            Ok(tasks) => tasks,
            Err(e) => {
                crate::error_log!("Failed to load tasks from DB: {}", e);
                return;
            }
        };
        
        // 预先计算所有新任务的时间点，减少锁内计算
        let (new_task_details, new_keys) = self.calculate_task_schedules(&new_tasks).await;
        
        // 更新内部状态
        self.update_internal_state(new_tasks, new_task_details, new_keys).await;
        
        // 重新调度所有任务
        self.clone().reschedule_all().await;
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
    ) -> Result<(), String> {
        crate::info_log!("crontask::schedule 发送调度消息: {} at {} + {}ms", key, timestamp, millis);
        self.message_bus.send(CronMessage::ScheduleTask {
            timestamp,
            delay_ms: millis,
            key,
            arg,
        }).map_err(|e| e.to_string())
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
    ) -> Result<(), String> {
        crate::info_log!("crontask::cancel 发送取消消息: {} at {} + {}ms", key, timestamp, millis);
        self.message_bus.send(CronMessage::CancelTask {
            timestamp,
            delay_ms: millis,
            key,
        }).map_err(|e| e.to_string())
    }
}

// 私有辅助方法实现
impl CronTask {
    /// 根据任务描述和当前触发次数构建任务消息
    /// 
    /// # 参数
    /// * `description` - 任务描述
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
    
    /// 取消任务
    async fn cancel_tasks(&self, to_cancel: Vec<(NaiveDateTime, u64, String)>) {
        for (ndt, delay_ms, task_key) in to_cancel {
            let _ = self.message_bus.send(CronMessage::CancelTask {
                timestamp: ndt,
                delay_ms,
                key: task_key.clone(),
            });
            crate::info_log!("发送任务取消消息: {}", task_key);
        }
    }
    
    /// 调度任务
    async fn schedule_tasks(&self, to_schedule: Vec<(NaiveDateTime, u64, String, String, i32, String)>) {
        // 先收集所有需要更新状态的任务信息
        let mut status_updates = Vec::new();
        
        for (ndt, delay_ms, task_key, message, taskid, timepoint) in to_schedule {
            match self.message_bus.send(CronMessage::ScheduleTask {
                timestamp: ndt,
                delay_ms,
                key: task_key.clone(),
                arg: message,
            }) {
                Ok(_) => {
                    status_updates.push((taskid, timepoint, TASK_STATUS_MONITORING));
                    crate::info_log!("发送任务调度消息: {}", task_key);
                },
                Err(e) => {
                    crate::error_log!("发送任务调度消息失败: {} - {}", task_key, e);
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
    }
    
    /// 清理已删除的任务
    async fn cleanup_deleted_tasks(&self) {
        let mut guard = self.inner.lock().await;
        guard.taskdetails.retain(|detail| detail.tag != TASK_TAG_DELETE || detail.status != TASK_STATUS_UNMONITORED);
    }
    
    /// 计算任务调度时间点
    async fn calculate_task_schedules(
        &self, 
        new_tasks: &std::collections::HashMap<i32, crate::task_engine::model::Task>
    ) -> (Vec<TaskDetail>, HashSet<String>) {
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
        
        (new_task_details, new_keys)
    }
    
    /// 更新内部状态
    async fn update_internal_state(
        &self, 
        new_tasks: std::collections::HashMap<i32, crate::task_engine::model::Task>,
        new_task_details: Vec<TaskDetail>,
        new_keys: HashSet<String>
    ) {
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
    }
}