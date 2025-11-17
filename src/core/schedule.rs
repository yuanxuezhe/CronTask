// 标准库导入
use std::collections::HashSet;
use std::sync::Arc;

// 外部 crate 导入
use chrono::{NaiveDateTime, TimeDelta, Utc};
use chrono_tz::Asia::Shanghai;

// 内部模块导入
use crate::common::consts::*;
use crate::common::utils::gen_task_key;
use crate::core::core::CronTask;
use crate::message::message_bus::CronMessage;
use crate::task_engine::model::TaskDetail;

/// 任务调度实现
impl CronTask {
    /// 检查所有任务的状态并根据需要进行调度或取消调度
    pub async fn reschedule_all(self: &Arc<Self>) {
        // 快速获取任务数据，尽量减少锁持有时间
        let (to_cancel, to_schedule) = self.collect_tasks_for_processing().await;
        
        // 并行处理取消和调度任务
        let (cancel_result, schedule_result) = tokio::join!(
            self.cancel_tasks(to_cancel),
            self.schedule_tasks(to_schedule)
        );
        
        // 处理结果（如果需要记录错误）
        if let Err(e) = cancel_result {
            crate::error_log!("取消任务失败: {}", e);
        }
        if let Err(e) = schedule_result {
            crate::error_log!("调度任务失败: {}", e);
        }
        
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
    
}

// 私有辅助方法实现
impl CronTask {
    /// 收集需要处理的任务数据，尽量减少锁持有时间
    async fn collect_tasks_for_processing(&self) -> (Vec<(NaiveDateTime, u64, String, i32)>, Vec<(NaiveDateTime, u64, String, String, i32, String)>) {
        let mut to_cancel = Vec::new();
        let mut to_schedule = Vec::new();
        
        // 直接在锁内处理数据，避免完整克隆
        let guard = self.inner.lock().await;
        
        for taskdetail in &guard.taskdetails {
            // 获取任务信息
            let task = match guard.tasks.get(&taskdetail.taskid) {
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
            
            let delay_ms = (task.retry_interval * taskdetail.current_trigger_count) as u64;
            
            // 处理需要删除的任务
            if taskdetail.tag == TASK_TAG_DELETE {
                if taskdetail.status == TASK_STATUS_MONITORING {
                    // 仅在需要时生成task_key
                    let task_key = gen_task_key(task.taskid, &taskdetail.timepoint);
                    to_cancel.push((ndt, delay_ms, task_key, taskdetail.taskid));
                }
                continue;
            }
            
            // 跳过已监控但不需要重新调度的任务
            if taskdetail.status != TASK_STATUS_UNMONITORED {
                continue;
            }
            
            // 准备需要调度的任务
            let task_key = gen_task_key(task.taskid, &taskdetail.timepoint);
            // 使用task.discribe的引用构建消息，避免克隆
            let message = self.build_task_message(&task.discribe, taskdetail.current_trigger_count);
            to_schedule.push((
                ndt,
                delay_ms,
                task_key,
                message,
                taskdetail.taskid,
                taskdetail.timepoint.clone(), // 保留必要的克隆，因为我们需要所有权
            ));
        }
        
        (to_cancel, to_schedule)
    }
    
    /// 取消任务
    async fn cancel_tasks(&self, to_cancel: Vec<(NaiveDateTime, u64, String, i32)>) -> Result<(), String> {
        let mut status_updates = Vec::new();
        
        for (ndt, delay_ms, task_key, task_id) in to_cancel {
            // 发送取消消息
            match self.message_bus.send(CronMessage::CancelTask {
                timestamp: ndt,
                delay_ms,
                key: task_key.clone(),
            }) {
                Ok(_) => {
                    status_updates.push((task_id, task_key.clone()));
                    crate::info_log!("发送任务取消消息: {}", task_key);
                },
                Err(e) => {
                    crate::error_log!("发送取消消息失败: {} - {}", task_key, e);
                }
            }
        }
        
        // 批量更新状态，减少锁竞争
        if !status_updates.is_empty() {
            self.batch_update_task_status(status_updates).await?;
        }
        
        Ok(())
    }
    
    /// 批量更新任务状态
    async fn batch_update_task_status(&self, updates: Vec<(i32, String)>) -> Result<(), String> {
        let mut guard = self.inner.lock().await;
        
        for (task_id, task_key) in updates {
            if let Some(detail) = guard.taskdetails
                .iter_mut()
                .find(|d| d.taskid == task_id && gen_task_key(d.taskid, &d.timepoint) == task_key) {
                detail.status = TASK_STATUS_UNMONITORED;
                crate::info_log!("更新任务 {} 状态为未监控", task_key);
            }
        }
        
        Ok(())
    }
    
    /// 调度任务
    async fn schedule_tasks(&self, to_schedule: Vec<(NaiveDateTime, u64, String, String, i32, String)>) -> Result<(), String> {
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
                    // crate::info_log!("发送任务调度消息: {}", task_key);
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
        
        Ok(())
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
        
        // 获取时间轮配置
        let time_wheel = self.taskscheduler.time_wheel();
        let tick_duration_secs = time_wheel.tick_duration.as_secs();
        let total_slots = time_wheel.total_slots;
        
        // 计算时间轮能表示的最大未来时间
        let current_time = Utc::now().with_timezone(&Shanghai).naive_local();
        let max_duration = tick_duration_secs * total_slots as u64;
        let max_future_time = current_time + TimeDelta::seconds(max_duration as i64);

        for task in new_tasks.values() {
            let timepoints = task.get_schedules_in_range(max_future_time);
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