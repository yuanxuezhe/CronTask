// 标准库导入
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// 外部 crate 导入
use chrono::{NaiveDateTime, TimeDelta, Utc};
use chrono_tz::Asia::Shanghai;

// 内部模块导入
use crate::common::consts::*;
use crate::common::utils::gen_task_key;
use crate::core::core::CronTask;
use crate::basic::message::message_bus::CronMessage;
use crate::task_engine::model::{Task, TaskDetail};

// 错误类型导入
use crate::common::error::CronTaskError;

/// 任务处理元数据
/// 用于在锁外传递任务的关键信息，减少锁持有时间
#[derive(Clone)]
pub struct TaskProcessMetadata {
    pub taskid: i32,
    pub timepoint: String,
    pub current_trigger_count: i32,
    pub status: i32,
    pub tag: i32,
    pub retry_interval: i32,
    pub describe: String,
}

/// 任务取消信息
/// 包含取消任务所需的所有信息
#[derive(Clone)]
pub struct TaskCancelInfo {
    pub timestamp: NaiveDateTime,
    pub delay_ms: u64,
    pub task_key: String,
    pub taskid: i32,
}

/// 任务调度信息
/// 包含调度任务所需的所有信息
#[derive(Debug, Clone)]
pub struct TaskScheduleInfo {
    pub timestamp: NaiveDateTime,
    pub delay_ms: u64,
    pub task_key: String,
    pub taskid: i32,
    pub timepoint: String,
}

/// 任务调度实现
impl CronTask {
    /// 检查所有任务的状态并根据需要进行调度或取消调度
    pub async fn reschedule_all(self: &Arc<Self>) {
        // 快速获取任务数据，尽量减少锁持有时间
        let (to_cancel, to_schedule) = self.collect_tasks_for_processing().await;

        // 并行处理取消和调度任务，提高效率
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
        let new_tasks = match crate::task_engine::model::Task::load_tasks_from_db(&self.db).await {
            Ok(tasks) => tasks,
            Err(e) => {
                crate::error_log!("从数据库加载任务失败: {}", e);
                return;
            }
        };

        // 预先计算所有新任务的时间点，减少锁内计算
        let (new_task_details, new_keys) = self.calculate_task_schedules(&new_tasks);

        // 更新内部状态
        self.update_internal_state(new_tasks, new_task_details, new_keys)
            .await;

        // 重新调度所有任务
        self.clone().reschedule_all().await;
    }

    /// 收集需要处理的任务数据，优化锁粒度
    async fn collect_tasks_for_processing(&self) -> (Vec<TaskCancelInfo>, Vec<TaskScheduleInfo>) {
        // 先在锁内收集必要的任务信息，然后释放锁
        let task_data: Vec<TaskProcessMetadata> = {
            // 只需要读取操作，使用read()而不是write()
            let guard = self.inner.read().await;

            // 预分配容量，减少重新分配
            let mut result = Vec::with_capacity(guard.taskdetails.len());

            for detail in guard.taskdetails.values() {
                // 只收集必要的数据，避免在锁内进行复杂操作
                if let Some(task) = guard.tasks.get(&detail.taskid) {
                    result.push(TaskProcessMetadata {
                        taskid: task.taskid,
                        timepoint: detail.timepoint.clone(), // 必须克隆，因为需要在锁外使用
                        current_trigger_count: detail.current_trigger_count,
                        status: detail.status,
                        tag: detail.tag,
                        retry_interval: task.retry_interval, // 只保存需要的值，避免完整克隆task
                        describe: task.discribe.clone(),     // 必须克隆，因为需要在锁外构建消息
                    });
                }
            }

            result
        };

        let mut to_cancel = Vec::new();
        let mut to_schedule = Vec::new();

        // 预分配容量，减少重新分配
        to_cancel.reserve(task_data.len() / 10); // 假设10%的任务需要取消
        to_schedule.reserve(task_data.len());

        // 在锁外处理数据解析和复杂计算
        for metadata in task_data {
            // 解析时间点
            let ndt = match NaiveDateTime::parse_from_str(&metadata.timepoint, DATETIME_FORMAT) {
                Ok(dt) => dt,
                Err(e) => {
                    crate::error_log!("时间点解析失败: {} - {}", metadata.timepoint, e);
                    continue;
                }
            };

            let delay_ms = (metadata.retry_interval * metadata.current_trigger_count) as u64;

            // 处理需要删除的任务
            if metadata.tag == TASK_TAG_DELETE {
                if metadata.status == TASK_STATUS_MONITORING {
                    let task_key = gen_task_key(metadata.taskid, &metadata.timepoint);
                    to_cancel.push(TaskCancelInfo {
                        timestamp: ndt,
                        delay_ms,
                        task_key,
                        taskid: metadata.taskid,
                    });
                }
                continue;
            }

            // 跳过已监控但不需要重新调度的任务
            if metadata.status != TASK_STATUS_UNMONITORED {
                continue;
            }

            // 准备需要调度的任务
            let task_key = gen_task_key(metadata.taskid, &metadata.timepoint);
            to_schedule.push(TaskScheduleInfo {
                timestamp: ndt,
                delay_ms,
                task_key,
                taskid: metadata.taskid,
                timepoint: metadata.timepoint, // 已经在锁外克隆
            });
        }

        (to_cancel, to_schedule)
    }

    /// 取消任务
    async fn cancel_tasks(&self, to_cancel: Vec<TaskCancelInfo>) -> Result<(), CronTaskError> {
        // 预分配容量，减少重新分配
        let mut status_updates = Vec::with_capacity(to_cancel.len());

        for cancel_info in to_cancel {
            // 发送取消消息
            let task_key = &cancel_info.task_key;

            match self.message_bus.send(CronMessage::CancelTask {
                timestamp: cancel_info.timestamp,
                delay_ms: cancel_info.delay_ms,
                key: task_key.clone(),
            }) {
                Ok(_) => {
                    status_updates.push((cancel_info.taskid, task_key.clone()));
                    crate::info_log!("发送任务取消消息: {}", task_key);
                }
                Err(e) => {
                    crate::error_log!("发送取消消息失败: {} - {}", task_key, e);
                    // 记录错误但继续处理其他任务
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
    async fn batch_update_task_status(
        &self,
        updates: Vec<(i32, String)>,
    ) -> Result<(), CronTaskError> {
        // 需要修改任务状态，使用write()
        let mut guard = self.inner.write().await;

        // 创建临时HashMap，以任务键为索引，存储需要更新的任务ID
        let mut task_key_map: HashMap<String, i32> = HashMap::with_capacity(updates.len());
        for (task_id, task_key) in updates {
            task_key_map.insert(task_key, task_id);
        }

        // 只遍历一次taskdetails，通过任务键查找需要更新的任务
        for (_, detail) in guard.taskdetails.iter_mut() {
            // 使用TaskDetail的task_key方法生成任务键，避免重复代码
            let current_task_key = detail.task_key();

            // 如果该任务键在需要更新的列表中且ID匹配
            if let Some(task_id) = task_key_map.remove(&current_task_key) {
                if task_id == detail.taskid {
                    detail.status = TASK_STATUS_UNMONITORED;
                    crate::info_log!("更新任务 {} 状态为未监控", current_task_key);

                    // 如果所有任务都已处理完成，提前退出循环
                    if task_key_map.is_empty() {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    /// 调度任务
    async fn schedule_tasks(
        &self,
        to_schedule: Vec<TaskScheduleInfo>,
    ) -> Result<(), CronTaskError> {
        // 先收集所有需要更新状态的任务信息
        let mut status_updates = Vec::with_capacity(to_schedule.len());

        for info in to_schedule {
            // 借用而不是克隆task_key用于日志记录
            let task_key_ref = &info.task_key;

            match self.message_bus.send(CronMessage::ScheduleTask {
                timestamp: info.timestamp,
                delay_ms: info.delay_ms,
                key: task_key_ref.clone(), // 这里仍然需要克隆，因为消息总线需要所有权
            }) {
                Ok(_) => {
                    status_updates.push((info.taskid, info.timepoint, TASK_STATUS_MONITORING));
                    // crate::info_log!("发送任务调度消息: {}", task_key_ref);
                }
                Err(e) => {
                    crate::error_log!("发送任务调度消息失败: {} - {}", task_key_ref, e);
                    // 记录错误但继续处理其他任务
                }
            }
        }

        // 批量更新状态以减少锁竞争
        if !status_updates.is_empty() {
            // 需要修改任务状态，使用write()
            let mut guard = self.inner.write().await;

            for (taskid, timepoint, new_status) in status_updates {
                // 使用(taskid, timepoint)作为键直接查找，避免生成任务键
                if let Some(detail) = guard.taskdetails.get_mut(&(taskid, timepoint)) {
                    detail.status = new_status;
                }
            }
        }

        Ok(())
    }

    /// 清理已删除的任务
    /// 只保留未标记为删除的任务，或已标记为删除但仍在监控中的任务
    async fn cleanup_deleted_tasks(&self) {
        // 需要删除任务，使用write()
        let mut guard = self.inner.write().await;

        // 保留条件：任务未标记为删除，或已标记为删除但仍在监控中
        guard.taskdetails.retain(|_key, detail| {
            detail.tag != TASK_TAG_DELETE || detail.status != TASK_STATUS_UNMONITORED
        });
    }

    /// 计算任务调度时间点
    fn calculate_task_schedules(
        &self,
        new_tasks: &HashMap<i32, Task>,
    ) -> (Vec<TaskDetail>, HashSet<String>) {
        let mut new_task_details = Vec::new();

        // 获取时间轮配置
        let time_wheel = self.task_scheduler.time_wheel();
        let tick_duration_secs = time_wheel.tick_duration.as_secs();
        let total_slots = time_wheel.total_slots;

        // 计算时间轮能表示的最大未来时间
        let current_time = Utc::now().with_timezone(&Shanghai).naive_local();
        let max_duration = tick_duration_secs * total_slots as u64;
        let max_future_time = current_time + TimeDelta::seconds(max_duration as i64);

        // 计算预期任务数量，用于预分配容量
        let expected_task_count = new_tasks
            .values()
            .map(|task| {
                let timepoints = task.get_schedules_in_range(max_future_time);
                timepoints.len()
            })
            .sum();

        // 预先分配足够容量以减少重新分配
        new_task_details.reserve(expected_task_count);
        let mut new_keys = HashSet::with_capacity(expected_task_count);

        for task in new_tasks.values() {
            let timepoints = task.get_schedules_in_range(max_future_time);
            if timepoints.is_empty() {
                continue;
            }

            for tp in timepoints {
                // 只进行一次字符串转换
                let timepoint_str = tp.to_string();
                // 先生成任务键并添加到new_keys
                let key = gen_task_key(task.taskid, &timepoint_str);
                new_keys.insert(key);
                // 使用已转换的timepoint_str创建TaskDetail
                new_task_details.push(TaskDetail {
                    taskid: task.taskid,
                    timepoint: timepoint_str, // 避免重复转换
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
        new_tasks: HashMap<i32, Task>,
        new_task_details: Vec<TaskDetail>,
        new_keys: HashSet<String>,
    ) {
        // 第一阶段：在锁外准备需要添加的新任务
        // 先获取读锁来收集old_keys，避免长时间持有写锁
        let old_keys: HashSet<String> = {
            let guard = self.inner.read().await;
            guard
                .taskdetails
                .values()
                .map(|detail| detail.task_key()) // 使用TaskDetail的task_key方法，避免重复代码
                .collect()
        };

        // 在锁外准备需要添加的新任务（过滤掉已存在的）
        let filtered_new_details: Vec<TaskDetail> = new_task_details
            .into_iter()
            .filter(|detail| {
                let key = detail.task_key(); // 使用TaskDetail的task_key方法，避免重复代码
                !old_keys.contains(&key)
            })
            .collect();

        // 第二阶段：获取写锁执行状态更新和数据修改
        {
            let mut guard = self.inner.write().await;

            // 标记需要保留的任务
            for (_, detail) in guard.taskdetails.iter_mut() {
                let key = detail.task_key(); // 使用TaskDetail的task_key方法，避免重复代码

                // 注意：new_keys已经包含了所有需要保留的任务键
                if new_keys.contains(&key) && detail.status == TASK_STATUS_MONITORING {
                    detail.tag = TASK_TAG_KEEP;
                } else {
                    detail.tag = TASK_TAG_DELETE;
                }
            }

            // 添加过滤后的新任务
            for detail in filtered_new_details {
                // 使用(taskid, timepoint)作为键插入，这是guard.taskdetails的实际键类型
                guard
                    .taskdetails
                    .insert((detail.taskid, detail.timepoint.clone()), detail);
            }

            // 更新任务映射
            guard.tasks = new_tasks;
        } // 在这里释放写锁，减少锁持有时间
    }
}
