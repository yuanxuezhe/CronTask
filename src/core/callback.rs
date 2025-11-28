// 标准库导入

// 外部 crate 导入
use chrono::{Local, NaiveDateTime};

// 内部模块导入
use crate::common::consts::*;
use crate::common::error::CronTaskError;
use crate::common::utils::gen_task_key;
use crate::core::core::CronTask;

use crate::task_engine::model::TaskDetail;

/// 回调处理实现
impl CronTask {
    // 回调函数包装器，通过消息总线发送任务执行消息
    //
    // # 参数
    // * `key` - 任务唯一标识符
    // * `eventdata` - 任务相关的数据
    // pub fn on_call_back(self: &Arc<Self>, key: String, eventdata: String) {
    //     // 通过消息总线发送执行任务消息，避免直接调用
    //     let message_bus = self.message_bus.clone();
    //     tokio::spawn(async move {
    //         let _ = message_bus.send(CronMessage::ExecuteTask { key, eventdata });
    //     });
    // }

    /// 实际执行回调逻辑的函数
    ///
    /// 处理任务执行，包括重试逻辑和任务状态更新
    ///
    /// # 参数
    /// * `key` - 任务唯一标识符
    ///
    /// # 返回值
    /// 返回 Result<(), String> 表示执行结果
    pub async fn on_call_back_inner(
        &self,
        key: String,
    ) -> Result<(), CronTaskError> {
        crate::info_log!("task[{}] run", key);

        // 处理重新加载任务的特殊逻辑
        if key == RELOAD_TASK_NAME {
            return self.handle_reload_task(key).await.map_err(|e| {
                crate::error_log!("重新加载任务失败: {}", e);
                CronTaskError::ScheduleError(format!("重新加载任务失败: {e}"))
            });
        }

        // 获取任务信息
        let (task_id, time_point, task_key) =
            self.parse_and_validate_task_key(&key).map_err(|e| {
                crate::error_log!("任务键解析失败 ({}): {}", key, e);
                CronTaskError::TaskFormatError(format!("任务键解析失败 ({key}): {e}"))
            })?;

        let task = self.get_task_by_id(task_id).await.map_err(|e| {
            crate::error_log!("获取任务信息失败 (任务ID: {}): {}", task_id, e);
            CronTaskError::TaskNotFound(format!("任务ID {task_id} 信息获取失败: {e}"))
        })?;

        let mut taskdetail = self.get_task_detail(&task_key).await.map_err(|e| {
            crate::error_log!("获取任务详情失败 (任务键: {}): {}", task_key, e);
            CronTaskError::TaskNotFound(format!("任务详情 (键: {task_key}) 获取失败: {e}"))
        })?;

        // 检查并处理重试逻辑
        if taskdetail.current_trigger_count >= task.retry_count {
            return self
                .handle_max_retries_reached(&task, &mut taskdetail)
                .await
                .map_err(|e| {
                    crate::error_log!(
                        "处理最大重试次数失败 (任务ID: {}, 时间点: {}): {}",
                        task.taskid,
                        taskdetail.timepoint,
                        e
                    );
                    CronTaskError::TaskExecutionError(format!(
                        "任务达到最大重试次数 (ID: {}): {}",
                        task.taskid, e
                    ))
                });
        }

        // 处理任务重试
        self.handle_task_retry(&task, time_point, &task_key, &mut taskdetail)
            .await
            .map_err(|e| {
                crate::error_log!(
                    "任务重试处理失败 (任务ID: {}, 当前重试次数: {}): {}",
                    task.taskid,
                    taskdetail.current_trigger_count,
                    e
                );
                CronTaskError::TaskExecutionError(format!(
                    "任务重试处理失败 (ID: {}, 重试次数: {}): {}",
                    task.taskid, taskdetail.current_trigger_count, e
                ))
            })
    }
}

// 私有辅助方法实现
impl CronTask {
    /// 处理重新加载任务
    async fn handle_reload_task(
        &self,
        key: String,
    ) -> Result<(), CronTaskError> {
        let _ = self.message_bus.send_schedule_task(
            Local::now().naive_local(),
            self.reload_interval,
            key.clone(),
        );

        // 通过消息总线发送重新加载任务消息
        let _ = self.message_bus.send_reload_tasks();
        Ok(())
    }

    /// 解析并验证任务键，返回任务ID、时间点和任务键
    fn parse_and_validate_task_key(
        &self,
        key: &str,
    ) -> Result<(i32, NaiveDateTime, String), CronTaskError> {
        let parts: Vec<&str> = key.split(TASK_KEY_SEPARATOR).collect();
        if parts.len() != 2 {
            return Err(CronTaskError::TaskFormatError(format!(
                "任务名称格式错误: {key}"
            )));
        }

        let task_id: i32 = parts[0]
            .parse()
            .map_err(|_| CronTaskError::TaskFormatError(format!("任务ID解析失败: {}", parts[0])))?;
        let time_point: NaiveDateTime = NaiveDateTime::parse_from_str(parts[1], DATETIME_FORMAT)
            .map_err(|_| {
                CronTaskError::TimeConversionFailed(format!("时间点解析失败: {}", parts[1]))
            })?;

        let task_key = gen_task_key(task_id, &time_point.to_string());
        Ok((task_id, time_point, task_key))
    }

    /// 处理任务重试逻辑
    async fn handle_task_retry(
        &self,
        task: &crate::task_engine::model::Task,
        time_point: NaiveDateTime,
        task_key: &str,
        taskdetail: &mut TaskDetail,
    ) -> Result<(), CronTaskError> {
        // 增加重试计数
        taskdetail.increment_trigger_count();
        let delay_ms = (task.retry_interval * taskdetail.get_trigger_count()) as u64;

        // 调度重试任务
        self.schedule_retry_task(
            time_point,
            delay_ms,
            task_key.to_string(),
            taskdetail,
        )
        .await?;

        // 更新任务详情
        self.update_task_detail(taskdetail.clone()).await?;

        Ok(())
    }

    /// 根据任务ID获取任务信息
    async fn get_task_by_id(
        &self,
        task_id: i32,
    ) -> Result<crate::task_engine::model::Task, CronTaskError> {
        // 只读操作，使用read()
        let guard = self.inner.read().await;
        guard
            .tasks
            .get(&task_id)
            .cloned()
            .ok_or_else(|| CronTaskError::TaskNotFound(format!("任务ID {task_id} 获取失败")))
    }

    /// 获取任务详情
    async fn get_task_detail(&self, task_key: &str) -> Result<TaskDetail, CronTaskError> {
        // 只读操作，使用read()
        let guard = self.inner.read().await;
        // 直接使用values()迭代，避免不必要的克隆
        guard
            .taskdetails
            .values()
            .find(|detail| detail.task_key() == task_key)
            .cloned()
            .ok_or_else(|| CronTaskError::TaskNotFound(format!("任务明细不存在: {task_key}")))
    }

    /// 处理达到最大重试次数的情况
    async fn handle_max_retries_reached(
        &self,
        task: &crate::task_engine::model::Task,
        taskdetail: &mut TaskDetail,
    ) -> Result<(), CronTaskError> {
        crate::info_log!("任务 {} 达到最大重试次数，停止调度", task.taskname);
        taskdetail.update_status(TASK_STATUS_UNMONITORED);
        self.update_task_detail(taskdetail.clone()).await?;
        // 更新数据库中的状态
        self.update_task_status_in_db(task.taskid, &taskdetail.timepoint, taskdetail.status)
            .await?;
        Ok(())
    }

    /// 调度重试任务
    async fn schedule_retry_task(
        &self,
        time_point: NaiveDateTime,
        delay_ms: u64,
        task_key: String,
        taskdetail: &mut TaskDetail,
    ) -> Result<(), CronTaskError> {
        match self.message_bus.send_schedule_task(
            time_point,
            delay_ms,
            task_key.clone(),
        ) {
            Ok(_) => {
                taskdetail.update_status(TASK_STATUS_RETRY);
                // crate::info_log!("任务重试调度成功: {}", task_key);
            }
            Err(e) => {
                taskdetail.update_status(TASK_STATUS_UNMONITORED);
                self.update_task_detail(taskdetail.clone()).await?;
                self.update_task_status_in_db(
                    taskdetail.taskid,
                    &taskdetail.timepoint,
                    taskdetail.status,
                )
                .await?;
                return Err(CronTaskError::ScheduleError(format!(
                    "任务重试调度失败: {task_key} - {e}"
                )));
            }
        }
        Ok(())
    }

    /// 持久化更新任务详情
    async fn update_task_detail(&self, taskdetail: TaskDetail) -> Result<(), CronTaskError> {
        // 需要修改任务详情，使用write()
        let mut guard = self.inner.write().await;
        // 使用(taskid, timepoint)作为键更新或插入任务详情
        let key = (taskdetail.taskid, taskdetail.timepoint.clone());
        guard.taskdetails.insert(key, taskdetail);
        // 不需要检查是否存在，insert会直接更新或插入
        Ok(())
    }

    /// 更新数据库中的任务状态
    async fn update_task_status_in_db(
        &self,
        task_id: i32,
        timepoint: &str,
        status: i32,
    ) -> Result<(), CronTaskError> {
        // 在这里实现更新数据库中任务状态的逻辑
        // 由于TaskDetail没有独立的表，我们可以考虑在task表中添加额外的字段
        // 或者创建一个task_status表来跟踪每个任务在特定时间点的状态
        // 当前实现中，我们只是记录日志表明需要更新状态
        crate::info_log!(
            "需要更新任务(taskid={}, timepoint={})的状态为: {}",
            task_id,
            timepoint,
            status
        );
        // 实际的数据库更新操作应该在这里实现
        Ok(())
    }

}
