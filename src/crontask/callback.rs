use std::sync::Arc;
use chrono::{Utc, Local, NaiveDateTime};
use chrono_tz::Asia::Shanghai;
use crate::crontask::core::CronTask;
use crate::bus::message_bus::CronMessage;
use crate::comm::consts::*;
use crate::comm::utils::gen_task_key;
use crate::task::TaskDetail;

// 导入日志宏
use crate::info_log;

impl CronTask {
    /// 回调函数包装器，通过消息总线发送任务执行消息
    /// 
    /// # 参数
    /// * `key` - 任务唯一标识符
    /// * `eventdata` - 任务相关的数据
    pub fn on_call_back(self: &Arc<Self>, key: String, eventdata: String) {
        // 通过消息总线发送执行任务消息，避免直接调用
        let message_bus = self.message_bus.clone();
        tokio::spawn(async move {
            let _ = message_bus.send(CronMessage::ExecuteTask { key, eventdata });
        });
    }

    /// 实际执行回调逻辑的函数
    /// 
    /// 处理任务执行，包括重试逻辑和任务状态更新
    /// # 参数
    /// * `key` - 任务唯一标识符
    /// * `eventdata` - 任务相关的数据
    /// 
    /// # 返回值
    /// 返回Result<(), String>表示执行结果
    pub async fn on_call_back_inner(&self, key: String, eventdata: String) -> Result<(), String> {
        let now = Utc::now().with_timezone(&Shanghai);
        info_log!("task[{}] run with param:{}", key, eventdata);
        if key == RELOAD_TASK_NAME {
            let _ = self.message_bus.send(CronMessage::ScheduleTask {
                timestamp: Local::now().naive_local(), 
                delay_ms: self.reload_interval, 
                key: key.clone(), 
                arg: eventdata,
            });
            // 通过消息总线发送重新加载任务消息
            let _ = self.message_bus.send(CronMessage::ReloadTasks);
            return Ok(());
        }
        let parts: Vec<&str> = key.split(TASK_KEY_SEPARATOR).collect();
        if parts.len() != 2 {
            return Err(format!("任务名称格式错误: {}", key));
        }
        let task_id: i32 = parts[0].parse()
            .map_err(|_| format!("任务ID解析失败: {}", parts[0]))?;
        let time_point: NaiveDateTime = NaiveDateTime::parse_from_str(parts[1], DATETIME_FORMAT)
            .map_err(|_| format!("时间点解析失败: {}", parts[1]))?;
        let task = {
            let guard = self.inner.lock().await;
            guard.tasks.get(&task_id)
                .cloned()
                .ok_or_else(|| format!("任务ID {} 不存在", task_id))?
        };
        
        // 在锁外获取任务详情以减少锁竞争
        let task_key = gen_task_key(task_id, &time_point.to_string());
        let mut taskdetail = {
            let guard = self.inner.lock().await;
            let taskdetail = guard.taskdetails
                .iter()
                .find(|detail| gen_task_key(detail.taskid, &detail.timepoint) == task_key)
                .cloned();
            taskdetail.ok_or_else(|| format!("任务明细不存在: {}", task_key))?
        };

        if taskdetail.current_trigger_count >= task.retry_count {
            info_log!("任务 {} 达到最大重试次数，停止调度", task.taskname);
            taskdetail.status = TASK_STATUS_UNMONITORED;
            self.update_task_detail(taskdetail).await?;
            return Ok(());
        }
        taskdetail.current_trigger_count += 1;
        let message = self.build_task_message(task.discribe, taskdetail.current_trigger_count);
        let delay_ms = (task.retry_interval * taskdetail.current_trigger_count) as u64;
        
        match self.message_bus.send(CronMessage::ScheduleTask {
            timestamp: time_point,
            delay_ms,
            key: task_key.clone(),
            arg: message,
        }) {
            Ok(_) => {
                taskdetail.status = TASK_STATUS_RETRY;
                info_log!("任务重试调度成功: {}", task_key);
            },
            Err(e) => {
                taskdetail.status = TASK_STATUS_UNMONITORED;
                self.update_task_detail(taskdetail).await?;
                return Err(format!("任务重试调度失败: {} - {}", task_key, e));
            },
        }
        self.update_task_detail(taskdetail).await?;
        Ok(())
    }

    /// 更新任务详情状态并持久化
    async fn update_task_detail_status(&self, task_key: &str, status: i32) -> Result<(), String> {
        let mut guard = self.inner.lock().await;
        if let Some(detail) = guard.taskdetails
            .iter_mut()
            .find(|d| gen_task_key(d.taskid, &d.timepoint) == task_key) {
            detail.status = status;
        }
        Ok(())
    }

    /// 持久化更新任务详情
    async fn update_task_detail(&self, taskdetail: TaskDetail) -> Result<(), String> {
        let mut guard = self.inner.lock().await;
        if let Some(detail) = guard.taskdetails
            .iter_mut()
            .find(|d| gen_task_key(d.taskid, &d.timepoint) == gen_task_key(taskdetail.taskid, &taskdetail.timepoint)) {
            *detail = taskdetail;
        }
        Ok(())
    }
}