// 标准库导入
use std::sync::Arc;

// 外部 crate 导入
use chrono::{Local, NaiveDateTime, Utc};
use chrono_tz::Asia::Shanghai;

// 内部模块导入
use crate::common::consts::*;
use crate::common::utils::gen_task_key;
use crate::core::core::CronTask;
use crate::message::message_bus::CronMessage;
use crate::task_engine::model::TaskDetail;

/// 回调处理实现
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
    /// 返回 Result<(), String> 表示执行结果
    pub async fn on_call_back_inner(&self, key: String, eventdata: String) -> Result<(), String> {
        let _now = Utc::now().with_timezone(&Shanghai);
        crate::info_log!("task[{}] run with param:{}", key, eventdata);
        
        // 处理重新加载任务的特殊逻辑
        if key == RELOAD_TASK_NAME {
            return self.handle_reload_task(key, eventdata).await;
        }
        
        // 解析任务键
        let (task_id, time_point) = self.parse_task_key(&key)?;
        
        // 获取任务信息
        let task = self.get_task_by_id(task_id).await?;
        
        // 获取任务详情
        let task_key = gen_task_key(task_id, &time_point.to_string());
        let mut taskdetail = self.get_task_detail(&task_key).await?;
        
        // 检查重试次数
        if taskdetail.current_trigger_count >= task.retry_count {
            return self.handle_max_retries_reached(&task, &mut taskdetail).await;
        }
        
        // 增加重试计数并构建消息
        taskdetail.current_trigger_count += 1;
        let message = self.build_task_message(task.discribe, taskdetail.current_trigger_count);
        let delay_ms = (task.retry_interval * taskdetail.current_trigger_count) as u64;
        
        // 调度重试任务
        self.schedule_retry_task(time_point, delay_ms, task_key, message, &mut taskdetail).await?;
        
        // 更新任务详情
        self.update_task_detail(taskdetail).await?;
        Ok(())
    }
}

// 私有辅助方法实现
impl CronTask {
    /// 处理重新加载任务
    async fn handle_reload_task(&self, key: String, eventdata: String) -> Result<(), String> {
        let _ = self.message_bus.send(CronMessage::ScheduleTask {
            timestamp: Local::now().naive_local(), 
            delay_ms: self.reload_interval, 
            key: key.clone(), 
            arg: eventdata,
        });
        
        // 通过消息总线发送重新加载任务消息
        let _ = self.message_bus.send(CronMessage::ReloadTasks);
        Ok(())
    }
    
    /// 解析任务键
    fn parse_task_key(&self, key: &str) -> Result<(i32, NaiveDateTime), String> {
        let parts: Vec<&str> = key.split(TASK_KEY_SEPARATOR).collect();
        if parts.len() != 2 {
            return Err(format!("任务名称格式错误: {}", key));
        }
        
        let task_id: i32 = parts[0].parse()
            .map_err(|_| format!("任务ID解析失败: {}", parts[0]))?;
        let time_point: NaiveDateTime = NaiveDateTime::parse_from_str(parts[1], DATETIME_FORMAT)
            .map_err(|_| format!("时间点解析失败: {}", parts[1]))?;
            
        Ok((task_id, time_point))
    }
    
    /// 根据任务ID获取任务信息
    async fn get_task_by_id(&self, task_id: i32) -> Result<crate::task_engine::model::Task, String> {
        let guard = self.inner.lock().await;
        guard.tasks.get(&task_id)
            .cloned()
            .ok_or_else(|| format!("任务ID {} 不存在", task_id))
    }
    
    /// 获取任务详情
    async fn get_task_detail(&self, task_key: &str) -> Result<TaskDetail, String> {
        let guard = self.inner.lock().await;
        let taskdetail = guard.taskdetails
            .iter()
            .find(|detail| gen_task_key(detail.taskid, &detail.timepoint) == task_key)
            .cloned();
        taskdetail.ok_or_else(|| format!("任务明细不存在: {}", task_key))
    }
    
    /// 处理达到最大重试次数的情况
    async fn handle_max_retries_reached(
        &self, 
        task: &crate::task_engine::model::Task, 
        taskdetail: &mut TaskDetail
    ) -> Result<(), String> {
        crate::info_log!("任务 {} 达到最大重试次数，停止调度", task.taskname);
        taskdetail.status = TASK_STATUS_UNMONITORED;
        self.update_task_detail(taskdetail.clone()).await?;
        Ok(())
    }
    
    /// 调度重试任务
    async fn schedule_retry_task(
        &self,
        time_point: NaiveDateTime,
        delay_ms: u64,
        task_key: String,
        message: String,
        taskdetail: &mut TaskDetail
    ) -> Result<(), String> {
        match self.message_bus.send(CronMessage::ScheduleTask {
            timestamp: time_point,
            delay_ms,
            key: task_key.clone(),
            arg: message,
        }) {
            Ok(_) => {
                taskdetail.status = TASK_STATUS_RETRY;
                crate::info_log!("任务重试调度成功: {}", task_key);
            },
            Err(e) => {
                taskdetail.status = TASK_STATUS_UNMONITORED;
                self.update_task_detail(taskdetail.clone()).await?;
                return Err(format!("任务重试调度失败: {} - {}", task_key, e));
            },
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
    
    /// 根据任务描述和当前触发次数构建任务消息
    /// 
    /// # 参数
    /// * `description` - 任务描述
    /// * `current_trigger_count` - 当前触发次数
    /// 
    /// # 返回值
    /// 返回构建好的任务消息字符串
    pub(crate) fn build_task_message(&self, description: String, current_trigger_count: i32) -> String {
        if current_trigger_count == 0 {
            description
        } else {
            format!("{}（重复提醒第{}次）", description, current_trigger_count)
        }
    }
}