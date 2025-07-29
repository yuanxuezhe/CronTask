use std::sync::Arc;
use chrono::{Utc, Local, NaiveDateTime};
use chrono_tz::Asia::Shanghai;
use crate::crontask::core::CronTask;
use crate::consts::*;
use crate::utils::gen_task_key;

impl CronTask {
    /// 回调函数包装器，在tokio任务中执行实际的回调逻辑
    /// 
    /// # 参数
    /// * `key` - 任务唯一标识符
    /// * `eventdata` - 任务相关的数据
    pub fn on_call_back(self: &Arc<Self>, key: String, eventdata: String) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(e) = this.on_call_back_inner(key, eventdata).await {
                eprintln!("任务回调执行失败: {}", e);
            }
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
    async fn on_call_back_inner(self: &Arc<Self>, key: String, eventdata: String) -> Result<(), String> {
        let now = Utc::now().with_timezone(&Shanghai);
        println!("[{}] 执行任务[{}]: {}", now, key, eventdata);
        if key == RELOAD_TASK_NAME {
            let _ = self.schedule(
                Local::now().naive_local(), 
                self.reload_interval, 
                key.clone(),
                String::new(),
            ).await;
            self.reload_tasks().await;
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
        
        // 在锁外查找任务详情以减少锁竞争
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
            println!("任务 {} 达到最大重试次数，停止调度", task.taskname);
            taskdetail.status = TASK_STATUS_UNMONITORED;
            let mut guard = self.inner.lock().await;
            if let Some(detail) = guard.taskdetails
                .iter_mut()
                .find(|d| gen_task_key(d.taskid, &d.timepoint) == task_key) {
                *detail = taskdetail;
            }
            return Ok(());
        }
        taskdetail.current_trigger_count += 1;
        let message = self.build_task_message(task.discribe, taskdetail.current_trigger_count);
        let delay_ms = (task.retry_interval * taskdetail.current_trigger_count) as u64;
        
        match self.schedule(
            time_point,
            delay_ms,
            task_key.clone(),
            message,
        ).await {
            Ok(_) => {
                taskdetail.status = TASK_STATUS_RETRY;
                println!("任务重试调度成功: {}", task_key);
            },
            Err(e) => {
                taskdetail.status = TASK_STATUS_UNMONITORED;
                return Err(format!("任务重试调度失败: {} - {}", task_key, e));
            },
        }
        let mut guard = self.inner.lock().await;
        if let Some(detail) = guard.taskdetails
            .iter_mut()
            .find(|d| gen_task_key(d.taskid, &d.timepoint) == task_key) {
            *detail = taskdetail;
        }
        Ok(())
    }
}