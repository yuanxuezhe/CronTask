use std::sync::Arc;
use chrono::{Utc, Local, NaiveDateTime};
use chrono_tz::Asia::Shanghai;
use crate::crontask::core::CronTask;
use crate::consts::*;
use crate::utils::gen_task_key;

impl CronTask {
    pub fn on_call_back(self: &Arc<Self>, key: String, eventdata: String) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            this.on_call_back_inner(key, eventdata).await;
        });
    }
    pub async fn on_call_back_inner(self: &Arc<Self>, key: String, eventdata: String) {
        let now = Utc::now().with_timezone(&Shanghai);
        println!("[{}] 执行任务[{}]: {}", now, key, eventdata);
        if key == RELOAD_TASK_NAME {
            let _ = self.schedule(
                Local::now().naive_local(), 
                self.reload_interval, 
                key.clone(), 
                eventdata.clone()
            ).await;
            self.reload_tasks().await;
            return;
        }
        let parts: Vec<&str> = key.split(TASK_KEY_SEPARATOR).collect();
        if parts.len() != 2 {
            eprintln!("任务名称格式错误: {}", key);
            return;
        }
        let task_id: i32 = match parts[0].parse() {
            Ok(id) => id,
            Err(_) => {
                eprintln!("任务ID解析失败: {}", parts[0]);
                return;
            }
        };
        let time_point: NaiveDateTime = match NaiveDateTime::parse_from_str(parts[1], DATETIME_FORMAT) {
            Ok(tp) => tp,
            Err(_) => {
                eprintln!("时间点解析失败: {}", parts[1]);
                return;
            }
        };
        let (task, taskdetail) = {
            let guard = self.inner.lock().await;
            let task = match guard.tasks.get(&task_id) {
                Some(task) => task.clone(),
                None => {
                    eprintln!("任务ID {} 不存在", task_id);
                    return;
                }
            };
            let taskdetail = guard.taskdetails
                .iter()
                .find(|detail| gen_task_key(detail.taskid, &detail.timepoint) == key)
                .cloned();
            (task, taskdetail)
        };
        let mut taskdetail = match taskdetail {
            Some(detail) => detail,
            None => {
                eprintln!("任务明细不存在: {}", key);
                return;
            }
        };
        if taskdetail.current_trigger_count >= task.retry_count {
            println!("任务 {} 达到最大重试次数，停止调度", task.taskname);
            taskdetail.status = TASK_STATUS_UNMONITORED;
            let mut guard = self.inner.lock().await;
            if let Some(detail) = guard.taskdetails
                .iter_mut()
                .find(|d| gen_task_key(d.taskid, &d.timepoint) == key) {
                *detail = taskdetail;
            }
            return;
        }
        taskdetail.current_trigger_count += 1;
        match self.schedule(
            time_point,
            (task.retry_interval * taskdetail.current_trigger_count).try_into().unwrap(),
            key.clone(),
            self.build_task_message(task.discribe.clone(), taskdetail.current_trigger_count),
        ).await {
            Ok(_) => {
                taskdetail.status = TASK_STATUS_RETRY;
                println!("任务重试调度成功: {}", key);
            },
            Err(e) => {
                taskdetail.status = TASK_STATUS_UNMONITORED;
                eprintln!("任务重试调度失败: {} - {}", key, e);
            },
        }
        let mut guard = self.inner.lock().await;
        if let Some(detail) = guard.taskdetails
            .iter_mut()
            .find(|d| gen_task_key(d.taskid, &d.timepoint) == key) {
            *detail = taskdetail;
        }
    }
} 