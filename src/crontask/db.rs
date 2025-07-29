use std::collections::HashMap;
use crate::task::Task;

impl crate::crontask::core::CronTask {
    /// 从数据库加载任务
    pub async fn load_tasks_from_db(&self) -> Result<HashMap<i32, Task>, Box<dyn std::error::Error>> {
        let rs = self.db.open("select * from task where taskid >= ?").set_param(0).query(&self.db).await?;
        let mut tasks = HashMap::new();

        for row_data in rs.iter() {
            let task = Task {
                taskid: row_data.get("taskid")?,
                taskname: row_data.get("taskname")?,
                start_date: row_data.get("start_date")?,
                end_date: row_data.get("end_date")?,
                cycle_type: row_data.get("cycle_type")?,
                period: row_data.get("period")?,
                time_point: row_data.get("time_point")?,
                retry_type: row_data.get("retry_type")?,
                retry_interval: row_data.get("retry_interval")?,
                retry_count: row_data.get("retry_count")?,
                status: row_data.get("status")?,
                discribe: row_data.get("discribe")?,
            };
            tasks.insert(task.taskid, task);
        }
        Ok(tasks)
    }
    /// 从缓存获取所有任务
    pub async fn get_all_tasks_from_cache(&self) -> HashMap<i32, Task> {
        let guard = self.inner.lock().await;
        guard.tasks.clone()
    }
} 