use crate::task::model::Task;
use dbcore::Database;
use crate::comm::error::CronTaskError;
use std::collections::HashMap;
use std::sync::Arc;

pub struct TaskRepository {
    db: Arc<Database>,
}

impl TaskRepository {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    /// 从数据库中查询所有任务并构建成HashMap返回
    /// 
    /// # 返回值
    /// 成功时返回包含所有任务的HashMap，键为taskid，值为Task对象
    /// 失败时返回错误信息
    pub async fn load_all_tasks(&self) -> Result<HashMap<i32, Task>, CronTaskError> {
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
    
    /// 初始化表结构
    pub async fn init_table(&self) -> Result<(), CronTaskError> {
        Task::init(&*self.db).await?;
        Ok(())
    }
}