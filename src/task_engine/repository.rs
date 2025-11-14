// 注意：此文件已弃用，数据库操作现在由 core/db.rs 中的 CronTask 实现处理
// 保留此文件作为将来可能的仓库模式实现参考

#[allow(dead_code)]
use crate::task_engine::model::Task;
use dbcore::Database;
use crate::common::error::CronTaskError;
use std::collections::HashMap;
use std::sync::Arc;

#[allow(dead_code)]
pub struct TaskRepository {
    db: Arc<Database>,
}

#[allow(dead_code)]
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