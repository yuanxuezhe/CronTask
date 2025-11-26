use chrono::{NaiveDateTime, Utc, NaiveTime, NaiveDate};
use chrono_tz::Asia::Shanghai;
use dbcore::{Database, ResultSet};
use serde::Serialize;

use crate::common::consts::TIME_FORMAT;
use crate::common::error::CronTaskError;
use crate::common::date_checker::parse_date;

#[derive(Debug, Clone)]
pub struct TaskDetail {
    pub taskid: i32,
    pub timepoint: String,
    pub current_trigger_count: i32,
    pub status: i32,
    pub tag: i32,
}

#[derive(Debug, Default, Clone, macros::SqlCRUD, Serialize)]
#[table_name = "task"]
pub struct Task {
    #[primary_key]
    pub taskid: i32,
    pub taskname: String,
    pub start_date: String,
    pub end_date: String,
    pub cycle_type: String,
    pub period: String,
    pub time_point: String,
    pub retry_type: String,
    pub retry_interval: i32,
    pub retry_count: i32,
    pub status: String,
    pub discribe: String,
}

impl TaskDetail {
    /// 生成并返回任务键
    pub fn task_key(&self) -> String {
        format!(
            "{}{}{}",
            self.taskid,
            crate::common::consts::TASK_KEY_SEPARATOR,
            self.timepoint
        )
    }

    /// 更新任务状态
    pub fn update_status(&mut self, new_status: i32) {
        self.status = new_status;
    }

    /// 增加触发次数
    pub fn increment_trigger_count(&mut self) {
        self.current_trigger_count += 1;
    }

    /// 获取当前触发次数
    pub fn get_trigger_count(&self) -> i32 {
        self.current_trigger_count
    }


}

impl Task {
    /// 从数据库中查询所有任务并构建成HashMap返回
    ///
    /// # 参数
    /// * `db` - 数据库连接
    ///
    /// # 返回值
    /// 成功时返回包含所有任务的HashMap，键为taskid，值为Task对象
    /// 失败时返回错误信息
    pub async fn load_tasks_from_db(
        db: &Database,
    ) -> Result<std::collections::HashMap<i32, Task>, Box<dyn std::error::Error + Send + Sync>> {
        let rs = db
            .open("select * from task where taskid >= ?")
            .set_param(0)
            .query(db)
            .await?;
        let mut tasks = std::collections::HashMap::new();

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

    /// 根据任务的调度规则计算指定时间范围内的所有调度时间点列表
    ///
    /// # 参数
    /// * `max_future_time` - 最大未来时间
    ///
    /// # 返回值
    /// 返回指定时间范围内的所有调度时间点列表
    pub fn get_schedules_in_range(&self, max_future_time: NaiveDateTime) -> Vec<NaiveDateTime> {
        let current_time = Utc::now().with_timezone(&Shanghai).naive_local();

        let max_days = (max_future_time - current_time).num_days() as u32 + 1;

        let time_points: Vec<NaiveTime> = self
            .time_point
            .split(',')
            .filter_map(|s| NaiveTime::parse_from_str(s.trim(), TIME_FORMAT).ok())
            .collect();
        if time_points.is_empty() {
            return vec![];
        }
        let candidates = self
            .generate_candidate_dates(current_time.date(), max_days)
            .unwrap_or_default();
        let mut result = Vec::with_capacity(candidates.len() * time_points.len());
        for date in candidates {
            for &tp in &time_points {
                let datetime = date.and_time(tp);
                if datetime > current_time && datetime <= max_future_time {
                    result.push(datetime);
                }
            }
        }
        result
    }

    /// 根据任务调度规则生成未来指定天数内的候选日期列表
    ///
    /// # 参数
    /// * `start` - 开始日期
    /// * `max_days` - 最大天数
    ///
    /// # 返回值
    /// 返回候选日期列表
    fn generate_candidate_dates(&self, start: NaiveDate, max_days: u32) -> Option<Vec<NaiveDate>> {
        let mut dates = Vec::with_capacity(max_days as usize);
        let mut current = start;
        let mut remaining = max_days;
        while remaining > 0 {
            if self.is_date_valid(current) {
                dates.push(current);
            }
            current = match current.succ_opt() {
                Some(next) => next,
                None => break,
            };
            remaining -= 1;
        }
        Some(dates)
    }

    /// 根据任务的周期类型和调度规则检查指定日期是否有效
    ///
    /// # 参数
    /// * `date` - 要检查的日期
    ///
    /// # 返回值
    /// 如果日期有效返回true，否则返回false
    fn is_date_valid(&self, date: NaiveDate) -> bool {
        use crate::common::date_checker::is_date_valid;
        let in_range = self.check_date(date).unwrap_or(false);
        in_range && is_date_valid(date, &self.cycle_type, &self.period)
    }

    /// 检查指定日期是否在任务的开始日期和结束日期之间
    ///
    /// # 参数
    /// * `date` - 要检查的日期
    ///
    /// # 返回值
    /// 成功时返回检查结果，失败时返回错误信息
    fn check_date(&self, date: NaiveDate) -> Result<bool, CronTaskError> {
        use crate::common::error::CronTaskError;
        let start = parse_date(&self.start_date)
            .ok_or(CronTaskError::TaskFormatError("无效开始日期".to_string()))?;
        let end = parse_date(&self.end_date)
            .ok_or(CronTaskError::TaskFormatError("无效结束日期".to_string()))?;
        Ok(date >= start && date <= end)
    }
    

}
