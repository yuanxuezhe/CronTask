use chrono::{NaiveDate, NaiveTime, NaiveDateTime, Utc};
use chrono_tz::Asia::Shanghai;
use dbcore::{Database,ResultSet};
use macros::SqlCRUD;
use chrono::Datelike;
use chrono::Weekday;
use std::collections::HashSet;

#[derive(Debug)]
#[derive(Default)]
#[derive(Clone)]
#[derive(SqlCRUD)]
#[table_name = "task"]
pub struct Task {
    // 任务编号
    #[primary_key]
    pub taskid: i32,
    // 任务名称
    pub taskname: String,
    // 任务开始日期
    pub start_date: String,
    // 任务结束日期
    pub end_date: String,
    // 周期类型 0 每天 1 工作日(跳过节假日) 2 节假日  3 自定义
    pub cycle_type: String, // 任务周期类型  每天 D  每周13   W{1,3}  每月13 M{1,3}  每年 Y{1,3}
    // 所属周期  每天 D  每周13   W{1,3}  每月13 M{1,3}  每年 Y{1,3}
    pub period: String, 
    // 任务时间点
    pub time_point: String,
    // 重试类型：0 不重试  1  向后 2 向前
    pub retry_type: String,
    // 重试间隔
    pub retry_interval: i32,
    // 重试次数
    pub retry_count: i32,
    // 当前触发次数
    #[none]
    pub current_trigger_count: i32,
    // 任务状态
    pub status: String,
    // 描述
    pub discribe: String,
}

/*
    let task1 = Task {
        taskid: 1,
        taskname: "Task 1".to_string(),
        start_date: "20250512".to_string(),
        end_date: "20250612".to_string(),
        cycle_type: "0".to_string(),
        period: "D".to_string(),
        time_point: "083000".to_string(),
        retry_type: "1".to_string(),
        retry_interval: 10000,
        retry_count: 3,
        current_trigger_count: 0,
        status: "A".to_string(),
    };
*/

/// 解析日期（格式：YYYYMMDD）
fn parse_date(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y%m%d").ok()
}

/// 判断周末（示例实现）
fn is_weekend(date: NaiveDate) -> bool {
    matches!(date.weekday(), Weekday::Sat | Weekday::Sun)
}

/// 判断节假日（示例实现，需替换实际数据）
fn is_holiday(date: NaiveDate) -> bool {
    let holidays: HashSet<NaiveDate> = [
        "20230101", "20230122", "20230123", "20230124", // 示例假期
        "20230405", "20230501", "20230622", "20230929",
    ]
    .iter()
    .filter_map(|s| parse_date(s))
    .collect();
    
    holidays.contains(&date)
}

impl Task {
    /// 获取未来10天内（含今天）的有效触发时间点，最多返回10个
    pub fn next_n_schedules(&self, maxdays: u32) -> Vec<NaiveDateTime> {
        // 1. 准备基础数据
        let current_time = Utc::now().with_timezone(&Shanghai).naive_local();
        // let Ok(task_time) = NaiveTime::parse_from_str(&self.time_point, "%H%M%S") else {
        //     return vec![];
        // };

        // 解析多个时间点
        let time_points: Vec<NaiveTime> = self.time_point
        .split(',')
        .filter_map(|s| NaiveTime::parse_from_str(s.trim(), "%H%M%S").ok())
        .collect();

        if time_points.is_empty() {
            return vec![];
        }

        // 2. 生成候选日期（从今天开始最多10天）
        let candidates = self.generate_candidate_dates(current_time.date(), maxdays)
            .unwrap_or_default();

        // 3. 生成有效时间点
        // candidates
        //     .into_iter()
        //     .filter_map(|date| {
        //         let datetime = date.and_time(task_time);
        //         // 过滤过去时间（允许当天未过的时间点）  && self.is_valid(datetime) 
        //         if datetime > current_time {
        //             Some(datetime)
        //         } else {
        //             None
        //         }
        //     })
        //     //.take(10) // 最多取10个
        //     .collect()
        candidates
        .into_iter()
        .flat_map(|date| {
            time_points.iter().filter_map(move |&tp| {
                let datetime = date.and_time(tp);
                if datetime > current_time {
                    Some(datetime)
                } else {
                    None
                }
            })
        })
        .collect()
    }

    /// 生成候选日期（优化版：动态生成避免全量计算）
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
                None => break, // 处理日期溢出（如9999-12-31之后没有日期）
            };
            remaining -= 1;
        }

        Some(dates)
    }

    /// 验证日期有效性（复用之前逻辑）
    fn is_date_valid(&self, date: NaiveDate) -> bool {
        // 检查起止日期
        let in_range = self.check_date(date).unwrap_or(false);
        
        // 检查周期类型
        in_range && match self.cycle_type.as_str() {
            "0" => self.is_daily_valid(date),
            "1" => self.is_workday(date),
            "2" => self.is_holiday(date),
            "3" => self.is_custom_valid(date),
            _ => false
        }
    }

    /// 检查日期范围
    fn check_date(&self, date: NaiveDate) -> Result<bool, String> {
        let start = parse_date(&self.start_date).ok_or("无效开始日期")?;
        let end = parse_date(&self.end_date).ok_or("无效结束日期")?;
        Ok(date >= start && date <= end)
    }

    /// 每天的有效性检查
    fn is_daily_valid(&self, _: NaiveDate) -> bool {
        true // 无条件通过
    }

    /// 工作日检查（需要接入节假日数据）
    fn is_workday(&self, date: NaiveDate) -> bool {
        !is_holiday(date) && !is_weekend(date)
    }

    /// 节假日检查
    fn is_holiday(&self, date: NaiveDate) -> bool {
        is_holiday(date)
    }

    /// 自定义周期检查
    fn is_custom_valid(&self, date: NaiveDate) -> bool {
        match self.period.chars().next() {
            Some('D') => true,
            Some('W') => self.check_weekly(date),
            Some('M') => self.check_monthly(date),
            Some('Y') => self.check_yearly(date),
            _ => false
        }
    }

    /// 周规则检查（示例：W13 表示每周一、三）
    fn check_weekly(&self, date: NaiveDate) -> bool {
        let weekday = date.weekday() as u8 + 1; // 周一=1,...,周日=7
        self.period[1..]
            .chars()
            .filter_map(|c| c.to_digit(10))
            .any(|d| d == weekday as u32)
    }

    /// 月规则检查（示例：M15 表示每月15号）
    fn check_monthly(&self, date: NaiveDate) -> bool {
        let day = date.day();
        self.period[1..]
            .chars()
            .filter_map(|c| c.to_digit(10))
            .any(|d| d == day)
    }

    /// 年规则检查（示例：Y0101 表示每年1月1日）
    fn check_yearly(&self, date: NaiveDate) -> bool {
        let md = format!("{:02}{:02}", date.month(), date.day());
        self.period[1..] == md
    }
}