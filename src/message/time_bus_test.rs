#[cfg(test)]
mod tests {
    use crate::message::time_bus::TimeBus;
    use std::sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    };
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_time_bus_callbacks_with_logging() {
        let time_bus = TimeBus::new();

        // 创建原子计数器来跟踪回调调用次数
        let millisecond_callback_count = Arc::new(AtomicU32::new(0));
        let second_callback_count = Arc::new(AtomicU32::new(0));
        let minute_callback_count = Arc::new(AtomicU32::new(0));

        let millisecond_count_clone = millisecond_callback_count.clone();
        let second_count_clone = second_callback_count.clone();
        let minute_count_clone = minute_callback_count.clone();

        // 注册毫秒回调
        time_bus
            .register_callback(1, move |_pulse| {
                millisecond_count_clone.fetch_add(1, Ordering::Relaxed);
            })
            .await;

        // 注册秒回调
        time_bus
            .register_callback(2, move |_pulse| {
                second_count_clone.fetch_add(1, Ordering::Relaxed);
            })
            .await;

        // 注册分钟回调 (组合信号：毫秒|秒 = 1|2 = 3)
        time_bus
            .register_callback(3, move |pulse| {
                let count = minute_count_clone.fetch_add(1, Ordering::Relaxed) + 1;
                println!(
                    "[{}] 组合回调 #{}: 时间戳 = {}",
                    pulse.timestamp.format("%Y-%m-%d %H:%M:%S%.3f"),
                    count,
                    pulse.timestamp.timestamp()
                );
            })
            .await;

        // 等待一段时间以收集回调
        tokio::time::sleep(Duration::from_secs(3)).await;

        // 获取最终计数
        let millisecond_calls = millisecond_callback_count.load(Ordering::Relaxed);
        let second_calls = second_callback_count.load(Ordering::Relaxed);
        let minute_calls = minute_callback_count.load(Ordering::Relaxed);

        println!("测试结果:");
        println!("  - 毫秒回调被调用 {} 次", millisecond_calls);
        println!("  - 秒回调被调用 {} 次", second_calls);
        println!("  - 分钟回调被调用 {} 次", minute_calls);

        // 验证回调确实被调用了
        assert!(millisecond_calls > 0, "毫秒回调未被调用");
        assert!(second_calls > 0, "秒回调未被调用");
        assert!(minute_calls > 0, "分钟回调未被调用");

        println!("时间总线回调测试通过!");
    }

    #[tokio::test]
    async fn test_combined_signal_callbacks() {
        let time_bus = TimeBus::new();

        // 创建原子计数器来跟踪回调调用次数
        let millisecond_callback_count = Arc::new(AtomicU32::new(0));
        let second_callback_count = Arc::new(AtomicU32::new(0));
        let combined_callback_count = Arc::new(AtomicU32::new(0));

        let millisecond_count_clone = millisecond_callback_count.clone();
        let second_count_clone = second_callback_count.clone();
        let combined_count_clone = combined_callback_count.clone();

        // 注册毫秒回调
        time_bus
            .register_callback(1, move |_pulse| {
                millisecond_count_clone.fetch_add(1, Ordering::Relaxed);
            })
            .await;

        // 注册秒回调
        time_bus
            .register_callback(2, move |_pulse| {
                second_count_clone.fetch_add(1, Ordering::Relaxed);
            })
            .await;

        // 注册同时订阅毫秒和秒的组合回调 (组合信号：毫秒|秒 = 1|2 = 3)
        time_bus
            .register_callback(3, move |_pulse| {
                combined_count_clone.fetch_add(1, Ordering::Relaxed);
            })
            .await;

        // 等待一段时间以收集回调
        tokio::time::sleep(Duration::from_secs(2)).await;

        // 获取最终计数
        let millisecond_calls = millisecond_callback_count.load(Ordering::Relaxed);
        let second_calls = second_callback_count.load(Ordering::Relaxed);
        let combined_calls = combined_callback_count.load(Ordering::Relaxed);

        println!("组合信号测试结果:");
        println!("  - 毫秒回调被调用 {} 次", millisecond_calls);
        println!("  - 秒回调被调用 {} 次", second_calls);
        println!("  - 组合回调被调用 {} 次", combined_calls);

        // 验证回调确实被调用了
        assert!(millisecond_calls > 0, "毫秒回调未被调用");
        assert!(second_calls > 0, "秒回调未被调用");
        assert!(combined_calls > 0, "组合回调未被调用");

        println!("组合信号回调测试通过!");
    }
}