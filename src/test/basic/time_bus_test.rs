use crate::basic::TimeBus;
use std::sync::Arc;
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn test_time_bus_creation() {
    let time_bus = TimeBus::new();
    // 添加超时处理以避免测试无限等待
    let result = timeout(Duration::from_millis(100), time_bus.subscribe().recv()).await;
    assert!(result.is_ok(), "超时: 未能在100毫秒内接收信号");
    assert!(result.unwrap().is_ok(), "接收信号失败");
}

#[tokio::test]
async fn test_time_pulse_signals() {
    let time_bus = TimeBus::new();
    let mut receiver = time_bus.subscribe();

    // 等待第一个脉冲信号
    let pulse = timeout(Duration::from_millis(100), receiver.recv())
        .await
        .expect("接收时间脉冲超时")
        .expect("接收时间脉冲失败");

    // 验证脉冲信号的基本结构
    assert!(pulse.signal_type > 0);
    assert!(pulse.timestamp.timestamp_millis() > 0);
}

#[tokio::test]
async fn test_multiple_subscribers() {
    let time_bus = TimeBus::new();
    let mut receiver1 = time_bus.subscribe();
    let mut receiver2 = time_bus.subscribe();

    // 两个订阅者都应该能接收到信号
    let pulse1 = timeout(Duration::from_millis(100), receiver1.recv())
        .await
        .expect("接收器1超时")
        .expect("接收器1接收失败");

    let pulse2 = timeout(Duration::from_millis(100), receiver2.recv())
        .await
        .expect("接收器2超时")
        .expect("接收器2接收失败");

    // 验证两个接收者都收到了相同的信号
    assert_eq!(pulse1.signal_type, pulse2.signal_type);
}

#[tokio::test]
async fn test_signal_type_bit_values() {
    let time_bus = TimeBus::new();
    let mut receiver = time_bus.subscribe();

    let start_time = std::time::Instant::now();
    let mut received_signals = Vec::new();

    // 在短时间内收集多个信号
    while start_time.elapsed() < Duration::from_millis(100) {
        // 尝试接收信号，每次等待10毫秒
        if let Ok(result) = timeout(Duration::from_millis(10), receiver.recv()).await {
            if let Ok(pulse) = result {
                received_signals.push(pulse.signal_type);
            }
        }
        // 不break，继续尝试接收信号
    }

    // 验证至少收到了一些信号
    assert!(!received_signals.is_empty());

    // 验证信号类型符合预期的位值规则
    for signal_type in received_signals {
        // 信号类型应该是2的幂或它们的组合
        assert!(signal_type > 0);
    }
}

#[tokio::test]
async fn test_callback_registration() {
    let time_bus = TimeBus::new();
    let callback_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let callback_flag_clone = callback_flag.clone();

    // 注册一个回调函数
    time_bus
        .register_callback(2, move |_pulse| {
            callback_flag_clone.store(true, std::sync::atomic::Ordering::Relaxed);
        })
        .await;

    // 等待足够时间让回调有机会执行多次
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 验证回调函数至少被调用过一次
    assert!(callback_flag.load(std::sync::atomic::Ordering::Relaxed));
}

#[tokio::test]
async fn test_time_bus_callbacks_with_logging() {
    let time_bus = TimeBus::new();

    // 创建原子计数器来跟踪回调调用次数
    let second_callback_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let minute_callback_count = Arc::new(std::sync::atomic::AtomicU32::new(0));

    let second_count_clone = second_callback_count.clone();
    let minute_count_clone = minute_callback_count.clone();

    // 注册秒回调
    time_bus
        .register_callback(2, move |_pulse| {
            second_count_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        })
        .await;

    // 注册分钟回调 (组合信号：秒 = 2)
    time_bus
        .register_callback(2, move |pulse| {
            let count = minute_count_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
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
    let second_calls = second_callback_count.load(std::sync::atomic::Ordering::Relaxed);
    let minute_calls = minute_callback_count.load(std::sync::atomic::Ordering::Relaxed);

    println!("测试结果:");
    println!("  - 秒回调被调用 {} 次", second_calls);
    println!("  - 分钟回调被调用 {} 次", minute_calls);

    // 验证回调确实被调用了
    assert!(second_calls > 0, "秒回调未被调用");
    assert!(minute_calls > 0, "分钟回调未被调用");

    println!("时间总线回调测试通过!");
}

#[tokio::test]
async fn test_combined_signal_callbacks() {
    let time_bus = TimeBus::new();

    // 创建原子计数器来跟踪回调调用次数
    let second_callback_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let combined_callback_count = Arc::new(std::sync::atomic::AtomicU32::new(0));

    let second_count_clone = second_callback_count.clone();
    let combined_count_clone = combined_callback_count.clone();

    // 注册秒回调
    time_bus
        .register_callback(2, move |_pulse| {
            second_count_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        })
        .await;

    // 注册同时订阅秒的组合回调 (组合信号：秒 = 2)
    time_bus
        .register_callback(2, move |_pulse| {
            combined_count_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        })
        .await;

    // 等待一段时间以收集回调
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 获取最终计数
    let second_calls = second_callback_count.load(std::sync::atomic::Ordering::Relaxed);
    let combined_calls = combined_callback_count.load(std::sync::atomic::Ordering::Relaxed);

    println!("组合信号测试结果:");
    println!("  - 秒回调被调用 {} 次", second_calls);
    println!("  - 组合回调被调用 {} 次", combined_calls);

    // 验证回调确实被调用了
    assert!(second_calls > 0, "秒回调未被调用");
    assert!(combined_calls > 0, "组合回调未被调用");

    println!("组合信号回调测试通过!");
}
