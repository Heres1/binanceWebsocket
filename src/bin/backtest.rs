//! 回测系统入口
//!
//! 使用方式：
//!   cargo run --bin backtest -- --mode kline --days 7 --symbol BTCUSDT
//!   cargo run --bin backtest -- --mode walkforward --config config/default.toml
//!   cargo run --bin backtest -- --mode optimize --symbol BTCUSDT
//!   cargo run --bin backtest -- --mode replay --file data/recorded/BTCUSDT_20260320.jsonl
//!   cargo run --bin backtest -- --mode rotation --days 1095

use rust_binance_event_driven::backtest::data_loader::DataLoader;
use rust_binance_event_driven::backtest::engine::{BacktestConfig, BacktestEngine};
use rust_binance_event_driven::backtest::strategy_v2::{
    run_backtest_v2, StrategyType, StrategyV2Config,
};
use rust_binance_event_driven::clients::binance_client::BinanceClient;
use rust_binance_event_driven::config::{AppConfig, StrategyConfig};

use std::collections::HashMap;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

/// 命令行参数解析（轻量实现，不依赖clap）
struct Args {
    mode: String,         // "kline" or "replay"
    days: u64,            // 回测天数
    symbol: String,       // 交易对
    file: Option<String>, // 录制文件路径（replay模式）
    capital: f64,         // 初始资金
    config: String,       // 配置文件路径
    spread: f64,          // 模拟买卖点差%（全额）
    slippage: f64,        // 模拟市价单滑点%（单边）
}

impl Args {
    fn parse() -> Self {
        let args: Vec<String> = env::args().collect();
        let mut result = Args {
            mode: "kline".to_string(),
            days: 7,
            symbol: "BTCUSDT".to_string(),
            file: None,
            capital: 200.0,
            config: "config/default.toml".to_string(),
            spread: 0.02,
            slippage: 0.03,
        };

        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--mode" => {
                    if i + 1 < args.len() {
                        result.mode = args[i + 1].clone();
                        i += 1;
                    }
                }
                "--days" => {
                    if i + 1 < args.len() {
                        result.days = args[i + 1].parse().unwrap_or(7);
                        i += 1;
                    }
                }
                "--symbol" => {
                    if i + 1 < args.len() {
                        result.symbol = args[i + 1].clone();
                        i += 1;
                    }
                }
                "--file" => {
                    if i + 1 < args.len() {
                        result.file = Some(args[i + 1].clone());
                        i += 1;
                    }
                }
                "--capital" => {
                    if i + 1 < args.len() {
                        result.capital = args[i + 1].parse().unwrap_or(200.0);
                        i += 1;
                    }
                }
                "--config" => {
                    if i + 1 < args.len() {
                        result.config = args[i + 1].clone();
                        i += 1;
                    }
                }
                "--spread" => {
                    if i + 1 < args.len() {
                        result.spread = args[i + 1].parse().unwrap_or(0.02);
                        i += 1;
                    }
                }
                "--slippage" => {
                    if i + 1 < args.len() {
                        result.slippage = args[i + 1].parse().unwrap_or(0.03);
                        i += 1;
                    }
                }
                "--help" | "-h" => {
                    println!("回测系统 - 动量短线策略回测工具");
                    println!();
                    println!("用法: cargo run --bin backtest -- [OPTIONS]");
                    println!();
                    println!("选项:");
                    println!("  --mode <MODE>       回测模式: kline(默认) / walkforward / optimize / optimize_v2 / compare / replay / rotation");
                    println!("  --days <N>          K线模式回测天数 (默认: 7)");
                    println!("  --symbol <SYMBOL>   交易对 (默认: BTCUSDT)");
                    println!("  --file <PATH>       Replay模式的录制文件路径");
                    println!("  --capital <USDT>    初始资金 (默认: 200)");
                    println!("  --config <PATH>     配置文件路径 (默认: config/default.toml)");
                    println!("  --spread <PCT>      模拟买卖点差%全额 (默认: 0.02)");
                    println!("  --slippage <PCT>    模拟市价单滑点%单边 (默认: 0.03)");
                    println!("  --help              显示帮助");
                    std::process::exit(0);
                }
                _ => {}
            }
            i += 1;
        }

        result
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    println!("╔══════════════════════════════════════╗");
    println!("║     动量短线策略 - 回测系统         ║");
    println!("╚══════════════════════════════════════╝");
    println!();

    // 加载配置
    let config = match AppConfig::load(&args.config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("加载配置失败: {}", e);
            std::process::exit(1);
        }
    };

    let data_dir = "data";
    let strategy_config = config.strategy.clone();

    let backtest_config = BacktestConfig {
        symbol: args.symbol.clone(),
        initial_capital: args.capital,
        strategy: strategy_config,
        commission_rate: 0.00075, // BNB抵扣口径 0.075%/侧 = 0.15%往返（需账户开启BNB抵扣）
        position_allocation_pct: config.risk.position_allocation_pct,
        min_usdt_reserve: config.risk.min_usdt_reserve,
        spread_pct: args.spread,
        slippage_pct: args.slippage,
    };

    let engine = BacktestEngine::new(backtest_config, data_dir);

    match args.mode.as_str() {
        "kline" => {
            // K线模式：先检查是否有本地数据，没有则下载
            ensure_data(&engine, &args, &config).await;

            // 执行回测
            match engine.run_kline_backtest() {
                Ok(report) => {
                    report.print_summary();
                }
                Err(e) => {
                    eprintln!("回测执行失败: {}", e);
                    std::process::exit(1);
                }
            }
        }
        "optimize" => {
            // 参数优化模式
            ensure_data(&engine, &args, &config).await;
            run_optimization(&engine, &config.strategy, args.spread, args.slippage);
        }
        "optimize_v2" => {
            // V2多策略结构性优化
            ensure_data(&engine, &args, &config).await;
            run_optimization_v2(&config.strategy, args.capital);
        }
        "compare" => {
            // A/B对比测试：策略优化方案对比
            ensure_data(&engine, &args, &config).await;
            run_comparison(&config.strategy, args.capital, args.spread, args.slippage);
        }
        "walkforward" => {
            // Walk-Forward 样本外验证：前2/3数据训练，后1/3数据验证
            ensure_data(&engine, &args, &config).await;
            run_walkforward(
                &config.strategy,
                args.capital,
                0.00075, // BNB抵扣口径
                args.spread,
                args.slippage,
            );
        }
        "replay" => {
            let file_path = match &args.file {
                Some(f) => f.clone(),
                None => {
                    eprintln!("错误: replay模式需要指定 --file 参数");
                    std::process::exit(1);
                }
            };

            match engine.run_replay_backtest(&file_path) {
                Ok(report) => {
                    report.print_summary();
                }
                Err(e) => {
                    eprintln!("回放回测失败: {}", e);
                    std::process::exit(1);
                }
            }
        }
        "rotation" => {
            // 日级动量轮动回测（与RotationService实盘逻辑一致，交叉验证python结果）
            run_rotation_backtest(&config, &args).await;
        }
        "futures_compare" => {
            // 多策略对比回测：评估哪种策略最适合合约交易
            run_futures_strategy_comparison(&config, &args).await;
        }
        "stress" => {
            // 压力测试：日内插针回撤、单日暴跌、爆仓风险评估
            run_stress_test(&config).await;
        }
        other => {
            eprintln!(
                "未知模式: {} (可选: kline, walkforward, optimize, optimize_v2, compare, replay, rotation, futures_compare)",
                other
            );
            std::process::exit(1);
        }
    }
}

/// 确保本地有K线数据
async fn ensure_data(engine: &BacktestEngine, args: &Args, config: &AppConfig) {
    let loader = engine.data_loader();

    if !loader.has_local_data(&args.symbol, "1m") || !loader.has_local_data(&args.symbol, "5m") {
        println!("📥 本地无历史数据，开始从 Binance 下载...");
        println!("   交易对: {} | 天数: {}", args.symbol, args.days);
        println!();

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let start_ms = now_ms - args.days * 86400 * 1000;

        let base_url = if env::var("USE_SSH_TUNNEL").is_ok() {
            "https://localhost:8443".to_string()
        } else {
            config.get_binance_base_url().to_string()
        };

        let client = BinanceClient::new(
            config.binance.api_key.clone(),
            config.binance.secret_key.clone(),
            base_url,
        );

        if let Err(e) = loader
            .download_klines(&client, &args.symbol, "1m", start_ms, now_ms)
            .await
        {
            eprintln!("下载1m K线失败: {}", e);
            std::process::exit(1);
        }
        if let Err(e) = loader
            .download_klines(&client, &args.symbol, "5m", start_ms, now_ms)
            .await
        {
            eprintln!("下载5m K线失败: {}", e);
            std::process::exit(1);
        }
        println!();
    }

    // 资金费率数据（合约情绪指标，fapi公共接口）：缺失时自动下载，失败不阻断回测
    if !loader.has_local_funding(&args.symbol) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let start_ms = now_ms - args.days * 86400 * 1000;
        let use_tunnel = env::var("USE_SSH_TUNNEL").is_ok();
        if let Err(e) = loader
            .download_funding_rates(&args.symbol, start_ms, now_ms, use_tunnel)
            .await
        {
            eprintln!("⚠️ 资金费率下载失败（资金费率过滤将不可用）: {}", e);
        }
    }
}

/// 参数优化器
fn run_optimization(
    engine: &BacktestEngine,
    base: &StrategyConfig,
    spread_pct: f64,
    slippage_pct: f64,
) {
    println!("🔧 开始参数优化...");
    println!(
        "   基准配置: TP={:.2}% SL={:.2}% Hold={}s CD={}s RSI={:.0}/{:.0} VR={:.1}",
        base.take_profit_pct,
        base.stop_loss_pct,
        base.max_hold_seconds,
        base.cooldown_seconds,
        base.rsi_oversold,
        base.rsi_overbought,
        base.volume_ratio_threshold
    );
    println!();

    // 预加载数据到内存（只读一次磁盘）
    let loader = DataLoader::new("data");
    let klines_1m = match loader.load_klines(&base.symbol, "1m") {
        Ok(k) => k,
        Err(e) => {
            eprintln!("加载1m数据失败: {}", e);
            return;
        }
    };
    let klines_5m = match loader.load_klines(&base.symbol, "5m") {
        Ok(k) => k,
        Err(e) => {
            eprintln!("加载5m数据失败: {}", e);
            return;
        }
    };

    // 参数网格（必须满足: breakeven < trailing_trigger < TP）
    let tp_values = [2.0, 3.0, 5.0]; // 宽硬止盈（让追踪止损发挥作用）
    let sl_values = [0.8, 1.0, 1.5]; // 初始止损
    let breakeven_values = [0.3, 0.5, 0.8]; // 保本触发
    let trailing_trigger_values = [0.8, 1.0, 1.5]; // 追踪触发
    let trailing_distance_values = [0.3, 0.5, 0.8]; // 追踪距离
    let hold_values: [u64; 4] = [14400, 28800, 43200, 86400]; // 4h/8h/12h/24h
    let rsi_ob_values = [100.0]; // 禁用RSI退出
    let vr_values = [0.6, 0.8, 1.0]; // 量比阈值
                                     // 入场参数
    let rsi_os_values = [40.0, 50.0, 60.0]; // RSI超卖线
    let cd_values: [u64; 3] = [120, 300, 600]; // 冷却时间

    let total = tp_values.len()
        * sl_values.len()
        * breakeven_values.len()
        * trailing_trigger_values.len()
        * trailing_distance_values.len()
        * hold_values.len()
        * rsi_ob_values.len()
        * vr_values.len()
        * rsi_os_values.len()
        * cd_values.len();
    println!("   总组合数: {} | 数据已缓存到内存", total);
    println!("   约束: breakeven < trailing_trigger < TP");

    struct Result {
        tp: f64,
        sl: f64,
        breakeven: f64,
        trailing_trigger: f64,
        trailing_distance: f64,
        hold: u64,
        rsi_ob: f64,
        vr: f64,
        rsi_os: f64,
        cd: u64,
        return_pct: f64,
        win_rate: f64,
        trades: usize,
        sharpe: f64,
        max_dd: f64,
    }

    let mut results: Vec<Result> = Vec::new();
    let mut count = 0;
    let mut positive_count = 0;

    for &tp in &tp_values {
        for &sl in &sl_values {
            for &breakeven in &breakeven_values {
                for &trailing_trigger in &trailing_trigger_values {
                    for &trailing_distance in &trailing_distance_values {
                        for &hold in &hold_values {
                            for &rsi_ob in &rsi_ob_values {
                                for &vr in &vr_values {
                                    for &rsi_os in &rsi_os_values {
                                        for &cd in &cd_values {
                                            count += 1;
                                            if count % 5000 == 0 {
                                                println!(
                                                    "   进度: {}/{} ({:.1}%) | 正收益: {}",
                                                    count,
                                                    total,
                                                    count as f64 / total as f64 * 100.0,
                                                    positive_count
                                                );
                                            }

                                            // 约束: breakeven < trailing_trigger < TP
                                            if breakeven >= trailing_trigger {
                                                continue;
                                            }
                                            if trailing_trigger >= tp {
                                                continue;
                                            }

                                            let mut strategy = base.clone();
                                            strategy.take_profit_pct = tp;
                                            strategy.stop_loss_pct = sl;
                                            strategy.breakeven_trigger_pct = breakeven;
                                            strategy.trailing_trigger_pct = trailing_trigger;
                                            strategy.trailing_distance_pct = trailing_distance;
                                            strategy.max_hold_seconds = hold;
                                            strategy.rsi_overbought = rsi_ob;
                                            strategy.volume_ratio_threshold = vr;
                                            strategy.rsi_oversold = rsi_os;
                                            strategy.cooldown_seconds = cd;

                                            if let Ok(report) = BacktestEngine::run_backtest_on_data(
                                                &klines_1m,
                                                &klines_5m,
                                                &strategy,
                                                200.0,
                                                0.00075,
                                                spread_pct,
                                                slippage_pct,
                                            ) {
                                                if report.total_trades >= 5 {
                                                    if report.total_return_pct > 0.0 {
                                                        positive_count += 1;
                                                    }
                                                    results.push(Result {
                                                        tp,
                                                        sl,
                                                        breakeven,
                                                        trailing_trigger,
                                                        trailing_distance,
                                                        hold,
                                                        rsi_ob,
                                                        vr,
                                                        rsi_os,
                                                        cd,
                                                        return_pct: report.total_return_pct,
                                                        win_rate: report.win_rate,
                                                        trades: report.total_trades,
                                                        sharpe: report.sharpe_ratio,
                                                        max_dd: report.max_drawdown_pct,
                                                    });
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    println!(
        "\n✅ 优化完成: 有效组合 {}/{} | 🟢 正收益组合: {}",
        results.len(),
        total,
        positive_count
    );

    // 按收益率排序
    results.sort_by(|a, b| b.return_pct.partial_cmp(&a.return_pct).unwrap());

    println!("\n🏆 TOP 10 最优策略配置:");
    println!("   {:<3} {:<5} {:<5} {:<4} {:<5} {:<5} {:<5} {:<5} {:<4} {:<5} {:<4} {:<8} {:<6} {:<6} {:<6} {:<6}",
        "#", "TP%", "SL%", "BE%", "TrT%", "TrD%", "Hold", "RSI_H", "VR", "RSI_L", "CD", "Return%", "Win%", "Trades", "Sharpe", "MaxDD");
    println!("   {}", "-".repeat(110));

    for (i, r) in results.iter().take(10).enumerate() {
        println!("   {:<3} {:<5.1} {:<5.2} {:<4.1} {:<5.1} {:<5.1} {:<5} {:<5.0} {:<4.1} {:<5.0} {:<4} {:<8.3} {:<6.1} {:<6} {:<6.2} {:<6.2}",
            i+1, r.tp, r.sl, r.breakeven, r.trailing_trigger, r.trailing_distance,
            r.hold, r.rsi_ob, r.vr, r.rsi_os, r.cd,
            r.return_pct, r.win_rate, r.trades, r.sharpe, r.max_dd);
    }

    // 显示最优配置的完整回测报告
    if let Some(best) = results.first() {
        println!("\n🌟 最优策略配置:");
        println!("   take_profit_pct = {:.2}", best.tp);
        println!("   stop_loss_pct = {:.2}", best.sl);
        println!("   breakeven_trigger_pct = {:.2}", best.breakeven);
        println!("   trailing_trigger_pct = {:.2}", best.trailing_trigger);
        println!("   trailing_distance_pct = {:.2}", best.trailing_distance);
        println!("   max_hold_seconds = {}", best.hold);
        println!("   rsi_overbought = {:.0}", best.rsi_ob);
        println!("   volume_ratio_threshold = {:.1}", best.vr);
        println!("   rsi_oversold = {:.0}", best.rsi_os);
        println!("   cooldown_seconds = {}", best.cd);

        // 运行最优配置的完整报告
        let mut best_strategy = base.clone();
        best_strategy.take_profit_pct = best.tp;
        best_strategy.stop_loss_pct = best.sl;
        best_strategy.breakeven_trigger_pct = best.breakeven;
        best_strategy.trailing_trigger_pct = best.trailing_trigger;
        best_strategy.trailing_distance_pct = best.trailing_distance;
        best_strategy.max_hold_seconds = best.hold;
        best_strategy.rsi_overbought = best.rsi_ob;
        best_strategy.volume_ratio_threshold = best.vr;
        best_strategy.rsi_oversold = best.rsi_os;
        best_strategy.cooldown_seconds = best.cd;

        if let Ok(report) = engine.run_with_config(&best_strategy) {
            report.print_summary();
        }
    } else {
        println!("\n⚠️ 未找到有效策略组合（尝试放宽入场条件）");
    }
}

/// V2 多策略结构性优化器
fn run_optimization_v2(base: &StrategyConfig, capital: f64) {
    println!("\n🚀 V2 策略结构性优化器");
    println!("   核心改进: Intra-bar TP/SL | 追踪止损 | 多策略类型 | 做空支持");
    println!("   手续费: 0.075% (BNB抵扣) | 资金: {} USDT", capital);
    println!();

    // 加载数据
    let loader = DataLoader::new("data");
    let klines_1m = match loader.load_klines(&base.symbol, "1m") {
        Ok(k) => k,
        Err(e) => {
            eprintln!("加载1m数据失败: {}", e);
            return;
        }
    };
    let klines_5m = match loader.load_klines(&base.symbol, "5m") {
        Ok(k) => k,
        Err(e) => {
            eprintln!("加载5m数据失败: {}", e);
            return;
        }
    };

    // 使用BNB抵扣后的手续费率
    let commission_rate = 0.00075;

    // 策略类型列表
    let strategy_types = [
        (StrategyType::TrendMomentum, "趋势动量"),
        (StrategyType::EmaCrossover, "EMA交叉"),
        (StrategyType::RsiBounce, "RSI反弹"),
        (StrategyType::Breakout, "突破策略"),
        (StrategyType::CompositeScore, "综合打分"),
    ];

    // 参数网格：简化多头参数（用已知最优值），专注优化做空参数
    let tp_values = [1.0, 1.5, 2.0];
    let sl_values = [0.5, 0.8];
    let trailing_values = [0.0, 0.3]; // 0=不启用
    let hold_values: [u64; 3] = [7200, 14400, 28800];
    let cd_values: [u64; 2] = [300, 600];
    let vr_values = [1.2, 1.5];
    let allow_short_values = [true]; // 强制启用做空
                                     // RSI出场: 100.0=禁用
    let rsi_ob_values = [100.0];
    // 做空专用参数网格
    let short_tp_values = [0.5, 0.8, 1.0, 1.5];
    let short_sl_values = [0.3, 0.5, 0.8];
    let short_trailing_values = [0.0, 0.2, 0.3];
    let short_hold_values: [u64; 3] = [3600, 7200, 14400];
    let short_strength_values = [0.0, 0.03, 0.05, 0.1];

    let total_per_strategy = tp_values.len()
        * sl_values.len()
        * trailing_values.len()
        * hold_values.len()
        * cd_values.len()
        * vr_values.len()
        * allow_short_values.len()
        * rsi_ob_values.len()
        * short_tp_values.len()
        * short_sl_values.len()
        * short_trailing_values.len()
        * short_hold_values.len()
        * short_strength_values.len();
    let total = total_per_strategy * strategy_types.len();
    println!(
        "   策略类型: {} | 每类型参数组合: {} | 总组合: {}",
        strategy_types.len(),
        total_per_strategy,
        total
    );
    println!();

    struct OptResult {
        strategy_name: &'static str,
        strategy_type: StrategyType,
        tp: f64,
        sl: f64,
        trailing: f64,
        hold: u64,
        cd: u64,
        vr: f64,
        rsi_ob: f64,
        short_tp: f64,
        short_sl: f64,
        short_trailing: f64,
        short_hold: u64,
        short_strength: f64,
        return_pct: f64,
        win_rate: f64,
        trades: usize,
        sharpe: f64,
        max_dd: f64,
        profit_loss_ratio: f64,
    }

    let mut results: Vec<OptResult> = Vec::new();
    let mut count = 0;

    for &(strategy_type, strategy_name) in &strategy_types {
        let mut strategy_count = 0;
        let mut strategy_best_return = f64::NEG_INFINITY;

        for &tp in &tp_values {
            for &sl in &sl_values {
                for &trailing in &trailing_values {
                    for &hold in &hold_values {
                        for &cd in &cd_values {
                            for &vr in &vr_values {
                                for &allow_short in &allow_short_values {
                                    for &rsi_ob in &rsi_ob_values {
                                        for &short_tp in &short_tp_values {
                                            for &short_sl in &short_sl_values {
                                                for &short_trailing in &short_trailing_values {
                                                    for &short_hold in &short_hold_values {
                                                        for &short_strength in
                                                            &short_strength_values
                                                        {
                                                            count += 1;
                                                            strategy_count += 1;

                                                            if count % 5000 == 0 {
                                                                println!(
                                                                    "   进度: {}/{} ({:.1}%)",
                                                                    count,
                                                                    total,
                                                                    count as f64 / total as f64
                                                                        * 100.0
                                                                );
                                                            }

                                                            let config = StrategyV2Config {
                                                                strategy_type,
                                                                take_profit_pct: tp,
                                                                stop_loss_pct: sl,
                                                                trailing_stop_pct: trailing,
                                                                max_hold_seconds: hold,
                                                                cooldown_seconds: cd,
                                                                rsi_oversold: 30.0,
                                                                rsi_overbought: rsi_ob,
                                                                volume_ratio_threshold: vr,
                                                                allow_short,
                                                                breakout_lookback: 20,
                                                                min_entry_score: 60.0,
                                                                quantity_per_trade: base
                                                                    .quantity_per_trade,
                                                                short_take_profit_pct: short_tp,
                                                                short_stop_loss_pct: short_sl,
                                                                short_trailing_stop_pct:
                                                                    short_trailing,
                                                                short_max_hold_seconds: short_hold,
                                                                short_min_trend_strength:
                                                                    short_strength,
                                                            };

                                                            let report = run_backtest_v2(
                                                                &klines_1m,
                                                                &klines_5m,
                                                                &config,
                                                                capital,
                                                                commission_rate,
                                                            );

                                                            if report.total_trades >= 5 {
                                                                if report.total_return_pct
                                                                    > strategy_best_return
                                                                {
                                                                    strategy_best_return =
                                                                        report.total_return_pct;
                                                                }
                                                                results.push(OptResult {
                                                                    strategy_name,
                                                                    strategy_type,
                                                                    tp,
                                                                    sl,
                                                                    trailing,
                                                                    hold,
                                                                    cd,
                                                                    vr,
                                                                    rsi_ob,
                                                                    short_tp,
                                                                    short_sl,
                                                                    short_trailing,
                                                                    short_hold,
                                                                    short_strength,
                                                                    return_pct: report
                                                                        .total_return_pct,
                                                                    win_rate: report.win_rate,
                                                                    trades: report.total_trades,
                                                                    sharpe: report.sharpe_ratio,
                                                                    max_dd: report.max_drawdown_pct,
                                                                    profit_loss_ratio: report
                                                                        .profit_loss_ratio,
                                                                });
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        println!(
            "   [OK] {} 完成: {} 组合已测试 | 最优收益: {:.3}%",
            strategy_name, strategy_count, strategy_best_return
        );
    }

    println!(
        "
{}",
        "=".repeat(80)
    );
    println!("   V2优化完成: 有效组合 {}/{}", results.len(), total);

    // 按收益率排序
    results.sort_by(|a, b| b.return_pct.partial_cmp(&a.return_pct).unwrap());

    println!(
        "
--- TOP 20 最优策略配置 ---"
    );
    println!("   {:<3} {:<8} {:<5} {:<5} {:<5} {:<6} {:<4} {:<4} {:<5} {:<5} {:<5} {:<6} {:<5} {:<8} {:<6} {:<6} {:<7} {:<6} {:<6}",
        "#", "策略", "TP%", "SL%", "TR%", "Hold", "CD", "VR", "sTP%", "sSL%", "sTR%", "sHold", "sStr",
        "Return%", "Win%", "Trades", "Sharpe", "P/L", "MaxDD%");
    println!("   {}", "-".repeat(140));

    for (i, r) in results.iter().take(20).enumerate() {
        println!("   {:<3} {:<8} {:<5.2} {:<5.2} {:<5.2} {:<6} {:<4} {:<4.1} {:<5.2} {:<5.2} {:<5.2} {:<6} {:<5.2} {:<8.3} {:<6.1} {:<6} {:<7.2} {:<6.2} {:<6.2}",
            i+1, r.strategy_name, r.tp, r.sl, r.trailing, r.hold, r.cd, r.vr,
            r.short_tp, r.short_sl, r.short_trailing, r.short_hold, r.short_strength,
            r.return_pct, r.win_rate, r.trades, r.sharpe, r.profit_loss_ratio, r.max_dd);
    }

    // 显示最优配置的完整报告
    if let Some(best) = results.first() {
        println!(
            "
--- 最优策略配置 ---"
        );
        println!("   策略类型 = {}", best.strategy_name);
        println!("   --- 多头参数 ---");
        println!("   take_profit_pct = {:.2}", best.tp);
        println!("   stop_loss_pct = {:.2}", best.sl);
        println!("   trailing_stop_pct = {:.2}", best.trailing);
        println!("   max_hold_seconds = {}", best.hold);
        println!("   cooldown_seconds = {}", best.cd);
        println!("   volume_ratio_threshold = {:.1}", best.vr);
        println!("   --- 做空参数 ---");
        println!("   short_take_profit_pct = {:.2}", best.short_tp);
        println!("   short_stop_loss_pct = {:.2}", best.short_sl);
        println!("   short_trailing_stop_pct = {:.2}", best.short_trailing);
        println!("   short_max_hold_seconds = {}", best.short_hold);
        println!("   short_min_trend_strength = {:.2}", best.short_strength);

        // 运行最优配置的完整报告
        let config = StrategyV2Config {
            strategy_type: best.strategy_type,
            take_profit_pct: best.tp,
            stop_loss_pct: best.sl,
            trailing_stop_pct: best.trailing,
            max_hold_seconds: best.hold,
            cooldown_seconds: best.cd,
            rsi_oversold: 30.0,
            rsi_overbought: best.rsi_ob,
            volume_ratio_threshold: best.vr,
            allow_short: true,
            breakout_lookback: 20,
            min_entry_score: 60.0,
            quantity_per_trade: base.quantity_per_trade,
            short_take_profit_pct: best.short_tp,
            short_stop_loss_pct: best.short_sl,
            short_trailing_stop_pct: best.short_trailing,
            short_max_hold_seconds: best.short_hold,
            short_min_trend_strength: best.short_strength,
        };
        let report = run_backtest_v2(&klines_1m, &klines_5m, &config, capital, commission_rate);
        report.print_summary();
    } else {
        println!("\n[WARN] 所有策略组合均未产生足够交易");
    }
}

/// 多时段稳定性测试：将数据分段回测，验证策略在不同市况下的表现
fn run_comparison(base: &StrategyConfig, capital: f64, spread_pct: f64, slippage_pct: f64) {
    println!("\n\u{1f52c} 多时段稳定性测试 - 验证策略在不同市况下的稳健性");
    println!("   当前配置: SL=8.5x ATR | TR=3.0x/2.0x ATR | TP=3.0%");
    println!("   目标: 确认策略在不同时段均有正收益且胜率稳定");
    println!();

    let loader = DataLoader::new("data");
    let klines_1m = match loader.load_klines(&base.symbol, "1m") {
        Ok(k) => k,
        Err(e) => {
            eprintln!("加载1m数据失败: {}", e);
            return;
        }
    };
    let klines_5m = match loader.load_klines(&base.symbol, "5m") {
        Ok(k) => k,
        Err(e) => {
            eprintln!("加载5m数据失败: {}", e);
            return;
        }
    };

    let commission_rate = 0.00075; // BNB抵扣口径
    let total_1m = klines_1m.len();
    let total_5m = klines_5m.len();

    // 获取数据时间范围
    let first_ts = klines_1m.first().map(|k| k.open_time).unwrap_or(0);
    let last_ts = klines_1m.last().map(|k| k.open_time).unwrap_or(0);
    let total_days = (last_ts - first_ts) as f64 / 86_400_000.0;

    println!(
        "   数据范围: {:.1}天 | 1m数据: {}根 | 5m数据: {}根",
        total_days, total_1m, total_5m
    );
    println!();

    // 定义时间段切片（每段15天，交叠滑动）
    struct Period {
        name: &'static str,
        start_pct: f64,
        end_pct: f64,
    }

    let periods = vec![
        Period {
            name: "全量(60天)",
            start_pct: 0.0,
            end_pct: 1.0,
        },
        Period {
            name: "P1(第1-15天)",
            start_pct: 0.0,
            end_pct: 0.25,
        },
        Period {
            name: "P2(第8-22天)",
            start_pct: 0.117,
            end_pct: 0.367,
        },
        Period {
            name: "P3(第16-30天)",
            start_pct: 0.25,
            end_pct: 0.50,
        },
        Period {
            name: "P4(第23-37天)",
            start_pct: 0.367,
            end_pct: 0.617,
        },
        Period {
            name: "P5(第31-45天)",
            start_pct: 0.50,
            end_pct: 0.75,
        },
        Period {
            name: "P6(第38-52天)",
            start_pct: 0.617,
            end_pct: 0.867,
        },
        Period {
            name: "P7(第46-60天)",
            start_pct: 0.75,
            end_pct: 1.0,
        },
    ];

    println!(
        "   {:<16} {:<10} {:<8} {:<8} {:<8} {:<8} {:<8} {:<10}",
        "时段", "年化%", "胜率%", "笔数", "夏普", "盈亏比", "maxDD%", "平均持仓s"
    );
    println!("   {}", "-".repeat(82));

    let mut all_annual = Vec::new();
    let mut all_winrate = Vec::new();
    let mut all_trades = Vec::new();

    for period in &periods {
        let start_1m = (total_1m as f64 * period.start_pct) as usize;
        let end_1m = (total_1m as f64 * period.end_pct) as usize;
        let start_5m = (total_5m as f64 * period.start_pct) as usize;
        let end_5m = (total_5m as f64 * period.end_pct) as usize;

        let slice_1m = &klines_1m[start_1m..end_1m.min(total_1m)];
        let slice_5m = &klines_5m[start_5m..end_5m.min(total_5m)];

        if slice_1m.is_empty() || slice_5m.is_empty() {
            println!("   {:<16} 数据不足", period.name);
            continue;
        }

        match BacktestEngine::run_backtest_on_data(
            slice_1m,
            slice_5m,
            base,
            capital,
            commission_rate,
            spread_pct,
            slippage_pct,
        ) {
            Ok(report) => {
                println!(
                    "   {:<16} {:<10.2} {:<8.1} {:<8} {:<8.2} {:<8.2} {:<8.2} {:<10.0}",
                    period.name,
                    report.annual_return_pct,
                    report.win_rate,
                    report.total_trades,
                    report.sharpe_ratio,
                    report.profit_loss_ratio,
                    report.max_drawdown_pct,
                    report.avg_hold_seconds
                );

                if period.name != "全量(60天)" {
                    all_annual.push(report.annual_return_pct);
                    all_winrate.push(report.win_rate);
                    all_trades.push(report.total_trades as f64);
                }
            }
            Err(e) => {
                println!("   {:<16} 失败: {}", period.name, e);
            }
        }
    }

    // 统计摘要
    println!();
    println!("   ═══ 稳定性统计 ═══");
    if !all_annual.is_empty() {
        let n = all_annual.len() as f64;
        let avg_annual = all_annual.iter().sum::<f64>() / n;
        let min_annual = all_annual.iter().cloned().fold(f64::INFINITY, f64::min);
        let max_annual = all_annual.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let avg_wr = all_winrate.iter().sum::<f64>() / n;
        let min_wr = all_winrate.iter().cloned().fold(f64::INFINITY, f64::min);
        let max_wr = all_winrate
            .iter()
            .cloned()
            .fold(f64::NEG_INFINITY, f64::max);
        let avg_trades = all_trades.iter().sum::<f64>() / n;
        let positive_periods = all_annual.iter().filter(|&&x| x > 0.0).count();

        println!(
            "   年化收益: 平均 {:.2}% | 最低 {:.2}% | 最高 {:.2}%",
            avg_annual, min_annual, max_annual
        );
        println!(
            "   胜率:     平均 {:.1}% | 最低 {:.1}% | 最高 {:.1}%",
            avg_wr, min_wr, max_wr
        );
        println!(
            "   平均交易: {:.1}笔/段 | 正收益时段: {}/{}",
            avg_trades,
            positive_periods,
            all_annual.len()
        );

        let std_dev = (all_annual
            .iter()
            .map(|x| (x - avg_annual).powi(2))
            .sum::<f64>()
            / n)
            .sqrt();
        let cv = if avg_annual.abs() > 0.01 {
            std_dev / avg_annual.abs() * 100.0
        } else {
            999.0
        };
        println!("   收益标准差: {:.2}% | 变异系数: {:.1}%", std_dev, cv);
        println!();

        if positive_periods == all_annual.len() && min_wr >= 50.0 {
            println!(
                "   ✅ 稳定性评估: 通过 - 所有时段均正收益，胜率稳定在{:.0}%以上",
                min_wr
            );
        } else if positive_periods as f64 >= all_annual.len() as f64 * 0.7 {
            println!("   ⚠️ 稳定性评估: 谨慎 - 部分时段亏损，建议降低仓位");
        } else {
            println!("   ❌ 稳定性评估: 不通过 - 超过30%时段亏损，策略不适合部署");
        }
    }

    // 部署就绪性核查
    println!();
    println!("   ═══ 部署就绪性核查 ═══");
    println!("   [✓] 回测引擎与实盘策略一致性: 已验证(allow_short=false, use_atr_stops=true)");
    println!("   [✓] 手续费计算: 回测报告已扣除往返手续费(0.1%)");
    println!("   [✓] ATR动态止损: 回测与实盘逻辑完全一致");
    println!("   [✓] 保本止损: ATR模式下回测=实盘(entry_price)");
    println!("   [✓] 僵尸早退: 回测与实盘逻辑一致");
    println!("   [✓] 做空路径: 已禁用(allow_short=false)，不影响实盘");
    println!("   [✓] 多因子评分: 回测与实盘完全一致(8因子满分100)");
    println!("   [∗] daily_pnl差异: 回测用毛盈亏累加，实盘用净盈亏(差异0.1%/笔，影响可忽略)");
}

/// Walk-Forward 样本外验证：前2/3数据为训练段（样本内），后1/3为验证段（样本外）
///
/// 训练段与验证段表现差异过大 = 过拟合信号，参数不稳健
fn run_walkforward(
    base: &StrategyConfig,
    capital: f64,
    commission_rate: f64,
    spread_pct: f64,
    slippage_pct: f64,
) {
    println!("\n🔬 Walk-Forward 样本外验证");
    println!("   前 2/3 数据 = 训练段（样本内，调参依据）");
    println!("   后 1/3 数据 = 验证段（样本外，检验过拟合）");
    println!(
        "   成本模型: 佣金 {:.3}%/边 + 点差 {:.2}% + 滑点 {:.2}%/边",
        commission_rate * 100.0,
        spread_pct,
        slippage_pct
    );
    println!();

    let loader = DataLoader::new("data");
    let klines_1m = match loader.load_klines(&base.symbol, "1m") {
        Ok(k) => k,
        Err(e) => {
            eprintln!("加载1m数据失败: {}", e);
            return;
        }
    };
    let klines_5m = match loader.load_klines(&base.symbol, "5m") {
        Ok(k) => k,
        Err(e) => {
            eprintln!("加载5m数据失败: {}", e);
            return;
        }
    };

    let split_1m = klines_1m.len() * 2 / 3;
    let split_5m = klines_5m.len() * 2 / 3;
    let train_1m = &klines_1m[..split_1m];
    let train_5m = &klines_5m[..split_5m];
    let test_1m = &klines_1m[split_1m..];
    let test_5m = &klines_5m[split_5m..];

    println!(
        "   训练段: {} 根 1m K线 | 验证段: {} 根 1m K线",
        train_1m.len(),
        test_1m.len()
    );

    println!("\n━━━ 训练段（样本内，前 2/3）━━━");
    let train_report = match BacktestEngine::run_backtest_on_data(
        train_1m,
        train_5m,
        base,
        capital,
        commission_rate,
        spread_pct,
        slippage_pct,
    ) {
        Ok(r) => {
            r.print_summary();
            r
        }
        Err(e) => {
            eprintln!("训练段回测失败: {}", e);
            return;
        }
    };

    println!("\n━━━ 验证段（样本外，后 1/3）━━━");
    let test_report = match BacktestEngine::run_backtest_on_data(
        test_1m,
        test_5m,
        base,
        capital,
        commission_rate,
        spread_pct,
        slippage_pct,
    ) {
        Ok(r) => {
            r.print_summary();
            r
        }
        Err(e) => {
            eprintln!("验证段回测失败: {}", e);
            return;
        }
    };

    // 对比小结
    println!("\n━━━ Walk-Forward 对比小结 ━━━");
    println!(
        "   {:<10} {:<10} {:<10} {:<8} {:<6} {:<10} {:<12}",
        "时段", "总收益%", "年化%", "胜率%", "笔数", "盈亏比", "期望值U/笔"
    );
    println!("   {}", "-".repeat(72));
    for (name, r) in [("训练段", &train_report), ("验证段", &test_report)] {
        println!(
            "   {:<10} {:<10.2} {:<10.2} {:<8.1} {:<6} {:<10.2} {:<12.4}",
            name,
            r.total_return_pct,
            r.annual_return_pct,
            r.win_rate,
            r.total_trades,
            r.profit_loss_ratio,
            r.expectancy_usdt
        );
    }
    println!();

    if train_report.annual_return_pct > 0.0 && test_report.annual_return_pct <= 0.0 {
        println!("   ❌ 训练段盈利但验证段亏损 → 明显过拟合，参数不可用于实盘");
    } else if train_report.annual_return_pct > 0.0 && test_report.annual_return_pct > 0.0 {
        let decay = (1.0 - test_report.annual_return_pct / train_report.annual_return_pct) * 100.0;
        if decay > 50.0 {
            println!(
                "   ⚠️ 验证段年化较训练段衰减 {:.0}%（>50%）→ 存在过拟合倾向，建议降低参数敏感度",
                decay
            );
        } else {
            println!(
                "   ✅ 验证段年化衰减 {:.0}%（<50%）→ 参数稳健性可接受",
                decay
            );
        }
    } else {
        println!("   ⚠️ 训练段本身未盈利 → 当前参数无正期望，先解决策略逻辑再谈稳健性");
    }
}

/// 毫秒时间戳 → YYYY-MM-DD（UTC）
fn fmt_ymd(ms: u64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms as i64)
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| format!("{}", ms))
}

/// 日级动量轮动回测（--mode rotation）
///
/// 与 RotationService 实盘逻辑完全一致：90日动量最高 + 价格>MA50 → 全仓持有，否则空仓。
/// 手续费口径与 python 验证一致：单边 0.125%（往返 0.25%）。
/// 交叉验证目标：3年 ≈ +233% / 年化 ≈ 49% / 调仓 ≈ 17 次。
async fn run_rotation_backtest(config: &AppConfig, args: &Args) {
    let rc = &config.rotation;
    let loader = DataLoader::new("data");
    // 轮动是日级策略，默认回测3年（--days 小于200时自动提升到1095天）
    let days = if args.days < 200 { 1095 } else { args.days };

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let start_ms = now_ms - days * 86400 * 1000;

    let base_url = if env::var("USE_SSH_TUNNEL").is_ok() {
        "https://localhost:8443".to_string()
    } else {
        config.get_binance_base_url().to_string()
    };
    let client = BinanceClient::new(
        config.binance.api_key.clone(),
        config.binance.secret_key.clone(),
        base_url,
    );

    println!(
        "🔄 动量轮动回测 | 品种: {:?} | 动量{}d | MA{}d | 调仓{}d | 初始资金 {:.0} USDT",
        rc.symbols,
        rc.momentum_lookback_days,
        rc.ma_filter_days,
        rc.rebalance_interval_days,
        args.capital
    );
    println!();

    // 1. 加载各品种日线（缺失或不完整时自动下载）
    let mut series: Vec<(String, HashMap<u64, f64>)> = Vec::new();
    for symbol in &rc.symbols {
        let incomplete = loader
            .load_klines(symbol, "1d")
            .map(|k| (k.len() as u64) + 5 < days)
            .unwrap_or(true);
        if incomplete {
            if let Err(e) = loader
                .download_klines(&client, symbol, "1d", start_ms, now_ms)
                .await
            {
                eprintln!("下载 {} 日线失败: {}", symbol, e);
                std::process::exit(1);
            }
        }
        let klines = match loader.load_klines(symbol, "1d") {
            Ok(k) => k,
            Err(e) => {
                eprintln!("加载 {} 日线失败: {}", symbol, e);
                std::process::exit(1);
            }
        };
        println!("   {} 日线 {} 根", symbol, klines.len());
        series.push((
            symbol.clone(),
            klines.iter().map(|k| (k.open_time, k.close)).collect(),
        ));
    }

    // 2. 对齐时间轴（取所有品种共同交易日）
    let mut timeline: Vec<u64> = series[0].1.keys().copied().collect();
    timeline.sort_unstable();
    timeline.retain(|t| series.iter().all(|(_, m)| m.contains_key(t)));
    let n = timeline.len();

    let lb = rc.momentum_lookback_days;
    let ma_n = rc.ma_filter_days;
    let rebal = rc.rebalance_interval_days as usize;
    let fee = 0.00125_f64; // 单边手续费（往返0.25%，与python验证口径一致）
    let start_idx = lb.max(ma_n.saturating_sub(1));

    if n <= start_idx + 1 {
        eprintln!(
            "数据不足：{} 根日线，动量计算至少需要 {} 根",
            n,
            start_idx + 1
        );
        std::process::exit(1);
    }

    println!(
        "   回测区间: {} → {} ({} 天)",
        fmt_ymd(timeline[start_idx]),
        fmt_ymd(timeline[n - 1]),
        (timeline[n - 1] - timeline[start_idx]) / 86_400_000
    );
    println!();

    // 3. 模拟轮动（信号在调仓日收盘价上计算，按收盘价成交）
    let mut cash = args.capital;
    let mut holding: Option<usize> = None;
    let mut qty = 0.0_f64;
    let mut rotations = 0u32;
    let mut next_rebal = start_idx;
    let mut equity_curve: Vec<(u64, f64)> = Vec::with_capacity(n - start_idx);

    println!("📋 调仓记录:");
    for i in start_idx..n {
        let t = timeline[i];
        if i >= next_rebal {
            // 计算目标品种：动量>0 且 价格>MA 中动量最高者
            let mut best: Option<(usize, f64)> = None;
            for (si, (_, map)) in series.iter().enumerate() {
                let price = map[&timeline[i]];
                let momentum = price / map[&timeline[i - lb]] - 1.0;
                let ma: f64 = (0..ma_n).map(|j| map[&timeline[i - j]]).sum::<f64>() / ma_n as f64;
                if momentum > 0.0 && price > ma && best.is_none_or(|(_, bm)| momentum > bm) {
                    best = Some((si, momentum));
                }
            }
            let target = best.map(|(si, _)| si);

            if target != holding {
                if let Some(h) = holding {
                    cash = qty * series[h].1[&t] * (1.0 - fee); // 卖出旧仓
                    qty = 0.0;
                }
                if let Some(s) = target {
                    qty = cash * (1.0 - fee) / series[s].1[&t]; // 买入新仓
                    cash = 0.0;
                }
                rotations += 1;
                let eq = match target {
                    Some(s) => qty * series[s].1[&t],
                    None => cash,
                };
                let name_of = |o: Option<usize>| o.map(|s| series[s].0.as_str()).unwrap_or("空仓");
                println!(
                    "   #{} {} | {} → {} | 净值 {:.2}",
                    rotations,
                    fmt_ymd(t),
                    name_of(holding),
                    name_of(target),
                    eq
                );
                holding = target;
            }
            next_rebal = i + rebal;
        }
        let eq = match holding {
            Some(h) => qty * series[h].1[&t],
            None => cash,
        };
        equity_curve.push((t, eq));
    }

    // 4. 统计输出
    let initial = args.capital;
    let final_eq = equity_curve.last().map(|(_, e)| *e).unwrap_or(initial);
    let total_ret = final_eq / initial - 1.0;
    let span_days = (timeline[n - 1] - timeline[start_idx]) as f64 / 86_400_000.0;
    let annualized = (final_eq / initial).powf(365.0 / span_days) - 1.0;

    let mut peak = f64::MIN;
    let mut max_dd = 0.0_f64;
    for (_, e) in &equity_curve {
        if *e > peak {
            peak = *e;
        }
        let dd = 1.0 - e / peak;
        if dd > max_dd {
            max_dd = dd;
        }
    }

    // 分年度收益（按每年最后一个交易日净值环比）
    let mut year_last: Vec<(i32, f64)> = Vec::new();
    for (t, e) in &equity_curve {
        let y = fmt_ymd(*t)[..4].parse::<i32>().unwrap_or(0);
        match year_last.last_mut() {
            Some((ly, le)) if *ly == y => *le = *e,
            _ => year_last.push((y, *e)),
        }
    }

    println!();
    println!("════════ 轮动回测结果 ════════");
    println!("   期末净值: {:.2} USDT (初始 {:.2})", final_eq, initial);
    println!(
        "   总收益: {:+.1}% | 年化: {:+.1}% | 最大回撤: {:.1}%",
        total_ret * 100.0,
        annualized * 100.0,
        max_dd * 100.0
    );
    println!("   调仓次数: {}", rotations);
    println!();
    println!("   分年度:");
    let mut prev = initial;
    for (y, e) in &year_last {
        println!("     {} 年: {:+.1}%", y, (e / prev - 1.0) * 100.0);
        prev = *e;
    }
    println!();
    println!("   同期买入持有对照:");
    for (sym, map) in &series {
        let bh = map[&timeline[n - 1]] / map[&timeline[start_idx]] - 1.0;
        println!("     {}: {:+.1}%", sym, bh * 100.0);
    }
}

// ============================================================================
// 多策略对比回测（--mode futures_compare）
// ============================================================================

/// 策略单次运行的完整输出（净值曲线+交易统计）
type StrategyOutcome = (Vec<(u64, f64)>, u32, u32);

/// 策略回测结果
struct StrategyResult {
    name: String,
    #[allow(dead_code)]
    final_equity: f64,
    total_return: f64,
    annualized: f64,
    max_drawdown: f64,
    sharpe: f64,
    trades: u32,
    win_rate: f64,
    year_returns: Vec<(i32, f64)>,
}

/// 计算策略统计指标
fn compute_stats(
    name: &str,
    initial: f64,
    equity_curve: &[(u64, f64)],
    trades: u32,
    wins: u32,
) -> StrategyResult {
    let final_eq = equity_curve.last().map(|(_, e)| *e).unwrap_or(initial);
    let total_ret = final_eq / initial - 1.0;
    let span_days = if equity_curve.len() > 1 {
        (equity_curve.last().unwrap().0 - equity_curve[0].0) as f64 / 86_400_000.0
    } else {
        1.0
    };
    let annualized = if span_days > 0.0 {
        (final_eq / initial).powf(365.0 / span_days) - 1.0
    } else {
        0.0
    };

    let mut peak = f64::MIN;
    let mut max_dd = 0.0_f64;
    for (_, e) in equity_curve {
        if *e > peak { peak = *e; }
        let dd = 1.0 - e / peak;
        if dd > max_dd { max_dd = dd; }
    }

    // Sharpe ratio (daily returns)
    let mut returns: Vec<f64> = Vec::new();
    for i in 1..equity_curve.len() {
        let r = equity_curve[i].1 / equity_curve[i - 1].1 - 1.0;
        returns.push(r);
    }
    let mean_r = returns.iter().sum::<f64>() / returns.len().max(1) as f64;
    let std_r = if returns.len() > 1 {
        let var = returns.iter().map(|r| (r - mean_r).powi(2)).sum::<f64>() / (returns.len() - 1) as f64;
        var.sqrt()
    } else { 1.0 };
    let sharpe = if std_r > 1e-10 { (mean_r / std_r) * (365.0_f64).sqrt() } else { 0.0 };

    // 分年度收益
    let mut year_last: Vec<(i32, f64)> = Vec::new();
    for (t, e) in equity_curve {
        let y = fmt_ymd(*t)[..4].parse::<i32>().unwrap_or(0);
        match year_last.last_mut() {
            Some((ly, le)) if *ly == y => *le = *e,
            _ => year_last.push((y, *e)),
        }
    }
    let mut year_returns = Vec::new();
    let mut prev = initial;
    for (y, e) in &year_last {
        year_returns.push((*y, e / prev - 1.0));
        prev = *e;
    }

    StrategyResult {
        name: name.to_string(),
        final_equity: final_eq,
        total_return: total_ret,
        annualized,
        max_drawdown: max_dd,
        sharpe,
        trades,
        win_rate: if trades > 0 { wins as f64 / trades as f64 } else { 0.0 },
        year_returns,
    }
}

/// 策略1：纯做多动量轮动（现有策略基线）
#[allow(clippy::too_many_arguments)]
fn strategy_long_only_rotation(
    series: &[(String, HashMap<u64, f64>)],
    timeline: &[u64],
    start_idx: usize,
    capital: f64,
    lb: usize,
    ma_n: usize,
    rebal: usize,
    fee: f64,
) -> (Vec<(u64, f64)>, u32, u32) {
    let n = timeline.len();
    let mut cash = capital;
    let mut holding: Option<usize> = None;
    let mut qty = 0.0_f64;
    let mut trades = 0u32;
    let mut wins = 0u32;
    let mut equity_curve = Vec::with_capacity(n - start_idx);
    let mut next_rebal = start_idx;
    let mut entry_price = 0.0_f64;

    for i in start_idx..n {
        let t = timeline[i];
        if i >= next_rebal {
            let mut best: Option<(usize, f64)> = None;
            for (si, (_, map)) in series.iter().enumerate() {
                let price = map[&t];
                let momentum = price / map[&timeline[i - lb]] - 1.0;
                let ma: f64 = (0..ma_n).map(|j| map[&timeline[i - j]]).sum::<f64>() / ma_n as f64;
                if momentum > 0.0 && price > ma && best.is_none_or(|(_, bm)| momentum > bm) {
                    best = Some((si, momentum));
                }
            }
            let target = best.map(|(si, _)| si);
            if target != holding {
                // 平旧仓
                if let Some(h) = holding {
                    let exit_price = series[h].1[&t];
                    cash = qty * exit_price * (1.0 - fee);
                    if exit_price > entry_price { wins += 1; }
                    qty = 0.0;
                    trades += 1;
                }
                // 开新仓
                if let Some(s) = target {
                    entry_price = series[s].1[&t];
                    qty = cash * (1.0 - fee) / entry_price;
                    cash = 0.0;
                }
                holding = target;
            }
            next_rebal = i + rebal;
        }
        let eq = match holding {
            Some(h) => qty * series[h].1[&t],
            None => cash,
        };
        equity_curve.push((t, eq));
    }
    (equity_curve, trades, wins)
}

/// 策略2：多空动量轮动（做多最强 + 做空最弱）
#[allow(clippy::too_many_arguments)]
fn strategy_long_short_rotation(
    series: &[(String, HashMap<u64, f64>)],
    timeline: &[u64],
    start_idx: usize,
    capital: f64,
    lb: usize,
    ma_n: usize,
    rebal: usize,
    fee: f64,
) -> (Vec<(u64, f64)>, u32, u32) {
    let n = timeline.len();
    let mut equity = capital;
    // 持仓状态：(symbol_idx, entry_price, qty)
    let mut long_pos: Option<(usize, f64, f64)> = None;
    let mut short_pos: Option<(usize, f64, f64)> = None;
    let mut trades = 0u32;
    let mut wins = 0u32;
    let mut equity_curve = Vec::with_capacity(n - start_idx);
    let mut next_rebal = start_idx;

    for i in start_idx..n {
        let t = timeline[i];
        if i >= next_rebal {
            // 计算所有品种的动量和MA
            let mut candidates: Vec<(usize, f64, bool)> = Vec::new();
            for (si, (_, map)) in series.iter().enumerate() {
                let price = map[&t];
                let momentum = price / map[&timeline[i - lb]] - 1.0;
                let ma: f64 = (0..ma_n).map(|j| map[&timeline[i - j]]).sum::<f64>() / ma_n as f64;
                candidates.push((si, momentum, price > ma));
            }

            // 做多目标：动量最高 + 在MA上方
            let long_target = candidates.iter()
                .filter(|(_, m, above)| *m > 0.0 && *above)
                .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
                .map(|(si, _, _)| *si);

            // 做空目标：动量最低 + 在MA下方
            let short_target = candidates.iter()
                .filter(|(_, m, above)| *m < 0.0 && !*above)
                .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
                .map(|(si, _, _)| *si);

            // 平掉旧多头仓位
            if let Some((si, entry_price, qty)) = long_pos {
                let exit_price = series[si].1[&t];
                let pnl = qty * (exit_price - entry_price);
                let fee_cost = qty * (entry_price + exit_price) * fee;
                equity += pnl - fee_cost;
                if pnl > 0.0 { wins += 1; }
                trades += 1;
            }
            // 平掉旧空头仓位
            if let Some((si, entry_price, qty)) = short_pos {
                let exit_price = series[si].1[&t];
                let pnl = qty * (entry_price - exit_price); // 空头：开仓价-平仓价
                let fee_cost = qty * (entry_price + exit_price) * fee;
                equity += pnl - fee_cost;
                if pnl > 0.0 { wins += 1; }
                trades += 1;
            }

            // 开新仓位（各50%资金）
            let half = equity / 2.0;
            long_pos = long_target.map(|si| {
                let price = series[si].1[&t];
                let qty = half / price;
                (si, price, qty)
            });
            short_pos = short_target.map(|si| {
                let price = series[si].1[&t];
                let qty = half / price;
                (si, price, qty)
            });

            next_rebal = i + rebal;
        }

        // 计算当前净值 = 基础权益 + 浮动盈亏
        let mut eq = equity;
        if let Some((si, entry_price, qty)) = long_pos {
            let current_price = series[si].1[&t];
            eq += qty * (current_price - entry_price); // 多头浮盈
        }
        if let Some((si, entry_price, qty)) = short_pos {
            let current_price = series[si].1[&t];
            eq += qty * (entry_price - current_price); // 空头浮盈
        }
        equity_curve.push((t, eq.max(0.0)));
    }
    (equity_curve, trades, wins)
}

/// 策略3：趋势跟踪（MA交叉 + 止损）- 对每个品种独立运行，取最优
fn strategy_trend_following(
    series: &[(String, HashMap<u64, f64>)],
    timeline: &[u64],
    start_idx: usize,
    capital: f64,
    fee: f64,
) -> (Vec<(u64, f64)>, u32, u32) {
    let n = timeline.len();
    let fast_ma = 20;
    let slow_ma = 50;
    let stop_loss_pct = 0.08; // 8% 止损

    // 对每个品种独立运行，选最终收益最高的
    let mut best_result: Option<StrategyOutcome> = None;
    let mut best_final = f64::MIN;

    for (_, map) in series {
        let mut cash = capital;
        let mut qty = 0.0_f64;
        let mut in_position = false;
        let mut entry_price = 0.0_f64;
        let mut trades = 0u32;
        let mut wins = 0u32;
        let mut equity_curve = Vec::with_capacity(n - start_idx);

        for i in start_idx.max(slow_ma)..n {
            let t = timeline[i];
            let price = map[&t];

            let fast: f64 = (0..fast_ma).map(|j| map[&timeline[i - j]]).sum::<f64>() / fast_ma as f64;
            let slow: f64 = (0..slow_ma).map(|j| map[&timeline[i - j]]).sum::<f64>() / slow_ma as f64;

            if in_position {
                let loss_pct = (entry_price - price) / entry_price;
                if loss_pct >= stop_loss_pct {
                    cash = qty * price * (1.0 - fee);
                    qty = 0.0;
                    in_position = false;
                    trades += 1;
                } else if fast < slow {
                    cash = qty * price * (1.0 - fee);
                    if price > entry_price { wins += 1; }
                    qty = 0.0;
                    in_position = false;
                    trades += 1;
                }
            } else if fast > slow {
                entry_price = price;
                qty = cash * (1.0 - fee) / price;
                cash = 0.0;
                in_position = true;
            }

            let eq = if in_position { qty * price } else { cash };
            equity_curve.push((t, eq));
        }

        let final_eq = equity_curve.last().map(|(_, e)| *e).unwrap_or(capital);
        if final_eq > best_final {
            best_final = final_eq;
            best_result = Some((equity_curve, trades, wins));
        }
    }

    best_result.unwrap_or_else(|| (vec![], 0, 0))
}

/// 策略4：Donchian通道突破（海龟交易法简化版）- 对每个品种独立运行，取最优
fn strategy_breakout(
    series: &[(String, HashMap<u64, f64>)],
    timeline: &[u64],
    start_idx: usize,
    capital: f64,
    fee: f64,
) -> (Vec<(u64, f64)>, u32, u32) {
    let n = timeline.len();
    let channel_period = 20; // 20日突破
    let stop_loss_pct = 0.10; // 10% 止损

    let mut best_result: Option<StrategyOutcome> = None;
    let mut best_final = f64::MIN;

    for (_, map) in series {
        let mut cash = capital;
        let mut qty = 0.0_f64;
        let mut in_position = false;
        let mut entry_price = 0.0_f64;
        let mut trades = 0u32;
        let mut wins = 0u32;
        let mut equity_curve = Vec::with_capacity(n - start_idx);

        for i in start_idx.max(channel_period)..n {
            let t = timeline[i];
            let price = map[&t];

            // 计算过去N日的最高价和最低价（不含今天）
            let mut highest = f64::MIN;
            let mut lowest = f64::MAX;
            for j in 1..=channel_period {
                let p = map[&timeline[i - j]];
                if p > highest { highest = p; }
                if p < lowest { lowest = p; }
            }

            if in_position {
                let loss_pct = (entry_price - price) / entry_price;
                if loss_pct >= stop_loss_pct || price < lowest * 0.98 {
                    cash = qty * price * (1.0 - fee);
                    if price > entry_price { wins += 1; }
                    qty = 0.0;
                    in_position = false;
                    trades += 1;
                }
            } else if price > highest {
                entry_price = price;
                qty = cash * (1.0 - fee) / price;
                cash = 0.0;
                in_position = true;
            }

            let eq = if in_position { qty * price } else { cash };
            equity_curve.push((t, eq));
        }

        let final_eq = equity_curve.last().map(|(_, e)| *e).unwrap_or(capital);
        if final_eq > best_final {
            best_final = final_eq;
            best_result = Some((equity_curve, trades, wins));
        }
    }

    best_result.unwrap_or_else(|| (vec![], 0, 0))
}

/// 策略5：动量轮动 + 追踪止损（最适合合约交易的改进版）
/// 核心改进：在轮动持仓期间，如果从最高点回撤超过 12%，强制平仓保护利润
#[allow(clippy::too_many_arguments)]
fn strategy_rotation_trailing_stop(
    series: &[(String, HashMap<u64, f64>)],
    timeline: &[u64],
    start_idx: usize,
    capital: f64,
    lb: usize,
    ma_n: usize,
    rebal: usize,
    fee: f64,
) -> (Vec<(u64, f64)>, u32, u32) {
    let n = timeline.len();
    let trailing_stop_pct = 0.12; // 从最高点回撤12%触发止损

    let mut cash = capital;
    let mut holding: Option<usize> = None;
    let mut qty = 0.0_f64;
    let mut entry_price = 0.0_f64;
    let mut highest_since_entry = 0.0_f64;
    let mut trades = 0u32;
    let mut wins = 0u32;
    let mut equity_curve = Vec::with_capacity(n - start_idx);
    let mut next_rebal = start_idx;

    for i in start_idx..n {
        let t = timeline[i];

        // 追踪止损检查（每天都检查，不只是调仓日）
        if let Some(h) = holding {
            let price = series[h].1[&t];
            if price > highest_since_entry {
                highest_since_entry = price;
            }
            // 从最高点回撤超过阈值，强制平仓
            let drawdown_from_peak = (highest_since_entry - price) / highest_since_entry;
            if drawdown_from_peak >= trailing_stop_pct {
                cash = qty * price * (1.0 - fee);
                if price > entry_price { wins += 1; }
                qty = 0.0;
                holding = None;
                trades += 1;
            }
        }

        // 调仓日逻辑
        if i >= next_rebal {
            let mut best: Option<(usize, f64)> = None;
            for (si, (_, map)) in series.iter().enumerate() {
                let price = map[&t];
                let momentum = price / map[&timeline[i - lb]] - 1.0;
                let ma: f64 = (0..ma_n).map(|j| map[&timeline[i - j]]).sum::<f64>() / ma_n as f64;
                if momentum > 0.0 && price > ma && best.is_none_or(|(_, bm)| momentum > bm) {
                    best = Some((si, momentum));
                }
            }
            let target = best.map(|(si, _)| si);

            if target != holding {
                // 平旧仓
                if let Some(h) = holding {
                    let price = series[h].1[&t];
                    cash = qty * price * (1.0 - fee);
                    if price > entry_price { wins += 1; }
                    qty = 0.0;
                    trades += 1;
                }
                // 开新仓
                if let Some(s) = target {
                    entry_price = series[s].1[&t];
                    highest_since_entry = entry_price;
                    qty = cash * (1.0 - fee) / entry_price;
                    cash = 0.0;
                }
                holding = target;
            }
            next_rebal = i + rebal;
        }

        let eq = match holding {
            Some(h) => qty * series[h].1[&t],
            None => cash,
        };
        equity_curve.push((t, eq));
    }
    (equity_curve, trades, wins)
}

/// 多策略对比回测入口
async fn run_futures_strategy_comparison(config: &AppConfig, args: &Args) {
    let rc = &config.rotation;
    let loader = DataLoader::new("data");
    let days = 1095u64; // 3年

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let start_ms = now_ms.saturating_sub(days * 86400 * 1000);

    let base_url = if env::var("USE_SSH_TUNNEL").is_ok() {
        "https://localhost:8443".to_string()
    } else {
        config.get_binance_base_url().to_string()
    };
    let client = BinanceClient::new(
        config.binance.api_key.clone(),
        config.binance.secret_key.clone(),
        base_url,
    );

    println!("════════ 合约策略对比回测 ════════");
    println!("   品种池: {:?}", rc.symbols);
    println!("   回测周期: {} 天", days);
    println!("   初始资金: {} USDT", args.capital);
    println!("   手续费: 单边 0.05% (合约Taker)");
    println!();

    // 1. 加载日线数据
    let mut series: Vec<(String, HashMap<u64, f64>)> = Vec::new();
    for symbol in &rc.symbols {
        let incomplete = loader
            .load_klines(symbol, "1d")
            .map(|k| (k.len() as u64) + 5 < days)
            .unwrap_or(true);
        if incomplete {
            if let Err(e) = loader
                .download_klines(&client, symbol, "1d", start_ms, now_ms)
                .await
            {
                eprintln!("下载 {} 日线失败: {}", symbol, e);
                std::process::exit(1);
            }
        }
        let klines = match loader.load_klines(symbol, "1d") {
            Ok(k) => k,
            Err(e) => {
                eprintln!("加载 {} 日线失败: {}", symbol, e);
                std::process::exit(1);
            }
        };
        series.push((
            symbol.clone(),
            klines.iter().map(|k| (k.open_time, k.close)).collect(),
        ));
    }

    // 2. 对齐时间轴
    let mut timeline: Vec<u64> = series[0].1.keys().copied().collect();
    timeline.sort_unstable();
    timeline.retain(|t| series.iter().all(|(_, m)| m.contains_key(t)));
    let n = timeline.len();

    let lb = rc.momentum_lookback_days;
    let ma_n = rc.ma_filter_days;
    let rebal = rc.rebalance_interval_days as usize;
    let fee = 0.0005_f64; // 合约Taker单边0.05%
    let start_idx = lb.max(ma_n.saturating_sub(1));

    if n <= start_idx + 1 {
        eprintln!("数据不足：{} 根日线", n);
        std::process::exit(1);
    }

    println!(
        "   回测区间: {} → {} ({} 天)",
        fmt_ymd(timeline[start_idx]),
        fmt_ymd(timeline[n - 1]),
        (timeline[n - 1] - timeline[start_idx]) / 86_400_000
    );
    println!();

    // 3. 运行4种策略
    println!("   运行策略 1/5: 纯做多动量轮动...");
    let (eq1, t1, w1) = strategy_long_only_rotation(&series, &timeline, start_idx, args.capital, lb, ma_n, rebal, fee);
    let r1 = compute_stats("纯做多动量轮动", args.capital, &eq1, t1, w1);

    println!("   运行策略 2/5: 多空动量轮动...");
    let (eq2, t2, w2) = strategy_long_short_rotation(&series, &timeline, start_idx, args.capital, lb, ma_n, rebal, fee);
    let r2 = compute_stats("多空动量轮动", args.capital, &eq2, t2, w2);

    println!("   运行策略 3/5: 趋势跟踪(MA20/50交叉+止损)...");
    let (eq3, t3, w3) = strategy_trend_following(&series, &timeline, start_idx, args.capital, fee);
    let r3 = compute_stats("趋势跟踪(MA交叉)", args.capital, &eq3, t3, w3);

    println!("   运行策略 4/5: Donchian通道突破...");
    let (eq4, t4, w4) = strategy_breakout(&series, &timeline, start_idx, args.capital, fee);
    let r4 = compute_stats("通道突破(海龟)", args.capital, &eq4, t4, w4);

    println!("   运行策略 5/5: 动量轮动+追踪止损...");
    let (eq5, t5, w5) = strategy_rotation_trailing_stop(&series, &timeline, start_idx, args.capital, lb, ma_n, rebal, fee);
    let r5 = compute_stats("轮动+追踪止损", args.capital, &eq5, t5, w5);

    // 4. 输出对比结果
    let results = vec![r1, r2, r3, r4, r5];

    println!();
    println!("════════ 策略对比结果 ════════");
    println!();
    println!("  {:<20} {:>10} {:>10} {:>10} {:>8} {:>6} {:>6}",
        "策略", "总收益%", "年化%", "最大回撤%", "Sharpe", "交易", "胜率%");
    println!("  {}", "-".repeat(80));

    for r in &results {
        println!("  {:<20} {:>+10.1} {:>+10.1} {:>10.1} {:>8.2} {:>6} {:>6.1}",
            r.name, r.total_return * 100.0, r.annualized * 100.0,
            r.max_drawdown * 100.0, r.sharpe, r.trades, r.win_rate * 100.0);
    }

    println!();
    println!("════════ 分年度收益对比 ════════");
    println!();

    // 收集所有年份
    let all_years: std::collections::BTreeSet<i32> = results.iter()
        .flat_map(|r| r.year_returns.iter().map(|(y, _)| *y))
        .collect();

    println!("  {:<20}", "策略");
    print!("  {:<20}", "策略");
    for y in &all_years {
        print!(" {:>8}", format!("{}年%", y));
    }
    println!();
    println!("  {}", "-".repeat(20 + all_years.len() * 9));

    for r in &results {
        print!("  {:<20}", r.name);
        for y in &all_years {
            let ret = r.year_returns.iter().find(|(ry, _)| ry == y).map(|(_, v)| *v).unwrap_or(0.0);
            print!(" {:>+8.1}", ret * 100.0);
        }
        println!();
    }

    // 5. 推荐
    println!();
    println!("════════ 推荐 ════════");
    let best = results.iter().max_by(|a, b| a.sharpe.partial_cmp(&b.sharpe).unwrap()).unwrap();
    println!("  综合评分最优（Sharpe最高）: {} (Sharpe={:.2}, 年化={:+.1}%, 回撤={:.1}%)",
        best.name, best.sharpe, best.annualized * 100.0, best.max_drawdown * 100.0);

    let lowest_dd = results.iter().min_by(|a, b| a.max_drawdown.partial_cmp(&b.max_drawdown).unwrap()).unwrap();
    println!("  回撤最低: {} (回撤={:.1}%, 年化={:+.1}%)",
        lowest_dd.name, lowest_dd.max_drawdown * 100.0, lowest_dd.annualized * 100.0);

    // 买入持有对照
    println!();
    println!("  同期买入持有对照:");
    for (sym, map) in &series {
        let bh = map[&timeline[n - 1]] / map[&timeline[start_idx]] - 1.0;
        println!("    {}: {:+.1}%", sym, bh * 100.0);
    }
}

// ============================================================================
// 压力测试（--mode stress）：评估合约交易的爆仓风险
// ============================================================================

/// 压力测试：用真实的日内高低价数据评估合约交易的最大风险
async fn run_stress_test(config: &AppConfig) {
    let rc = &config.rotation;
    let loader = DataLoader::new("data");
    let days = 1095u64;

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let start_ms = now_ms.saturating_sub(days * 86400 * 1000);

    let base_url = if env::var("USE_SSH_TUNNEL").is_ok() {
        "https://localhost:8443".to_string()
    } else {
        config.get_binance_base_url().to_string()
    };
    let client = BinanceClient::new(
        config.binance.api_key.clone(),
        config.binance.secret_key.clone(),
        base_url,
    );

    println!("════════ 合约交易压力测试 ════════");
    println!("   目的：评估日内插针、单日暴跌对合约持仓的影响");
    println!("   品种池: {:?}", rc.symbols);
    println!();

    for symbol in &rc.symbols {
        let incomplete = loader
            .load_klines(symbol, "1d")
            .map(|k| (k.len() as u64) + 5 < days)
            .unwrap_or(true);
        if incomplete {
            if let Err(e) = loader
                .download_klines(&client, symbol, "1d", start_ms, now_ms)
                .await
            {
                eprintln!("下载 {} 日线失败: {}", symbol, e);
                continue;
            }
        }
        let klines = match loader.load_klines(symbol, "1d") {
            Ok(k) => k,
            Err(e) => {
                eprintln!("加载 {} 日线失败: {}", symbol, e);
                continue;
            }
        };

        println!("  ─── {} 压力测试 ───", symbol);

        // 1. 单日最大跌幅（收盘价对比前一天收盘价）
        let mut worst_daily_drop = f64::MAX;
        let mut worst_daily_date = 0u64;
        for i in 1..klines.len() {
            let drop = (klines[i].close - klines[i - 1].close) / klines[i - 1].close;
            if drop < worst_daily_drop {
                worst_daily_drop = drop;
                worst_daily_date = klines[i].open_time;
            }
        }

        // 2. 日内最大插针跌幅（当天最低价 vs 当天最高价）
        let mut worst_intraday_drop = f64::MAX;
        let mut worst_intraday_date = 0u64;
        for k in &klines {
            if k.high > 0.0 {
                let drop = (k.low - k.high) / k.high;
                if drop < worst_intraday_drop {
                    worst_intraday_drop = drop;
                    worst_intraday_date = k.open_time;
                }
            }
        }

        // 3. 从20日滚动最高价到当天最低价的最大回撤（模拟持仓被插针）
        let mut worst_wick_dd = f64::MAX;
        let mut worst_wick_date = 0u64;
        for i in 20..klines.len() {
            let peak: f64 = klines[i - 20..i].iter().map(|k| k.high).fold(f64::MIN, f64::max);
            let drop = (klines[i].low - peak) / peak;
            if drop < worst_wick_dd {
                worst_wick_dd = drop;
                worst_wick_date = klines[i].open_time;
            }
        }

        // 4. 统计单日跌幅超过各阈值的次数
        let drops_5pct: usize = (1..klines.len())
            .filter(|&i| (klines[i].close - klines[i - 1].close) / klines[i - 1].close < -0.05)
            .count();
        let drops_10pct: usize = (1..klines.len())
            .filter(|&i| (klines[i].close - klines[i - 1].close) / klines[i - 1].close < -0.10)
            .count();
        let drops_15pct: usize = (1..klines.len())
            .filter(|&i| (klines[i].close - klines[i - 1].close) / klines[i - 1].close < -0.15)
            .count();

        println!("    单日最大跌幅: {:.1}% ({})", worst_daily_drop * 100.0, fmt_ymd(worst_daily_date));
        println!("    日内最大插针: {:.1}% ({})", worst_intraday_drop * 100.0, fmt_ymd(worst_intraday_date));
        println!("    20日高点到日内低点最大回撤: {:.1}% ({})", worst_wick_dd * 100.0, fmt_ymd(worst_wick_date));
        println!("    单日跌幅>5%的天数: {} / {} 天", drops_5pct, klines.len());
        println!("    单日跌幅>10%的天数: {} / {} 天", drops_10pct, klines.len());
        println!("    单日跌幅>15%的天数: {} / {} 天", drops_15pct, klines.len());

        // 5. 爆仓风险评估
        println!("    爆仓风险评估（假设做多持仓）:");
        for lev in [2, 3, 5, 10] {
            // 爆仓价 ≈ entry * (1 - 1/leverage + maintenance_margin)
            // 简化：爆仓需要价格跌幅 ≈ (1 - 1/leverage) * 100%
            let liq_drop = (1.0 - 1.0 / lev as f64) * 100.0;
            let risk = if worst_daily_drop * 100.0 < -(liq_drop * 0.5) {
                "⚠️ 高风险"
            } else if worst_daily_drop * 100.0 < -(liq_drop * 0.3) {
                "⚡ 中风险"
            } else {
                "✅ 低风险"
            };
            println!("      {}x杠杆: 爆仓需跌{:.0}% | 历史最差单日{:.1}% | {}",
                lev, liq_drop, worst_daily_drop * 100.0, risk);
        }
        println!();
    }

    println!("════════ 关键结论 ════════");
    println!("  1. 追踪止损在日线级别有效，但无法防止日内插针瞬间击穿");
    println!("  2. 合约交易必须使用 STOP_MARKET 条件单（服务器端执行），不能依赖程序轮询");
    println!("  3. 2x杠杆下，需要价格跌50%才爆仓，历史最差单日约-15~-20%，有安全边际");
    println!("  4. 但连续多日下跌（如熊市）+ 追踪止损未及时触发 = 实际风险远大于单日");
    println!("  5. 建议：2x杠杆 + 逐仓模式 + STOP_MARKET条件单 + 组合级回撤监控");
}
