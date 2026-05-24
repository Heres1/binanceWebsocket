//! 回测系统入口
//!
//! 使用方式：
//!   cargo run --bin backtest -- --mode kline --days 7 --symbol BTCUSDT
//!   cargo run --bin backtest -- --mode optimize --symbol BTCUSDT
//!   cargo run --bin backtest -- --mode replay --file data/recorded/BTCUSDT_20260320.jsonl

use rust_binance_event_driven::backtest::engine::{BacktestConfig, BacktestEngine};
use rust_binance_event_driven::backtest::data_loader::DataLoader;
use rust_binance_event_driven::backtest::strategy_v2::{run_backtest_v2, StrategyV2Config, StrategyType};
use rust_binance_event_driven::clients::binance_client::BinanceClient;
use rust_binance_event_driven::config::{AppConfig, StrategyConfig};

use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

/// 命令行参数解析（轻量实现，不依赖clap）
struct Args {
    mode: String,        // "kline" or "replay"
    days: u64,           // 回测天数
    symbol: String,      // 交易对
    file: Option<String>, // 录制文件路径（replay模式）
    capital: f64,        // 初始资金
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
                "--help" | "-h" => {
                    println!("回测系统 - 动量短线策略回测工具");
                    println!();
                    println!("用法: cargo run --bin backtest -- [OPTIONS]");
                    println!();
                    println!("选项:");
                    println!("  --mode <MODE>       回测模式: kline(默认) / optimize / optimize_v2 / replay");
                    println!("  --days <N>          K线模式回测天数 (默认: 7)");
                    println!("  --symbol <SYMBOL>   交易对 (默认: BTCUSDT)");
                    println!("  --file <PATH>       Replay模式的录制文件路径");
                    println!("  --capital <USDT>    初始资金 (默认: 200)");
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
    let config = match AppConfig::load("config/default.toml") {
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
        commission_rate: 0.001, // 0.1% Binance现货手续费
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
            run_optimization(&engine, &config.strategy);
        }
        "optimize_v2" => {
            // V2多策略结构性优化
            ensure_data(&engine, &args, &config).await;
            run_optimization_v2(&config.strategy, args.capital);
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
        other => {
            eprintln!("未知模式: {} (可选: kline, optimize, replay)", other);
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

        if let Err(e) = loader.download_klines(&client, &args.symbol, "1m", start_ms, now_ms).await {
            eprintln!("下载1m K线失败: {}", e);
            std::process::exit(1);
        }
        if let Err(e) = loader.download_klines(&client, &args.symbol, "5m", start_ms, now_ms).await {
            eprintln!("下载5m K线失败: {}", e);
            std::process::exit(1);
        }
        println!();
    }
}

/// 参数优化器
fn run_optimization(engine: &BacktestEngine, base: &StrategyConfig) {
    println!("🔧 开始参数优化...");
    println!("   基准配置: TP={:.2}% SL={:.2}% Hold={}s CD={}s RSI={:.0}/{:.0} VR={:.1}",
        base.take_profit_pct, base.stop_loss_pct, base.max_hold_seconds,
        base.cooldown_seconds, base.rsi_oversold, base.rsi_overbought,
        base.volume_ratio_threshold);
    println!();

    // 预加载数据到内存（只读一次磁盘）
    let loader = DataLoader::new("data");
    let klines_1m = match loader.load_klines(&base.symbol, "1m") {
        Ok(k) => k,
        Err(e) => { eprintln!("加载1m数据失败: {}", e); return; }
    };
    let klines_5m = match loader.load_klines(&base.symbol, "5m") {
        Ok(k) => k,
        Err(e) => { eprintln!("加载5m数据失败: {}", e); return; }
    };

    // 参数网格
    let tp_values = [0.15, 0.2, 0.3, 0.4, 0.5];
    let sl_values = [0.15, 0.2, 0.3, 0.4];
    let hold_values: [u64; 4] = [180, 300, 600, 900];
    let cd_values: [u64; 3] = [30, 60, 120];
    let rsi_os_values = [25.0, 30.0, 35.0, 40.0];
    let rsi_ob_values = [60.0, 65.0, 70.0, 75.0];
    let vr_values = [1.2, 1.3, 1.5, 2.0];

    let total = tp_values.len() * sl_values.len() * hold_values.len() * cd_values.len()
        * rsi_os_values.len() * rsi_ob_values.len() * vr_values.len();
    println!("   总组合数: {} | 数据已缓存到内存", total);

    struct Result {
        tp: f64, sl: f64, hold: u64, cd: u64, rsi_os: f64, rsi_ob: f64, vr: f64,
        return_pct: f64, win_rate: f64, trades: usize, sharpe: f64, max_dd: f64,
    }

    let mut results: Vec<Result> = Vec::new();
    let mut count = 0;

    for &tp in &tp_values {
        for &sl in &sl_values {
            for &hold in &hold_values {
                for &cd in &cd_values {
                    for &rsi_os in &rsi_os_values {
                        for &rsi_ob in &rsi_ob_values {
                            for &vr in &vr_values {
                                count += 1;
                                if count % 500 == 0 {
                                    println!("   进度: {}/{} ({:.1}%)", count, total, count as f64/total as f64*100.0);
                                }

                                let mut strategy = base.clone();
                                strategy.take_profit_pct = tp;
                                strategy.stop_loss_pct = sl;
                                strategy.max_hold_seconds = hold;
                                strategy.cooldown_seconds = cd;
                                strategy.rsi_oversold = rsi_os;
                                strategy.rsi_overbought = rsi_ob;
                                strategy.volume_ratio_threshold = vr;

                                if let Ok(report) = BacktestEngine::run_backtest_on_data(
                                    &klines_1m, &klines_5m, &strategy, 200.0, 0.001,
                                ) {
                                    if report.total_trades >= 5 {
                                        results.push(Result {
                                            tp, sl, hold, cd, rsi_os, rsi_ob, vr,
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

    println!("\n✅ 优化完成: 有效组合 {}/{}", results.len(), total);

    // 按收益率排序
    results.sort_by(|a, b| b.return_pct.partial_cmp(&a.return_pct).unwrap());

    println!("\n🏆 TOP 10 最优策略配置:");
    println!("   {:<4} {:<6} {:<6} {:<6} {:<5} {:<6} {:<6} {:<5} {:<8} {:<7} {:<6} {:<8} {:<7}",
        "#", "TP%", "SL%", "Hold", "CD", "RSI_L", "RSI_H", "VR", "Return%", "Win%", "Trades", "Sharpe", "MaxDD%");
    println!("   {}", "-".repeat(95));

    for (i, r) in results.iter().take(10).enumerate() {
        println!("   {:<4} {:<6.2} {:<6.2} {:<6} {:<5} {:<6.0} {:<6.0} {:<5.1} {:<8.3} {:<7.1} {:<6} {:<8.2} {:<7.2}",
            i+1, r.tp, r.sl, r.hold, r.cd, r.rsi_os, r.rsi_ob, r.vr,
            r.return_pct, r.win_rate, r.trades, r.sharpe, r.max_dd);
    }

    // 显示最优配置的完整回测报告
    if let Some(best) = results.first() {
        println!("\n🌟 最优策略配置:");
        println!("   take_profit_pct = {:.2}", best.tp);
        println!("   stop_loss_pct = {:.2}", best.sl);
        println!("   max_hold_seconds = {}", best.hold);
        println!("   cooldown_seconds = {}", best.cd);
        println!("   rsi_oversold = {:.0}", best.rsi_os);
        println!("   rsi_overbought = {:.0}", best.rsi_ob);
        println!("   volume_ratio_threshold = {:.1}", best.vr);

        // 运行最优配置的完整报告
        let mut best_strategy = base.clone();
        best_strategy.take_profit_pct = best.tp;
        best_strategy.stop_loss_pct = best.sl;
        best_strategy.max_hold_seconds = best.hold;
        best_strategy.cooldown_seconds = best.cd;
        best_strategy.rsi_oversold = best.rsi_os;
        best_strategy.rsi_overbought = best.rsi_ob;
        best_strategy.volume_ratio_threshold = best.vr;

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
        Err(e) => { eprintln!("加载1m数据失败: {}", e); return; }
    };
    let klines_5m = match loader.load_klines(&base.symbol, "5m") {
        Ok(k) => k,
        Err(e) => { eprintln!("加载5m数据失败: {}", e); return; }
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

    let total_per_strategy = tp_values.len() * sl_values.len() * trailing_values.len()
        * hold_values.len() * cd_values.len() * vr_values.len() * allow_short_values.len()
        * rsi_ob_values.len() * short_tp_values.len() * short_sl_values.len()
        * short_trailing_values.len() * short_hold_values.len() * short_strength_values.len();
    let total = total_per_strategy * strategy_types.len();
    println!("   策略类型: {} | 每类型参数组合: {} | 总组合: {}", strategy_types.len(), total_per_strategy, total);
    println!();

    struct OptResult {
        strategy_name: &'static str,
        strategy_type: StrategyType,
        tp: f64, sl: f64, trailing: f64, hold: u64, cd: u64, vr: f64, allow_short: bool, rsi_ob: f64,
        short_tp: f64, short_sl: f64, short_trailing: f64, short_hold: u64, short_strength: f64,
        return_pct: f64, win_rate: f64, trades: usize, sharpe: f64, max_dd: f64,
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
                                for &short_strength in &short_strength_values {
                                    count += 1;
                                    strategy_count += 1;

                                    if count % 5000 == 0 {
                                        println!("   进度: {}/{} ({:.1}%)", count, total,
                                            count as f64 / total as f64 * 100.0);
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
                                        quantity_per_trade: base.quantity_per_trade,
                                        short_take_profit_pct: short_tp,
                                        short_stop_loss_pct: short_sl,
                                        short_trailing_stop_pct: short_trailing,
                                        short_max_hold_seconds: short_hold,
                                        short_min_trend_strength: short_strength,
                                    };

                                    let report = run_backtest_v2(
                                        &klines_1m, &klines_5m, &config, capital, commission_rate,
                                    );

                                    if report.total_trades >= 5 {
                                        if report.total_return_pct > strategy_best_return {
                                            strategy_best_return = report.total_return_pct;
                                        }
                                        results.push(OptResult {
                                            strategy_name,
                                            strategy_type,
                                            tp, sl, trailing, hold, cd, vr, allow_short, rsi_ob,
                                            short_tp, short_sl, short_trailing, short_hold, short_strength,
                                            return_pct: report.total_return_pct,
                                            win_rate: report.win_rate,
                                            trades: report.total_trades,
                                            sharpe: report.sharpe_ratio,
                                            max_dd: report.max_drawdown_pct,
                                            profit_loss_ratio: report.profit_loss_ratio,
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
        println!("   [OK] {} 完成: {} 组合已测试 | 最优收益: {:.3}%",
            strategy_name, strategy_count, strategy_best_return);
    }

    println!("
{}", "=".repeat(80));
    println!("   V2优化完成: 有效组合 {}/{}", results.len(), total);

    // 按收益率排序
    results.sort_by(|a, b| b.return_pct.partial_cmp(&a.return_pct).unwrap());

    println!("
--- TOP 20 最优策略配置 ---");
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
        println!("
--- 最优策略配置 ---");
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
        println!("
[WARN] 所有策略组合均未产生足够交易");
    }
}
