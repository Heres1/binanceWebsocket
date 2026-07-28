//! 回测统计报告模块
//!
//! 生成包含基础指标、详细指标和逐笔明细的完整回测报告

use serde::{Deserialize, Serialize};

/// 单笔交易记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    /// 交易序号
    pub id: usize,
    /// 入场时间（毫秒时间戳）
    pub entry_time: u64,
    /// 出场时间（毫秒时间戳）
    pub exit_time: u64,
    /// 入场价格
    pub entry_price: f64,
    /// 出场价格
    pub exit_price: f64,
    /// 交易数量
    pub quantity: f64,
    /// 盈亏百分比
    pub pnl_pct: f64,
    /// 盈亏金额(USDT)
    pub pnl_usdt: f64,
    /// 手续费(USDT)
    pub commission: f64,
    /// 持仓时长（秒）
    pub hold_seconds: u64,
    /// 出场原因
    pub exit_reason: String,
}

/// 回测报告
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestReport {
    // === 基础指标 ===
    /// 初始资金
    pub initial_capital: f64,
    /// 最终资金
    pub final_capital: f64,
    /// 总收益率(%)
    pub total_return_pct: f64,
    /// 年化收益率(%)
    pub annual_return_pct: f64,
    /// 胜率(%)
    pub win_rate: f64,
    /// 最大回撤(%)
    pub max_drawdown_pct: f64,
    /// 总交易次数
    pub total_trades: usize,
    /// 盈利笔数
    pub winning_trades: usize,
    /// 亏损笔数
    pub losing_trades: usize,
    /// 平均持仓时间（秒）
    pub avg_hold_seconds: f64,

    // === 详细指标 ===
    /// 夏普比率
    pub sharpe_ratio: f64,
    /// 盈亏比
    pub profit_loss_ratio: f64,
    /// 日均收益率(%)
    pub daily_return_pct: f64,
    /// 最大连续亏损次数
    pub max_consecutive_losses: usize,
    /// 累计手续费
    pub total_commission: f64,
    /// 最大单笔盈利(%)
    pub max_profit_pct: f64,
    /// 最大单笔亏损(%)
    pub max_loss_pct: f64,
    /// 盈利因子（总盈利/总亏损，>1为盈利系统）
    pub profit_factor: f64,
    /// 单笔期望值(USDT，扣手续费后)
    pub expectancy_usdt: f64,

    // === 时间信息 ===
    /// 回测起始时间
    pub start_time: u64,
    /// 回测结束时间
    pub end_time: u64,
    /// 回测天数
    pub backtest_days: f64,

    // === 逐笔明细 ===
    pub trades: Vec<TradeRecord>,
}

impl BacktestReport {
    /// 从交易记录列表生成报告
    pub fn generate(
        trades: Vec<TradeRecord>,
        initial_capital: f64,
        start_time: u64,
        end_time: u64,
    ) -> Self {
        let total_trades = trades.len();
        let backtest_days = (end_time - start_time) as f64 / 86400000.0;

        if total_trades == 0 {
            return Self {
                initial_capital,
                final_capital: initial_capital,
                total_return_pct: 0.0,
                annual_return_pct: 0.0,
                win_rate: 0.0,
                max_drawdown_pct: 0.0,
                total_trades: 0,
                winning_trades: 0,
                losing_trades: 0,
                avg_hold_seconds: 0.0,
                sharpe_ratio: 0.0,
                profit_loss_ratio: 0.0,
                daily_return_pct: 0.0,
                max_consecutive_losses: 0,
                total_commission: 0.0,
                max_profit_pct: 0.0,
                max_loss_pct: 0.0,
                profit_factor: 0.0,
                expectancy_usdt: 0.0,
                start_time,
                end_time,
                backtest_days,
                trades,
            };
        }

        // 计算各项指标
        let winning_trades = trades.iter().filter(|t| t.pnl_pct > 0.0).count();
        let losing_trades = trades.iter().filter(|t| t.pnl_pct <= 0.0).count();
        let win_rate = winning_trades as f64 / total_trades as f64 * 100.0;

        let total_commission: f64 = trades.iter().map(|t| t.commission).sum();
        let total_pnl: f64 = trades.iter().map(|t| t.pnl_usdt).sum();
        let final_capital = initial_capital + total_pnl - total_commission;
        let total_return_pct = (final_capital - initial_capital) / initial_capital * 100.0;

        // 年化收益率
        let annual_return_pct = if backtest_days > 0.0 {
            ((final_capital / initial_capital).powf(365.0 / backtest_days) - 1.0) * 100.0
        } else {
            0.0
        };

        // 日均收益率
        let daily_return_pct = if backtest_days > 0.0 {
            total_return_pct / backtest_days
        } else {
            0.0
        };

        // 平均持仓时间
        let avg_hold_seconds =
            trades.iter().map(|t| t.hold_seconds as f64).sum::<f64>() / total_trades as f64;

        // 最大回撤
        let max_drawdown_pct = Self::calc_max_drawdown(&trades, initial_capital);

        // 盈亏比
        let avg_win: f64 = {
            let wins: Vec<f64> = trades
                .iter()
                .filter(|t| t.pnl_pct > 0.0)
                .map(|t| t.pnl_pct)
                .collect();
            if wins.is_empty() {
                0.0
            } else {
                wins.iter().sum::<f64>() / wins.len() as f64
            }
        };
        let avg_loss: f64 = {
            let losses: Vec<f64> = trades
                .iter()
                .filter(|t| t.pnl_pct < 0.0)
                .map(|t| t.pnl_pct.abs())
                .collect();
            if losses.is_empty() {
                0.0
            } else {
                losses.iter().sum::<f64>() / losses.len() as f64
            }
        };
        let profit_loss_ratio = if avg_loss > 0.0 {
            avg_win / avg_loss
        } else {
            0.0
        };

        // 夏普比率（日收益的标准差）
        let sharpe_ratio = Self::calc_sharpe(&trades, backtest_days);

        // 最大连续亏损
        let max_consecutive_losses = Self::calc_max_consecutive_losses(&trades);

        // 最大单笔
        let max_profit_pct = trades
            .iter()
            .map(|t| t.pnl_pct)
            .fold(f64::NEG_INFINITY, f64::max);
        let max_loss_pct = trades
            .iter()
            .map(|t| t.pnl_pct)
            .fold(f64::INFINITY, f64::min);

        // 盈利因子与单笔期望值
        let gross_profit: f64 = trades
            .iter()
            .filter(|t| t.pnl_usdt > 0.0)
            .map(|t| t.pnl_usdt)
            .sum();
        let gross_loss: f64 = trades
            .iter()
            .filter(|t| t.pnl_usdt < 0.0)
            .map(|t| t.pnl_usdt.abs())
            .sum();
        let profit_factor = if gross_loss > 0.0 {
            gross_profit / gross_loss
        } else if gross_profit > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };
        let expectancy_usdt = (total_pnl - total_commission) / total_trades as f64;

        Self {
            initial_capital,
            final_capital,
            total_return_pct,
            annual_return_pct,
            win_rate,
            max_drawdown_pct,
            total_trades,
            winning_trades,
            losing_trades,
            avg_hold_seconds,
            sharpe_ratio,
            profit_loss_ratio,
            daily_return_pct,
            max_consecutive_losses,
            total_commission,
            max_profit_pct,
            max_loss_pct,
            profit_factor,
            expectancy_usdt,
            start_time,
            end_time,
            backtest_days,
            trades,
        }
    }

    /// 计算最大回撤
    fn calc_max_drawdown(trades: &[TradeRecord], initial_capital: f64) -> f64 {
        let mut equity = initial_capital;
        let mut peak = initial_capital;
        let mut max_dd = 0.0_f64;

        for t in trades {
            equity += t.pnl_usdt - t.commission;
            if equity > peak {
                peak = equity;
            }
            let dd = (peak - equity) / peak * 100.0;
            max_dd = max_dd.max(dd);
        }

        max_dd
    }

    /// 计算夏普比率（简化版：日收益率标准差）
    fn calc_sharpe(trades: &[TradeRecord], backtest_days: f64) -> f64 {
        if trades.is_empty() || backtest_days <= 1.0 {
            return 0.0;
        }

        let returns: Vec<f64> = trades.iter().map(|t| t.pnl_pct / 100.0).collect();
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance =
            returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / returns.len() as f64;
        let std_dev = variance.sqrt();

        if std_dev == 0.0 {
            return 0.0;
        }

        // 年化：假设每天N笔交易
        let trades_per_day = trades.len() as f64 / backtest_days;
        let annual_factor = (trades_per_day * 252.0).sqrt();

        (mean / std_dev) * annual_factor
    }

    /// 计算最大连续亏损次数
    fn calc_max_consecutive_losses(trades: &[TradeRecord]) -> usize {
        let mut max_streak = 0;
        let mut current_streak = 0;

        for t in trades {
            if t.pnl_pct <= 0.0 {
                current_streak += 1;
                max_streak = max_streak.max(current_streak);
            } else {
                current_streak = 0;
            }
        }

        max_streak
    }

    /// 打印报告到控制台
    pub fn print_summary(&self) {
        println!("\n{}", "=".repeat(60));
        println!("               回测报告");
        println!("{}", "=".repeat(60));

        println!("\n📊 基础指标:");
        println!("   初始资金:       {:.2} USDT", self.initial_capital);
        println!("   最终资金:       {:.2} USDT", self.final_capital);
        println!("   总收益率:       {:.2}%", self.total_return_pct);
        println!("   年化收益率:     {:.2}%", self.annual_return_pct);
        println!(
            "   胜率:           {:.1}% ({}/{})",
            self.win_rate, self.winning_trades, self.total_trades
        );
        println!("   最大回撤:       {:.2}%", self.max_drawdown_pct);
        println!("   总交易次数:     {}", self.total_trades);
        println!("   平均持仓:       {:.0}秒", self.avg_hold_seconds);

        println!("\n📈 详细指标:");
        println!("   夏普比率:       {:.2}", self.sharpe_ratio);
        println!("   盈亏比:         {:.2}", self.profit_loss_ratio);
        println!("   日均收益率:     {:.4}%", self.daily_return_pct);
        println!("   最大连续亏损:   {}次", self.max_consecutive_losses);
        println!("   累计手续费:     {:.4} USDT", self.total_commission);
        println!("   最大单笔盈利:   {:.3}%", self.max_profit_pct);
        println!("   最大单笔亏损:   {:.3}%", self.max_loss_pct);
        if self.profit_factor.is_infinite() {
            println!("   盈利因子:       ∞ (无亏损单)");
        } else {
            println!("   盈利因子:       {:.2}", self.profit_factor);
        }
        println!("   单笔期望值:     {:.4} USDT", self.expectancy_usdt);

        println!("\n📅 时间范围:");
        println!("   回测天数:       {:.1}天", self.backtest_days);

        // 样本量统计意义检查：小样本指标不可信
        if self.total_trades > 0 && self.total_trades < 30 {
            println!("\n⚠️  样本警告:");
            println!(
                "   仅 {} 笔交易（<30笔），胜率/盈亏比/期望值等指标统计意义有限，",
                self.total_trades
            );
            println!("   结论可能因个别交易而大幅波动，请谨慎据此调参！");
        }

        // 逐笔明细（显示前20笔+后5笔）
        if !self.trades.is_empty() {
            println!("\n📝 交易明细 (共{}笔):", self.trades.len());
            println!(
                "   {:<4} {:<12} {:<12} {:<10} {:<10} {:<8} {:<8}",
                "#", "入场价", "出场价", "盈亏%", "盈亏USDT", "持仓秒", "原因"
            );
            println!("   {}", "-".repeat(70));

            let show_count = self.trades.len().min(20);
            for t in &self.trades[..show_count] {
                println!(
                    "   {:<4} {:<12.2} {:<12.2} {:<10.3} {:<10.4} {:<8} {:<8}",
                    t.id,
                    t.entry_price,
                    t.exit_price,
                    t.pnl_pct,
                    t.pnl_usdt,
                    t.hold_seconds,
                    t.exit_reason
                );
            }

            if self.trades.len() > 25 {
                let omitted = self.trades.len().saturating_sub(25);
                println!("   ... 省略 {} 笔 ...", omitted);
                for t in &self.trades[self.trades.len() - 5..] {
                    println!(
                        "   {:<4} {:<12.2} {:<12.2} {:<10.3} {:<10.4} {:<8} {:<8}",
                        t.id,
                        t.entry_price,
                        t.exit_price,
                        t.pnl_pct,
                        t.pnl_usdt,
                        t.hold_seconds,
                        t.exit_reason
                    );
                }
            }
        }

        println!("\n{}", "=".repeat(60));
    }
}
