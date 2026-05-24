//! 技术指标计算模块
//!
//! 实现 EMA、RSI、VolumeRatio 等流式指标计算器

use std::collections::VecDeque;

/// 指数移动平均线 (EMA)
#[derive(Debug, Clone)]
pub struct EMA {
    period: usize,
    k: f64,
    value: Option<f64>,
    count: usize,
    sum: f64,
}

impl EMA {
    /// 创建新的 EMA 计算器
    pub fn new(period: usize) -> Self {
        let k = 2.0 / (period as f64 + 1.0);
        Self {
            period,
            k,
            value: None,
            count: 0,
            sum: 0.0,
        }
    }

    /// 输入新价格，返回当前 EMA 值
    pub fn update(&mut self, price: f64) -> f64 {
        self.count += 1;

        match self.value {
            None => {
                // 预热阶段：累加求平均
                self.sum += price;
                if self.count >= self.period {
                    let sma = self.sum / self.period as f64;
                    self.value = Some(sma);
                    sma
                } else {
                    price
                }
            }
            Some(prev_ema) => {
                let new_ema = price * self.k + prev_ema * (1.0 - self.k);
                self.value = Some(new_ema);
                new_ema
            }
        }
    }

    /// 获取当前 EMA 值
    pub fn value(&self) -> Option<f64> {
        self.value
    }

    /// 是否已完成预热
    pub fn is_ready(&self) -> bool {
        self.value.is_some()
    }
}

/// 相对强弱指数 (RSI)
#[derive(Debug, Clone)]
pub struct RSI {
    period: usize,
    avg_gain: f64,
    avg_loss: f64,
    prev_price: Option<f64>,
    count: usize,
    value: Option<f64>,
    gains: Vec<f64>,
    losses: Vec<f64>,
}

impl RSI {
    /// 创建新的 RSI 计算器
    pub fn new(period: usize) -> Self {
        Self {
            period,
            avg_gain: 0.0,
            avg_loss: 0.0,
            prev_price: None,
            count: 0,
            value: None,
            gains: Vec::with_capacity(period),
            losses: Vec::with_capacity(period),
        }
    }

    /// 输入新价格，返回当前 RSI 值
    pub fn update(&mut self, price: f64) -> f64 {
        if let Some(prev) = self.prev_price {
            let change = price - prev;
            let gain = if change > 0.0 { change } else { 0.0 };
            let loss = if change < 0.0 { -change } else { 0.0 };

            self.count += 1;

            if self.count <= self.period {
                // 预热阶段：收集数据
                self.gains.push(gain);
                self.losses.push(loss);

                if self.count == self.period {
                    // 第一次计算: 简单平均
                    self.avg_gain = self.gains.iter().sum::<f64>() / self.period as f64;
                    self.avg_loss = self.losses.iter().sum::<f64>() / self.period as f64;
                    let rsi = self.calculate_rsi();
                    self.value = Some(rsi);
                }
            } else {
                // 平滑计算
                self.avg_gain = (self.avg_gain * (self.period as f64 - 1.0) + gain) / self.period as f64;
                self.avg_loss = (self.avg_loss * (self.period as f64 - 1.0) + loss) / self.period as f64;
                let rsi = self.calculate_rsi();
                self.value = Some(rsi);
            }
        }

        self.prev_price = Some(price);
        self.value.unwrap_or(50.0)
    }

    fn calculate_rsi(&self) -> f64 {
        if self.avg_loss == 0.0 {
            return 100.0;
        }
        let rs = self.avg_gain / self.avg_loss;
        100.0 - (100.0 / (1.0 + rs))
    }

    /// 获取当前 RSI 值
    pub fn value(&self) -> Option<f64> {
        self.value
    }

    /// 是否已完成预热
    pub fn is_ready(&self) -> bool {
        self.value.is_some()
    }
}

/// 买卖量比率计算器（滑动窗口）
#[derive(Debug, Clone)]
pub struct VolumeRatio {
    window_ms: u64,
    trades: VecDeque<(u64, f64, bool)>, // (timestamp_ms, quantity, is_buyer)
}

impl VolumeRatio {
    /// 创建新的成交量比率计算器
    /// window_secs: 滑动窗口秒数
    pub fn new(window_secs: u64) -> Self {
        Self {
            window_ms: window_secs * 1000,
            trades: VecDeque::new(),
        }
    }

    /// 添加一笔成交
    /// is_buyer_maker=true 表示卖方主动(买方挂单)，即卖单成交
    /// is_buyer_maker=false 表示买方主动(卖方挂单)，即买单成交
    pub fn add_trade(&mut self, timestamp_ms: u64, quantity: f64, is_buyer_maker: bool) -> f64 {
        // is_buyer_maker=false 意味着买方是taker（主动买入）
        let is_buyer = !is_buyer_maker;
        self.trades.push_back((timestamp_ms, quantity, is_buyer));

        // 移除窗口外的旧数据
        let cutoff = timestamp_ms.saturating_sub(self.window_ms);
        while let Some(&(ts, _, _)) = self.trades.front() {
            if ts < cutoff {
                self.trades.pop_front();
            } else {
                break;
            }
        }

        self.ratio()
    }

    /// 获取当前买/卖量比率
    /// >1.0 表示买方主导, <1.0 表示卖方主导
    pub fn ratio(&self) -> f64 {
        let mut buy_vol = 0.0;
        let mut sell_vol = 0.0;

        for &(_, qty, is_buyer) in &self.trades {
            if is_buyer {
                buy_vol += qty;
            } else {
                sell_vol += qty;
            }
        }

        if sell_vol == 0.0 {
            if buy_vol > 0.0 { 10.0 } else { 1.0 }
        } else {
            buy_vol / sell_vol
        }
    }

    /// 窗口内总成交笔数
    pub fn trade_count(&self) -> usize {
        self.trades.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ema_basic() {
        let mut ema = EMA::new(3);
        // 预热期
        ema.update(10.0);
        ema.update(11.0);
        let v = ema.update(12.0); // SMA = (10+11+12)/3 = 11.0
        assert!((v - 11.0).abs() < 0.01);
        assert!(ema.is_ready());

        // EMA计算: k=0.5, new = 13*0.5 + 11*0.5 = 12.0
        let v2 = ema.update(13.0);
        assert!((v2 - 12.0).abs() < 0.01);
    }

    #[test]
    fn test_rsi_basic() {
        let mut rsi = RSI::new(5);
        // 上涨序列
        let prices = vec![44.0, 44.5, 45.0, 45.5, 46.0, 46.5];
        let mut last_val = 50.0;
        for p in prices {
            last_val = rsi.update(p);
        }
        // 持续上涨, RSI应该>70
        assert!(last_val > 70.0, "RSI should be >70 for uptrend, got {}", last_val);
    }

    #[test]
    fn test_volume_ratio() {
        let mut vr = VolumeRatio::new(60);
        // 买方主导
        vr.add_trade(1000, 5.0, false); // 买方主动
        vr.add_trade(2000, 5.0, false); // 买方主动
        vr.add_trade(3000, 2.0, true);  // 卖方主动
        let ratio = vr.ratio();
        assert!((ratio - 5.0).abs() < 0.01); // 10/2 = 5.0

        // 过期测试: 窗口60s=60000ms, 新trade在ts=65000
        // cutoff = 65000 - 60000 = 5000, 所以ts=1000,2000,3000都被清除
        vr.add_trade(65000, 1.0, true); // 超出窗口
        // 前面的数据应该被清除，只剩当前这笔
        assert_eq!(vr.trade_count(), 1);
    }
}
