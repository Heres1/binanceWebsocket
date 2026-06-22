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

/// 平均真实波动幅度 (ATR) - 基于 Wilder 平滑
#[derive(Debug, Clone)]
pub struct ATR {
    period: usize,
    prev_close: Option<f64>,
    value: Option<f64>,
    count: usize,
    tr_sum: f64,        // 预热期累加
    history: Vec<f64>,  // ATR历史值缓冲区（用于百分位计算）
    history_cap: usize, // 历史缓冲区容量
}

impl ATR {
    /// 创建新的 ATR 计算器
    /// period: 平滑周期 (e.g. 14)
    /// history_cap: 历史缓冲区大小，用于百分位计算 (e.g. 100)
    pub fn new(period: usize, history_cap: usize) -> Self {
        Self {
            period,
            prev_close: None,
            value: None,
            count: 0,
            tr_sum: 0.0,
            history: Vec::with_capacity(history_cap),
            history_cap,
        }
    }

    /// 输入新的 (high, low, close)，返回当前 ATR 值
    pub fn update(&mut self, high: f64, low: f64, close: f64) -> f64 {
        let tr = if let Some(prev_c) = self.prev_close {
            // True Range = max(H-L, |H-PrevClose|, |L-PrevClose|)
            (high - low)
                .max((high - prev_c).abs())
                .max((low - prev_c).abs())
        } else {
            high - low // 第一根K线，TR = H-L
        };
        self.prev_close = Some(close);
        self.count += 1;

        match self.value {
            None => {
                // 预热期：累加TR
                self.tr_sum += tr;
                if self.count >= self.period {
                    let atr = self.tr_sum / self.period as f64;
                    self.value = Some(atr);
                    self.push_history(atr);
                    atr
                } else {
                    tr // 返回当前TR作为估计
                }
            }
            Some(prev_atr) => {
                // Wilder 平滑: ATR = (prevATR * (period-1) + TR) / period
                let atr = (prev_atr * (self.period as f64 - 1.0) + tr) / self.period as f64;
                self.value = Some(atr);
                self.push_history(atr);
                atr
            }
        }
    }

    fn push_history(&mut self, atr: f64) {
        self.history.push(atr);
        if self.history.len() > self.history_cap {
            self.history.remove(0);
        }
    }

    /// 获取当前 ATR 值
    pub fn value(&self) -> Option<f64> {
        self.value
    }

    /// 是否已完成预热
    pub fn is_ready(&self) -> bool {
        self.value.is_some()
    }

    /// 计算当前ATR在历史中的百分位 (0-100)
    /// 返回 None 如果历史数据不足
    pub fn percentile(&self) -> Option<f64> {
        if self.history.len() < 20 {
            return None;
        }
        let current = self.value?;
        let count_below = self.history.iter().filter(|&&v| v < current).count();
        Some(count_below as f64 / self.history.len() as f64 * 100.0)
    }
}

/// 平均方向指数 (ADX) - Wilder 方法
#[derive(Debug, Clone)]
pub struct ADX {
    period: usize,
    prev_high: Option<f64>,
    prev_low: Option<f64>,
    prev_close: Option<f64>,
    // 平滑的+DM, -DM, TR
    smoothed_plus_dm: f64,
    smoothed_minus_dm: f64,
    smoothed_tr: f64,
    // ADX
    dx_sum: f64,
    dx_count: usize,
    adx_value: Option<f64>,
    plus_di: f64,
    minus_di: f64,
    count: usize,
}

impl ADX {
    /// 创建新的 ADX 计算器
    pub fn new(period: usize) -> Self {
        Self {
            period,
            prev_high: None,
            prev_low: None,
            prev_close: None,
            smoothed_plus_dm: 0.0,
            smoothed_minus_dm: 0.0,
            smoothed_tr: 0.0,
            dx_sum: 0.0,
            dx_count: 0,
            adx_value: None,
            plus_di: 0.0,
            minus_di: 0.0,
            count: 0,
        }
    }

    /// 输入新的 (high, low, close)，返回当前 ADX 值
    pub fn update(&mut self, high: f64, low: f64, close: f64) -> f64 {
        if let (Some(prev_h), Some(prev_l), Some(prev_c)) = (self.prev_high, self.prev_low, self.prev_close) {
            self.count += 1;

            // 计算+DM和-DM
            let up_move = high - prev_h;
            let down_move = prev_l - low;

            let plus_dm = if up_move > down_move && up_move > 0.0 { up_move } else { 0.0 };
            let minus_dm = if down_move > up_move && down_move > 0.0 { down_move } else { 0.0 };

            // True Range
            let tr = (high - low)
                .max((high - prev_c).abs())
                .max((low - prev_c).abs());

            if self.count <= self.period {
                // 预热期：累加
                self.smoothed_plus_dm += plus_dm;
                self.smoothed_minus_dm += minus_dm;
                self.smoothed_tr += tr;

                if self.count == self.period {
                    // 第一次计算DI
                    if self.smoothed_tr > 0.0 {
                        self.plus_di = self.smoothed_plus_dm / self.smoothed_tr * 100.0;
                        self.minus_di = self.smoothed_minus_dm / self.smoothed_tr * 100.0;
                    }
                    let dx = self.compute_dx();
                    self.dx_sum += dx;
                    self.dx_count += 1;
                }
            } else {
                // Wilder 平滑
                let n = self.period as f64;
                self.smoothed_plus_dm = self.smoothed_plus_dm - self.smoothed_plus_dm / n + plus_dm;
                self.smoothed_minus_dm = self.smoothed_minus_dm - self.smoothed_minus_dm / n + minus_dm;
                self.smoothed_tr = self.smoothed_tr - self.smoothed_tr / n + tr;

                if self.smoothed_tr > 0.0 {
                    self.plus_di = self.smoothed_plus_dm / self.smoothed_tr * 100.0;
                    self.minus_di = self.smoothed_minus_dm / self.smoothed_tr * 100.0;
                }

                let dx = self.compute_dx();

                if self.adx_value.is_none() {
                    // 第二次预热：累加DX直到够 period 个
                    self.dx_sum += dx;
                    self.dx_count += 1;
                    if self.dx_count >= self.period {
                        let adx = self.dx_sum / self.period as f64;
                        self.adx_value = Some(adx);
                    }
                } else {
                    // Wilder 平滑 ADX
                    let prev_adx = self.adx_value.unwrap();
                    let adx = (prev_adx * (self.period as f64 - 1.0) + dx) / self.period as f64;
                    self.adx_value = Some(adx);
                }
            }
        }

        self.prev_high = Some(high);
        self.prev_low = Some(low);
        self.prev_close = Some(close);

        self.adx_value.unwrap_or(0.0)
    }

    fn compute_dx(&self) -> f64 {
        let di_sum = self.plus_di + self.minus_di;
        if di_sum == 0.0 {
            0.0
        } else {
            (self.plus_di - self.minus_di).abs() / di_sum * 100.0
        }
    }

    /// 获取当前 ADX 值
    pub fn value(&self) -> Option<f64> {
        self.adx_value
    }

    /// 获取 +DI 值
    pub fn plus_di(&self) -> f64 {
        self.plus_di
    }

    /// 获取 -DI 值
    pub fn minus_di(&self) -> f64 {
        self.minus_di
    }

    /// 是否已完成预热
    pub fn is_ready(&self) -> bool {
        self.adx_value.is_some()
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

    #[test]
    fn test_atr_basic() {
        let mut atr = ATR::new(3, 100);
        // 3根K线预热
        atr.update(110.0, 90.0, 100.0);  // TR = 20 (第一根, H-L)
        atr.update(115.0, 95.0, 110.0);  // TR = max(20, |115-100|, |95-100|) = 20
        let v = atr.update(120.0, 100.0, 115.0); // TR = max(20, |120-110|, |100-110|) = 20
        // ATR = (20+20+20) / 3 = 20.0
        assert!((v - 20.0).abs() < 0.01, "ATR should be 20.0, got {}", v);
        assert!(atr.is_ready());

        // Wilder平滑: ATR = (20*2 + 15) / 3 = 18.33
        let v2 = atr.update(125.0, 110.0, 120.0); // TR = max(15, |125-115|, |110-115|) = 15
        assert!((v2 - 18.33).abs() < 0.1, "ATR should be ~18.33, got {}", v2);
    }

    #[test]
    fn test_atr_percentile() {
        let mut atr = ATR::new(3, 100);
        // 填充足够历史
        for i in 0..30 {
            let h = 100.0 + (i as f64) * 0.5;
            let l = 100.0 - (i as f64) * 0.5;
            atr.update(h, l, 100.0);
        }
        // 应该有百分位值
        assert!(atr.percentile().is_some());
        let pct = atr.percentile().unwrap();
        assert!(pct >= 0.0 && pct <= 100.0);
    }

    #[test]
    fn test_adx_trending_market() {
        let mut adx = ADX::new(5);
        // 模拟强上升趋势
        let mut price = 100.0;
        let mut last_adx = 0.0;
        for _ in 0..40 {
            price += 2.0; // 持续上涨
            let h = price + 1.0;
            let l = price - 1.0;
            last_adx = adx.update(h, l, price);
        }
        // 强趋势下 ADX 应该 > 25
        assert!(last_adx > 25.0, "ADX should be >25 for strong trend, got {}", last_adx);
        assert!(adx.plus_di() > adx.minus_di(), "+DI should be > -DI in uptrend");
    }

    #[test]
    fn test_adx_ranging_market() {
        let mut adx = ADX::new(5);
        // 模拟震荡市（价格来回摆动）
        for i in 0..40 {
            let offset = if i % 2 == 0 { 2.0 } else { -2.0 };
            let price = 100.0 + offset;
            let h = price + 1.0;
            let l = price - 1.0;
            adx.update(h, l, price);
        }
        // 震荡市 ADX 应该较低
        if let Some(val) = adx.value() {
            assert!(val < 35.0, "ADX should be low for ranging market, got {}", val);
        }
    }
}
