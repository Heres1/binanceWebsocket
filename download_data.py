#!/usr/bin/env python3
"""通过本地代理下载Binance K线数据，输出为回测引擎格式"""
import json, time, sys, os, urllib.request

PROXY = "http://127.0.0.1:6384"
DATA_DIR = "data/history"
os.makedirs(DATA_DIR, exist_ok=True)

# 设置代理
proxy_handler = urllib.request.ProxyHandler({'https': PROXY, 'http': PROXY})
opener = urllib.request.build_opener(proxy_handler)

def download_klines(symbol, interval, days):
    output = f"{DATA_DIR}/{symbol}_{interval}.json"
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - days * 86400000
    current = start_ms
    all_klines = []
    
    print(f"📥 下载 {symbol} {interval} K线 ({days}天)...")
    
    while current < now_ms:
        url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit=1000&startTime={current}&endTime={now_ms}"
        try:
            req = urllib.request.Request(url)
            resp = opener.open(req, timeout=30)
            data = json.loads(resp.read().decode())
        except Exception as e:
            print(f"   ❌ 请求失败: {e}")
            break
        
        if not data:
            break
        
        for k in data:
            all_klines.append({
                "symbol": symbol,
                "interval": interval,
                "open_time": k[0],
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
                "close_time": k[6],
                "trades_count": k[8],
                "taker_buy_volume": float(k[9])
            })
        
        print(f"   已下载 {len(data)} 根 (总计: {len(all_klines)})")
        
        # 下一批从最后一根的close_time+1开始
        current = data[-1][6] + 1
        
        if len(data) < 1000:
            break
        
        time.sleep(0.2)  # 防限频
    
    with open(output, 'w') as f:
        json.dump(all_klines, f)
    
    print(f"✅ 保存: {output} ({len(all_klines)} 根)")
    return len(all_klines)

if __name__ == "__main__":
    symbols = sys.argv[1:] if len(sys.argv) > 1 else ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    days = 60  # 60天含牛熊转换期
    
    for symbol in symbols:
        download_klines(symbol, "1m", days)
        download_klines(symbol, "5m", days)
        print()
