#!/bin/bash
# 通过本地代理下载Binance K线数据
PROXY="http://127.0.0.1:6384"
DATA_DIR="data/history"
mkdir -p "$DATA_DIR"

download_klines() {
    local SYMBOL=$1
    local INTERVAL=$2
    local DAYS=$3
    local OUTPUT="$DATA_DIR/${SYMBOL}_${INTERVAL}.json"
    
    local NOW_MS=$(python3 -c "import time; print(int(time.time()*1000))")
    local START_MS=$((NOW_MS - DAYS * 86400000))
    local CURRENT=$START_MS
    local ALL_DATA="["
    local FIRST=true
    local TOTAL=0
    
    echo "📥 下载 $SYMBOL $INTERVAL K线 (${DAYS}天)..."
    
    while [ $CURRENT -lt $NOW_MS ]; do
        local RESP=$(curl -x "$PROXY" -s "https://api.binance.com/api/v3/klines?symbol=${SYMBOL}&interval=${INTERVAL}&limit=1000&startTime=${CURRENT}&endTime=${NOW_MS}")
        
        # 检查是否为空数组
        if [ "$RESP" = "[]" ] || [ -z "$RESP" ]; then
            break
        fi
        
        # 转换每条kline为BacktestKline格式
        local CONVERTED=$(python3 -c "
import json, sys
data = json.loads('''$RESP''')
if not data:
    sys.exit(0)
results = []
for k in data:
    results.append({
        'symbol': '$SYMBOL',
        'interval': '$INTERVAL',
        'open_time': k[0],
        'open': float(k[1]),
        'high': float(k[2]),
        'low': float(k[3]),
        'close': float(k[4]),
        'volume': float(k[5]),
        'close_time': k[6],
        'trades_count': k[8],
        'taker_buy_volume': float(k[9])
    })
# 输出最后一条的close_time用于分页
print(json.dumps(results))
print(k[6], file=sys.stderr)
" 2>/tmp/last_time)
        
        local LAST_TIME=$(cat /tmp/last_time 2>/dev/null)
        if [ -z "$LAST_TIME" ]; then
            break
        fi
        
        # 累加结果
        if [ "$FIRST" = true ]; then
            ALL_DATA="${CONVERTED:1:-1}"  # 去掉外层[]
            FIRST=false
        else
            ALL_DATA="${ALL_DATA},${CONVERTED:1:-1}"
        fi
        
        local BATCH_COUNT=$(python3 -c "import json; print(len(json.loads('''$RESP''')))")
        TOTAL=$((TOTAL + BATCH_COUNT))
        echo "   已下载 $BATCH_COUNT 根 (总计: $TOTAL)"
        
        # 下一批从last_time+1开始
        CURRENT=$((LAST_TIME + 1))
        
        # 防API限频
        sleep 0.3
        
        if [ "$BATCH_COUNT" -lt 1000 ]; then
            break
        fi
    done
    
    # 写入文件
    echo "[${ALL_DATA}]" > "$OUTPUT"
    echo "✅ 保存: $OUTPUT ($TOTAL 根)"
}

# 下载ETH和SOL的1m和5m数据
download_klines "ETHUSDT" "1m" 30
download_klines "ETHUSDT" "5m" 30
download_klines "SOLUSDT" "1m" 30
download_klines "SOLUSDT" "5m" 30
