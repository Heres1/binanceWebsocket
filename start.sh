#!/bin/bash
# 动量短线交易系统 - 启动脚本

set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

PID_FILE="$PROJECT_DIR/.trading.pid"
LOG_DIR="$PROJECT_DIR/logs"

# 检查是否已在运行
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "交易系统已在运行 (PID: $OLD_PID)"
        exit 1
    else
        rm -f "$PID_FILE"
    fi
fi

# 确保日志目录存在
mkdir -p "$LOG_DIR"

# 编译 release 版本
echo "编译中..."
cargo build --bin trading --release 2>&1 | tail -3

if [ $? -ne 0 ]; then
    echo "编译失败"
    exit 1
fi

# 后台启动交易系统
echo "启动交易系统..."
nohup ./target/release/trading > "$LOG_DIR/stdout.log" 2>&1 &
PID=$!

echo "$PID" > "$PID_FILE"
sleep 1

# 验证进程是否存活
if kill -0 "$PID" 2>/dev/null; then
    echo "交易系统已启动 (PID: $PID)"
    echo "日志: $LOG_DIR/stdout.log"
    echo "停止: ./stop.sh"
else
    echo "启动失败，查看日志: $LOG_DIR/stdout.log"
    rm -f "$PID_FILE"
    exit 1
fi
