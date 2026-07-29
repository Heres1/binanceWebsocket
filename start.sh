#!/bin/bash
# 动量短线交易系统 - 启动脚本

set -e
set -o pipefail

# 非交互式 SSH 部署时 PATH 可能不含 cargo，显式加载
if [ -f "$HOME/.cargo/env" ]; then
    . "$HOME/.cargo/env"
fi

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

# 编译 release 版本（pipefail 保证 cargo 失败不会被 tail 掩盖，防止带着旧二进制启动）
echo "编译中..."
if ! cargo build --bin trading --release 2>&1 | tail -3; then
    echo "编译失败，已中止启动"
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
