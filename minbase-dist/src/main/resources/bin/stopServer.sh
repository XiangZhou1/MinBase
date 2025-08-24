#!/bin/bash

# =================================================================
# MinBase Server Shutdown Script for Unix-like systems
# =================================================================

BASE_DIR=$(cd "$(dirname "$0")"; pwd)/..
PID_FILE=$BASE_DIR/minbase.pid

echo "================================================================="
echo " Stopping MinBase Server..."
echo "================================================================="

# 检查 PID 文件是否存在
if [ ! -f "$PID_FILE" ]; then
    echo "PID file not found at $PID_FILE. Is the server running?"
    exit 1
fi

# 从文件中读取 PID
PID=$(cat "$PID_FILE")

# 检查进程是否存在
if ! ps -p $PID > /dev/null; then
    echo "Process with PID $PID not found. Removing stale PID file."
    rm -f "$PID_FILE"
    exit 1
fi

# 尝试优雅地关闭 (发送 SIGTERM 信号)
echo "Sending shutdown signal to process $PID..."
kill $PID

# 等待进程关闭, 最多等待 15 秒
echo -n "Waiting for process to terminate."
COUNT=0
while ps -p $PID > /dev/null; do
    if [ "$COUNT" -lt 15 ]; then
        echo -n "."
        sleep 1
        ((COUNT++))
    else
        echo " Timeout!"
        break
    fi
done

# 再次检查进程是否还存在
if ps -p $PID > /dev/null; then
    echo "Process did not terminate gracefully. Forcing shutdown (kill -9)..."
    kill -9 $PID
    sleep 1
    if ps -p $PID > /dev/null; then
      echo "Failed to kill process $PID even with -9. Please check manually."
    else
      echo "Process killed."
      rm -f "$PID_FILE"
    fi
else
    echo " Process stopped successfully."
    rm -f "$PID_FILE"
fi

echo "================================================================="