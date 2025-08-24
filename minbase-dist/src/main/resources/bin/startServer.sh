#!/bin/bash

# =================================================================
# MinBase Server Startup Script for Unix-like systems
# =================================================================

# ... (BASE_DIR, CONF_DIR, LIB_DIR, CLASSPATH 的定义和之前一样) ...
BASE_DIR=$(cd "$(dirname "$0")"; pwd)/..
CONF_DIR=$BASE_DIR/conf
LIB_DIR=$BASE_DIR/lib
CLASSPATH="$CONF_DIR"
for jar in "$LIB_DIR"/*.jar; do
  CLASSPATH="$CLASSPATH":"$jar"
done

# === 新增部分开始 ===

# 定义 PID 文件的位置
PID_FILE=$BASE_DIR/minbase.pid

# 检查 PID 文件是否存在, 如果存在则可能服务已在运行
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    # 检查该 PID 是否真的在运行
    if ps -p $PID > /dev/null; then
        echo "MinBase Server is already running with PID: $PID"
        exit 1
    else
        # PID 文件存在但进程已死, 清理掉这个无用的 PID 文件
        echo "Warning: Removing stale PID file."
        rm -f "$PID_FILE"
    fi
fi

# === 新增部分结束 ===

JAVA_OPTS="${JAVA_OPTS:--Xms512m -Xmx1024m}"
# 你的 minbase-server 模块的主类 (请替换成你自己的)
MAIN_CLASS="org.minbase.server.MinBaseServer"

echo "================================================================="
echo " Starting MinBase Server..."
echo " BASE_DIR:    $BASE_DIR"
echo " JAVA_OPTS:   $JAVA_OPTS"
echo " MAIN_CLASS:  $MAIN_CLASS"
echo "================================================================="

# === 修改部分开始 ===

# 使用 nohup 在后台运行 Java 程序, 并将输出重定向到 logs/server.log
# & 符号让命令在后台执行
mkdir -p "$BASE_DIR/logs"
nohup java $JAVA_OPTS -cp "$CLASSPATH" $MAIN_CLASS  ${CONF_DIR}/minbase.conf > "$BASE_DIR/logs/server.log" 2>&1 &

# 获取后台进程的 PID
PID=$!

# 将 PID 写入文件
echo $PID > "$PID_FILE"

echo "MinBase Server started successfully with PID: $PID"
echo "Logs are being written to: $BASE_DIR/logs/server.log"

# === 修改部分结束 ===