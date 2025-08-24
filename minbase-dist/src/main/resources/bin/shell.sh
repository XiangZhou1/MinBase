#!/bin/bash

# =================================================================
# MinBase Command Line Interface Startup Script for Unix-like systems
# =================================================================

# 获取脚本所在的目录的绝对路径
# 这段代码确保无论从哪里调用此脚本, 都能正确找到相对路径
BASE_DIR=$(cd "$(dirname "$0")"; pwd)/..

# 设置配置文件目录
CONF_DIR=$BASE_DIR/conf

# 设置库目录
LIB_DIR=$BASE_DIR/lib

# 初始化 CLASSPATH, 首先包含配置文件目录
# Unix/Linux/macOS 下的路径分隔符是冒号 (:)
CLASSPATH="$CONF_DIR"

# 遍历 lib 目录下的所有 .jar 文件, 并将它们追加到 CLASSPATH 中
for jar in "$LIB_DIR"/*.jar; do
  CLASSPATH="$CLASSPATH":"$jar"
done

# 设置 JVM 选项。用户可以通过设置环境变量 JAVA_OPTS 来覆盖默认值
# 格式: 如果 JAVA_OPTS 未设置或为空, 则使用 :- 后面的默认值
JAVA_OPTS="${JAVA_OPTS:--Xms256m -Xmx512m}"

# 设置主启动类
MAIN_CLASS="org.minbase.client.shell.MinBaseShell" # 如果你的类有包名, 请写全, 例如: org.minbase.cli.MinbaseCli

# 打印一些调试信息
echo "================================================================="
echo " Starting MinBase CLI"
echo " BASE_DIR:    $BASE_DIR"
echo " JAVA_OPTS:   $JAVA_OPTS"
# echo " CLASSPATH:   $CLASSPATH" # Classpath 很长, 默认注释掉
echo "================================================================="
echo

# 执行 Java 命令
# (虽然这个交互式CLI目前用不到, 但这是个好习惯)
java $JAVA_OPTS -cp "$CLASSPATH" $MAIN_CLASS ${CONF_DIR}/minbase.conf