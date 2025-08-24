@echo off
REM =================================================================
REM MinBase Server Startup Script for Windows
REM =================================================================

REM 启用延迟环境变量扩展, 这是在 FOR 循环中正确构建 CLASSPATH 的关键
setlocal enabledelayedexpansion

REM 获取批处理文件所在的目录, 并通过 '..' 上溯到项目根目录
REM %~dp0 会扩展为驱动器号和路径, 例如: C:\path\to\project\bin\
set "BASE_DIR=%~dp0.."

REM 设置配置文件目录
set "CONF_DIR=%BASE_DIR%\conf"

REM 初始化 CLASSPATH, 首先包含配置文件目录
set "CLASSPATH=%CONF_DIR%"

REM 遍历 lib 目录下的所有 .jar 文件, 并将它们追加到 CLASSPATH 中
REM 注意: Windows 下的路径分隔符是分号 (;)
echo Building Classpath...
for %%i in ("%BASE_DIR%\lib\*.jar") do (
    set "CLASSPATH=!CLASSPATH!;%%i"
)

REM 设置 JVM 参数
set "JAVA_OPTS=-Xms512m -Xmx1024m"

REM 设置主启动类 (请确保这是你正确的类名)
set "MAIN_CLASS=org.minbase.server.MinBaseServer"

echo.
echo Starting MinBase Server...
echo =================================================================
echo BASE_DIR: %BASE_DIR%
echo JAVA_OPTS: %JAVA_OPTS%
REM CLASSPATH 可能会很长, 可以取消下面这行的注释来调试
REM echo CLASSPATH: %CLASSPATH%
echo MAIN_CLASS: %MAIN_CLASS%
echo =================================================================
echo.

REM 执行 Java 命令
REM -cp 是 -classpath 的缩写
java %JAVA_OPTS% -cp "%CLASSPATH%" %MAIN_CLASS% %CONF_DIR%/minbase.conf

REM 如果 Java 程序立即退出, 下面的 pause 命令可以防止窗口立即关闭, 便于查看错误信息
pause
endlocal