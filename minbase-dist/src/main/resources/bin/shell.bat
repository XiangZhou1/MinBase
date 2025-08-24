@echo off
rem =================================================================
rem MinBase Command Line Interface Startup Script for Windows
rem =================================================================

rem 启用延迟环境变量扩展, 这是在 FOR 循环中正确构建 CLASSPATH 的关键
setlocal enabledelayedexpansion

rem 获取批处理文件所在的目录, 并通过 '..' 上溯到项目根目录
rem %~dp0 会扩展为驱动器号和路径, 例如: C:\path\to\project\bin\
set "BASE_DIR=%~dp0.."

rem 设置配置文件目录
set "CONF_DIR=%BASE_DIR%\conf"

rem 设置库目录
set "LIB_DIR=%BASE_DIR%\lib"

rem 初始化 CLASSPATH, 首先包含配置文件目录
rem Windows 下的路径分隔符是分号 (;)
set "CLASSPATH=%CONF_DIR%"

rem 遍历 lib 目录下的所有 .jar 文件, 并将它们追加到 CLASSPATH 中
for %%i in ("%LIB_DIR%\*.jar") do (
    set "CLASSPATH=!CLASSPATH!;%%i"
)

rem 设置 JVM 选项。用户可以通过 set JAVA_OPTS=... 来覆盖默认值
if not defined JAVA_OPTS (
    set "JAVA_OPTS=-Xms256m -Xmx512m"
)

rem 设置主启动类 (请确保这是你正确的类名)
set "MAIN_CLASS=org.minbase.client.shell.MinBaseShell"

echo.
echo =================================================================
echo  Starting MinBase CLI
echo  BASE_DIR:    %BASE_DIR%
echo  JAVA_OPTS:   %JAVA_OPTS%
rem echo  CLASSPATH:   %CLASSPATH%
echo =================================================================
echo.

rem 执行 Java 命令
java %JAVA_OPTS% -cp "%CLASSPATH%" %MAIN_CLASS% %CONF_DIR%/minbase.conf

rem 脚本结束后保持窗口打开, 便于查看错误信息
pause
endlocal