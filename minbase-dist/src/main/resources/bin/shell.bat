@echo off
rem =================================================================
rem MinBase Command Line Interface Startup Script for Windows (Corrected)
rem =================================================================

setlocal
rem 获取批处理文件所在的目录, 并通过 '..' 上溯到项目根目录
set "BASE_DIR=%~dp0.."

rem 设置配置文件目录
set "CONF_DIR=%BASE_DIR%\conf"

rem 设置库目录
set "LIB_DIR=%BASE_DIR%\lib"

rem --- MODIFIED SECTION ---
rem 放弃使用 for 循环拼接超长字符串.
rem 直接使用 Java 的 classpath 通配符 (*), 它可以自动加载指定目录下的所有 JAR 文件.
set "CLASSPATH=%CONF_DIR%;%LIB_DIR%\*"
rem --- END MODIFIED SECTION ---

rem 设置 JVM 选项。
if not defined JAVA_OPTS (
    set "JAVA_OPTS=-Xms256m -Xmx512m"
)

rem 设置主启动类
set "MAIN_CLASS=org.minbase.client.shell.MinBaseShell"

echo.
echo =================================================================
echo  Starting MinBase CLI
echo  BASE_DIR:    %BASE_DIR%
echo  JAVA_OPTS:   %JAVA_OPTS%
rem echo  CLASSPATH:   %CLASSPATH%
echo =================================================================
echo.

rem 执行 Java 命令, 使用修正后的、简洁的 CLASSPATH
java %JAVA_OPTS% -cp "%CLASSPATH%" %MAIN_CLASS% %CONF_DIR%/minbase.conf

rem 脚本结束后保持窗口打开, 便于查看错误信息
pause
endlocal