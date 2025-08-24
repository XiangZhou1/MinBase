@echo off
rem =================================================================
rem MinBase Server Startup Script for Windows (Corrected Version)
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
rem 这更健壮、更高效, 并且完美解决了 "输入行太长" 的问题.
set "CLASSPATH=%CONF_DIR%;%LIB_DIR%\*"
rem --- END MODIFIED SECTION ---

rem 设置 JVM 选项。
if not defined JAVA_OPTS (
    set "JAVA_OPTS=-Xms512m -Xmx1024m"
)

rem 设置主启动类
set "MAIN_CLASS=org.minbase.server.MinBaseServer"

echo.
echo Starting MinBase Server...
echo =================================================================
echo BASE_DIR:   %BASE_DIR%
echo JAVA_OPTS:  %JAVA_OPTS%
echo MAIN_CLASS: %MAIN_CLASS%
echo CLASSPATH:  %CLASSPATH%
echo =================================================================
echo.

rem 执行 Java 命令, 使用修正后的、简洁的 CLASSPATH
java %JAVA_OPTS% -cp "%CLASSPATH%" %MAIN_CLASS% %CONF_DIR%/minbase.conf

rem 如果 Java 程序立即退出, 下面的 pause 命令可以防止窗口立即关闭, 便于查看错误信息
pause
endlocal