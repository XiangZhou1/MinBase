@echo off
rem =================================================================
rem MinBase Server Shutdown Script for Windows
rem =================================================================

setlocal
rem 设置要查找的主类
set "MAIN_CLASS=org.minbase.server.MinBaseServer"
set "PID="

echo =================================================================
echo  Stopping MinBase Server...
echo =================================================================

rem 使用 wmic 查找包含主类的 Java 进程
rem FOR /F 会逐行处理命令的输出
rem "tokens=2" 获取第二列 (PID), "delims==" 按等号分割
rem findstr 用于过滤出有效的 PID 行
for /f "tokens=2 delims==" %%i in ('wmic process where "name='java.exe' and commandline like '%%%MAIN_CLASS%%%'" get processid /value ^| findstr "ProcessId"') do (
    set "PID=%%i"
)

rem 检查是否找到了 PID
if not defined PID (
    echo MinBase Server process not found.
    goto :EOF
)

echo Found MinBase Server process with PID: %PID%
echo Sending shutdown signal...

rem 终止进程
taskkill /PID %PID%

rem 检查命令执行结果
if errorlevel 1 (
    echo Failed to stop the process. It may require administrator privileges.
) else (
    echo Process stopped successfully.
)

echo =================================================================
pause
endlocal
:EOF