@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0"
if not exist "run" mkdir "run"
type nul > "run\STOP"
echo 已创建停止标记 run\STOP。
echo 正在运行的 AStockAI 会在当前批次结束后（通常数秒内）写出最终状态并正常退出。
endlocal
