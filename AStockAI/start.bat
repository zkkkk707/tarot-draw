@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0"
rem 仅对本窗口生效的环境变量（setlocal 范围内），不修改系统或用户环境变量
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"

if not exist ".venv\Scripts\python.exe" (
    echo [提示] 未找到虚拟环境 .venv\Scripts\python.exe
    echo        请先按 README.md 的“安装步骤”手动创建虚拟环境并安装依赖。
    echo        本脚本不会自动安装任何依赖，也不会注册开机自启或计划任务。
    pause
    exit /b 1
)

echo 正在启动 AStockAI V1.0 ... 按 Ctrl+C 或运行 stop.bat 可正常退出。
".venv\Scripts\python.exe" -X utf8 src\main.py --config config\settings.yaml %*
set "RC=%ERRORLEVEL%"
echo.
echo AStockAI 已退出，退出码 %RC%（0=正常 2=配置错误 3=无法建立内存硬限制 4=已有实例运行 5=内存保护退出）
pause
endlocal & exit /b %RC%
