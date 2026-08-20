@echo off
chcp 65001 >nul
cd /d "%~dp0"

rem ---- 找 Python：优先 python 命令，其次 py -3 启动器 ----
set "PYCMD="
where python >nul 2>nul
if %errorlevel%==0 set "PYCMD=python"
if not defined PYCMD (
    where py >nul 2>nul
    if %errorlevel%==0 set "PYCMD=py -3"
)
if not defined PYCMD (
    echo.
    echo 没有找到 Python，请先安装 Python 3.9 或更高版本：
    echo   1. 打开 https://www.python.org/downloads/ 下载并安装
    echo   2. 安装时务必勾选 "Add Python to PATH"
    echo   3. 装好后重新双击本文件
    echo.
    pause
    exit /b 1
)

%PYCMD% startup.py
echo.
pause
