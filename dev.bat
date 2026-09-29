@echo off
REM ============================================================
REM LIFELINE: Combined Fullstack Development Launcher
REM Launches FastAPI backend on port 8000 and Vite frontend
REM ============================================================

set LIFELINE_DEMO=1
echo Starting LIFELINE Fullstack Platform...
echo API and Built UI will be available at: http://localhost:8000
echo.
py run_fullstack.py --dev
