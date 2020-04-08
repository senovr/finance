REM put your own path to the activate.bat and environment below
call C:\Users\Sergey\Anaconda3\Scripts\activate.bat C:\Users\Sergey\Anaconda3\envs\DL_py378
call python downloader.py
for /f "skip=1" %%d in ('wmic os get localdatetime') do if not defined mydate set mydate=%%d
set dirname_log=%mydate:~0,8%_downloader_log
md %dirname_log%
move *.log %dirname_log%