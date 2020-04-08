call C:\ProgramData\Anaconda3\Scripts\activate.bat C:\ProgramData\Anaconda3\envs\dev_py_376_rul_spod_2
call python downloader.py
for /f "skip=1" %%d in ('wmic os get localdatetime') do if not defined mydate set mydate=%%d
set dirname_log=%mydate:~0,8%_downloader_log
md %dirname_log%
move *.log %dirname_log%