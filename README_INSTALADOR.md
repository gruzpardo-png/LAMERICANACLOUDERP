@echo off
setlocal
cd /d %~dp0\..\local_app
py -m pip install requests pandas openpyxl pyserial python-barcode pillow pywin32 werkzeug pyinstaller
py -m PyInstaller --onefile --windowed lamericana_labeler_local.py --name LAMERICANA_LabelCloud --hidden-import=requests --hidden-import=urllib3 --hidden-import=certifi --hidden-import=charset_normalizer --hidden-import=idna
mkdir ..\dist_instalador 2>nul
copy dist\LAMERICANA_LabelCloud.exe ..\dist_instalador\LAMERICANA_LabelCloud.exe
copy terminal_config.json ..\dist_instalador\terminal_config.json 2>nul
mkdir ..\dist_instalador\logs 2>nul
echo Listo. EXE generado en dist_instalador\LAMERICANA_LabelCloud.exe
pause
