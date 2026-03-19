@echo off
pip install pyinstaller
pip install -r requirements.txt
pyinstaller --onefile --console --name PDF_Overlay_Processor pdf_overlay.py
echo Done! Executable is in dist folder
pause