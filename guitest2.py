#!/usr/bin/python3
# Thin entrypoint — all classes live in guiclasses/
import sys
import matplotlib
matplotlib.use("QtAgg")
from PySide6.QtWidgets import QApplication
from converter import get_args
from guiclasses import MainWindow
from loguru import logger

if __name__ == "__main__":
	args = get_args('guitest2')
	app = QApplication(sys.argv)
	window = MainWindow(args)
	logger.debug(f"Starting application event loop window: {window}")
	window.showMaximized()
	sys.exit(app.exec())
