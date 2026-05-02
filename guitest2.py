#!/usr/bin/python3
# Thin entrypoint — all classes live in guiclasses/
import sys
import matplotlib
matplotlib.use("QtAgg")
from PySide6.QtWidgets import QApplication
from sqlalchemy import create_engine
from utils import database_init
from converter import get_args
from guiclasses import MainWindow
from guiclasses import PositionManagerWindow

from loguru import logger

if __name__ == "__main__":
	args = get_args('guitest2')
	# Set up SQLAlchemy session
	# session = get_engine_session(args)
	# self.engine = create_engine(args.dburl)
	if args.dbmode == 'psql':
		dburl = f"postgresql://{args.dbuser}:{args.dbpass}@{args.dbhost}/{args.dbname}"
	elif args.dbmode == 'sqlite':
		dburl = f"sqlite:///{args.dbfile}"
	else:
		dburl = ''
	# engine = create_engine(dburl)
	if args.pos_manager:
		logger.debug("Starting position manager")
		engine = create_engine(dburl)
		database_init(engine)
		app = QApplication(sys.argv)
		pm = PositionManagerWindow(args, engine)
		pm.show()
		sys.exit(app.exec())
	elif args.main_window:
		logger.debug("Starting main window")
		engine = create_engine(dburl)
		database_init(engine)
		app = QApplication(sys.argv)
		window = MainWindow(args, engine)
		window.showMaximized()
		sys.exit(app.exec())
