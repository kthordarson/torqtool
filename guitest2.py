#!/usr/bin/python3
# Thin entrypoint — all classes live in guiclasses/
import sys
import argparse

from loguru import logger

# todo check openstreetmaps fore more info

def get_args(appname: str):
	parser = argparse.ArgumentParser(description=appname)
	parser.add_argument("--dbhost", default="localhost", action="store")
	parser.add_argument("--dbmode", default="sqlite", action="store", dest="dbmode")
	parser.add_argument("--dbname", default="torq", action="store")
	parser.add_argument("--dbpass", default="qrot", action="store")
	parser.add_argument("--dbuser", default="torq", action="store")
	parser.add_argument("--dbfile", default="torqdata.db", action="store")
	parser.add_argument("-d", "--debug", default=False, action="store_true", dest="debug")
	parser.add_argument('--main-window', help="start main window", action="store_true", dest='main_window', default=True)
	parser.add_argument('--pos-manager', help="start position manager window", action="store_true", dest='pos_manager', default=False)
	parser.add_argument('--start-end-window', help="start start/end grouped window", action="store_true", dest='start_end_window', default=False)
	parser.add_argument('--tabbed-workspace', help="start tabbed workspace with main/positions/start-end", action="store_true", dest='tabbed_workspace', default=False)
	return parser.parse_args()

if __name__ == "__main__":
	logger.debug("Entrypoint started")
	from PySide6.QtWidgets import QApplication
	from sqlalchemy import create_engine
	from datamodels import database_init

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
	if args.tabbed_workspace:
		from guiclasses import MainWindow
		logger.debug("Starting tabbed workspace")
		engine = create_engine(dburl)
		database_init(engine)
		app = QApplication(sys.argv)
		workspace = MainWindow(args, engine)
		workspace.showMaximized()
		sys.exit(app.exec())
	elif args.pos_manager:
		from guiclasses import PositionManagerWindow
		logger.debug("Starting position manager")
		engine = create_engine(dburl)
		database_init(engine)
		app = QApplication(sys.argv)
		pm = PositionManagerWindow(args, engine)
		pm.show()
		sys.exit(app.exec())
	elif args.start_end_window:
		from guiclasses import StartEndWindow
		logger.debug("Starting start/end grouped window")
		engine = create_engine(dburl)
		database_init(engine)
		app = QApplication(sys.argv)
		window = StartEndWindow(args, engine)
		window.showMaximized()
		sys.exit(app.exec())
	elif args.main_window:
		logger.debug("Starting main window")
		from guiclasses import MainWindow
		engine = create_engine(dburl)
		database_init(engine)
		app = QApplication(sys.argv)
		window = MainWindow(args, engine)
		window.showMaximized()
		sys.exit(app.exec())
