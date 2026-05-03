#!/usr/bin/python3
# Thin entrypoint — all classes live in guiclasses/
import sys

from loguru import logger


def get_args(appname: str):
	# Import parser helper lazily to keep process startup fast.
	from utils import get_parser

	parser = get_parser(appname)
	return parser.parse_args()

if __name__ == "__main__":
	logger.debug("Entrypoint started")
	from PySide6.QtWidgets import QApplication
	from sqlalchemy import create_engine
	from utils import database_init

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
		from guiclasses import MainWindow
		logger.debug("Starting main window")
		engine = create_engine(dburl)
		database_init(engine)
		app = QApplication(sys.argv)
		window = MainWindow(args, engine)
		window.showMaximized()
		sys.exit(app.exec())
