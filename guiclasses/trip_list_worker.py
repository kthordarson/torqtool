import pandas as pd
from sqlalchemy import create_engine
from PySide6.QtCore import QObject, Signal
from loguru import logger

class TripListWorker(QObject):
	finished = Signal(object)
	error = Signal(str)

	def __init__(self, db_url: str):
		super().__init__()
		self.db_url = db_url

	def run(self):
		engine = None
		try:
			engine = create_engine(self.db_url)
			df_trips = pd.read_sql("SELECT id,fileid,trip_distance,tripdate,time FROM torqtrips", engine)
			logger.debug(f"Loaded {len(df_trips)} trips from database")
			self.finished.emit(df_trips)
		except Exception as e:
			self.error.emit(f"Failed to load torqtrips: {e} ({type(e)})")
		finally:
			if engine is not None:
				engine.dispose()
