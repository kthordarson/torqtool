import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from PySide6.QtCore import QObject, Signal


class PositionLoadWorker(QObject):
	finished = Signal(object)
	error = Signal(str)

	def __init__(self, engine_url: str, pos_type: str | None = None, trip_id: int | None = None):
		super().__init__()
		self.engine_url = engine_url
		self.pos_type = pos_type
		self.trip_id = trip_id

	@staticmethod
	def _to_web_mercator(df: pd.DataFrame) -> pd.DataFrame:
		if df.empty:
			return df
		lon = df["longitude"].astype(float)
		lat = df["latitude"].astype(float)
		x = lon * 20037508.34 / 180.0
		y = np.log(np.tan((90.0 + lat) * np.pi / 360.0)) / (np.pi / 180.0)
		y = y * 20037508.34 / 180.0
		df = df.copy()
		df["x"] = x
		df["y"] = y
		return df

	def run(self):
		engine = None
		try:
			engine = create_engine(self.engine_url)

			def _load_for_type(pos_type: str) -> pd.DataFrame:
				table = "startpos" if pos_type == "start" else "endpos"
				id_col = "startid" if pos_type == "start" else "endid"
				query = f"SELECT {id_col} AS pos_id, latitude, longitude, count, label FROM {table}"
				if self.trip_id is not None:
					query += f" WHERE fileid = {int(self.trip_id)}"
				df_part = pd.read_sql(query, engine)
				df_part.insert(0, "pos_type", pos_type)
				return df_part

			if self.pos_type in ("start", "end"):
				df = _load_for_type(self.pos_type)
			else:
				df_start = _load_for_type("start")
				df_end = _load_for_type("end")
				df = pd.concat([df_start, df_end], ignore_index=True)

			df = self._to_web_mercator(df)
			self.finished.emit(df)
		except Exception as e:
			pos_type = self.pos_type if self.pos_type in ("start", "end") else "start/end"
			self.error.emit(f"Failed to load {pos_type} positions: {e} ({type(e)})")
		finally:
			if engine is not None:
				engine.dispose()
