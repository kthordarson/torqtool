from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from PySide6.QtCore import QObject, Signal, QThread


class TripPlotWorker(QObject):
	finished = Signal(int, object)
	error = Signal(int, str)
	progress = Signal(int, int, int)
	cancelled = Signal(int)

	def __init__(
		self,
		db_url: str,
		request_id: int,
		fileids: list[int],
		metric_name: str,
		lat_col: str,
		lon_col: str,
		time_col: str | None,
	):
		super().__init__()
		self.db_url = db_url
		self.request_id = int(request_id)
		self.fileids = [int(fid) for fid in fileids]
		self.metric_name = metric_name
		self.lat_col = lat_col
		self.lon_col = lon_col
		self.time_col = time_col

	@staticmethod
	def _lonlat_to_web_mercator_np(lon: np.ndarray, lat: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
		lat_clamped = np.clip(lat, -85.05112878, 85.05112878)
		x = lon * 20037508.34 / 180.0
		y = np.log(np.tan(np.pi / 4.0 + np.deg2rad(lat_clamped) / 2.0)) * 6378137.0
		return x, y

	def run(self):
		engine = None
		try:
			engine = create_engine(self.db_url)
			trips: list[dict[str, Any]] = []
			all_x: list[float] = []
			all_y: list[float] = []
			all_metric_values: list[float] = []

			time_select = f', "{self.time_col}" AS metric_time' if self.time_col else ''
			order_col = f'"{self.time_col}"' if self.time_col else 'id'

			for current_index, fileid in enumerate(self.fileids, start=1):
				thread = QThread.currentThread()
				if thread is not None and thread.isInterruptionRequested():
					self.cancelled.emit(self.request_id)
					return
				q = (
					f'SELECT "{self.lon_col}" AS longitude, "{self.lat_col}" AS latitude, '
					f'"{self.metric_name}" AS selectedmetric{time_select} '
					f'FROM torqlogs WHERE fileid = {int(fileid)} ORDER BY {order_col}'
				)
				df = pd.read_sql(q, engine)
				if df.empty:
					self.progress.emit(self.request_id, current_index, len(self.fileids))
					continue

				lon_series = pd.to_numeric(df["longitude"], errors="coerce")
				lat_series = pd.to_numeric(df["latitude"], errors="coerce")
				metric_series = pd.to_numeric(df["selectedmetric"], errors="coerce").fillna(0.0)
				valid = lon_series.notna() & lat_series.notna()
				if not valid.any():
					self.progress.emit(self.request_id, current_index, len(self.fileids))
					continue

				lon_vals = lon_series[valid].to_numpy(dtype=float)
				lat_vals = lat_series[valid].to_numpy(dtype=float)
				speed_vals = metric_series[valid].to_numpy(dtype=float)
				x_vals, y_vals = self._lonlat_to_web_mercator_np(lon_vals, lat_vals)

				time_values: list[Any] = []
				if self.time_col and "metric_time" in df.columns:
					time_values = pd.to_datetime(df.loc[valid, "metric_time"], errors="coerce").tolist()

				trip_payload = {
					"fileid": int(fileid),
					"x": x_vals.tolist(),
					"y": y_vals.tolist(),
					"speed": speed_vals.tolist(),
					"time": time_values,
				}
				trips.append(trip_payload)
				all_x.extend(trip_payload["x"])
				all_y.extend(trip_payload["y"])
				all_metric_values.extend(trip_payload["speed"])
				self.progress.emit(self.request_id, current_index, len(self.fileids))

			self.finished.emit(
				self.request_id,
				{
					"trips": trips,
					"all_x": all_x,
					"all_y": all_y,
					"all_metric_values": all_metric_values,
				},
			)
		except Exception as e:
			self.error.emit(self.request_id, f"Failed to load trip plot data: {e} ({type(e)})")
		finally:
			if engine is not None:
				engine.dispose()
