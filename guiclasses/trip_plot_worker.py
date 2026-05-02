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
	preview = Signal(int, object)
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
			preview_xy_by_file: dict[int, tuple[list[float], list[float]]] = {}

			order_col = f'"{self.time_col}"' if self.time_col else 'id'

			# Phase 1: fetch lon/lat first so UI can render trip paths immediately.
			for current_index, fileid in enumerate(self.fileids, start=1):
				thread = QThread.currentThread()
				if thread is not None and thread.isInterruptionRequested():
					self.cancelled.emit(self.request_id)
					return

				q_preview = (
					f'SELECT "{self.lon_col}" AS longitude, "{self.lat_col}" AS latitude '
					f'FROM torqlogs '
					f'WHERE fileid = {int(fileid)} '
					f'AND "{self.lon_col}" IS NOT NULL '
					f'AND "{self.lat_col}" IS NOT NULL '
					f'ORDER BY {order_col}'
				)
				df_preview = pd.read_sql(q_preview, engine)
				if df_preview.empty:
					self.preview.emit(self.request_id, {"done": current_index, "total": len(self.fileids), "trip": None})
					continue

				lon_series = pd.to_numeric(df_preview["longitude"], errors="coerce")
				lat_series = pd.to_numeric(df_preview["latitude"], errors="coerce")
				valid = lon_series.notna() & lat_series.notna()
				if not valid.any():
					self.preview.emit(self.request_id, {"done": current_index, "total": len(self.fileids), "trip": None})
					continue

				lon_vals = lon_series[valid].to_numpy(dtype=float)
				lat_vals = lat_series[valid].to_numpy(dtype=float)
				x_vals, y_vals = self._lonlat_to_web_mercator_np(lon_vals, lat_vals)
				x_list = x_vals.tolist()
				y_list = y_vals.tolist()
				preview_xy_by_file[int(fileid)] = (x_list, y_list)
				all_x.extend(x_list)
				all_y.extend(y_list)
				self.preview.emit(
					self.request_id,
					{
						"done": current_index,
						"total": len(self.fileids),
						"trip": {
							"fileid": int(fileid),
							"x": x_list,
							"y": y_list,
						},
					},
				)

			# Phase 2: fetch metric/time data while lon/lat preview is already displayed.
			time_select = f', "{self.time_col}" AS metric_time' if self.time_col else ''
			for current_index, fileid in enumerate(self.fileids, start=1):
				thread = QThread.currentThread()
				if thread is not None and thread.isInterruptionRequested():
					self.cancelled.emit(self.request_id)
					return

				xy_payload = preview_xy_by_file.get(int(fileid))
				if not xy_payload:
					self.progress.emit(self.request_id, current_index, len(self.fileids))
					continue
				x_list, y_list = xy_payload
				q = (
					f'SELECT "{self.metric_name}" AS selectedmetric{time_select} '
					f'FROM torqlogs '
					f'WHERE fileid = {int(fileid)} '
					f'AND "{self.lon_col}" IS NOT NULL '
					f'AND "{self.lat_col}" IS NOT NULL '
					f'ORDER BY {order_col}'
				)
				df = pd.read_sql(q, engine)
				if df.empty:
					self.progress.emit(self.request_id, current_index, len(self.fileids))
					continue

				metric_series = pd.to_numeric(df["selectedmetric"], errors="coerce").fillna(0.0)
				if metric_series.empty:
					self.progress.emit(self.request_id, current_index, len(self.fileids))
					continue

				speed_vals = metric_series.to_numpy(dtype=float)
				point_count = min(len(x_list), len(y_list), len(speed_vals))
				if point_count <= 0:
					self.progress.emit(self.request_id, current_index, len(self.fileids))
					continue

				time_values: list[Any] = []
				if self.time_col and "metric_time" in df.columns:
					time_values = pd.to_datetime(df["metric_time"], errors="coerce").tolist()[:point_count]

				trip_payload = {
					"fileid": int(fileid),
					"x": x_list[:point_count],
					"y": y_list[:point_count],
					"speed": speed_vals.tolist()[:point_count],
					"time": time_values,
				}
				trips.append(trip_payload)
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
