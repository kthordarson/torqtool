#!/usr/bin/python3
from loguru import logger
import contextily as ctx
import geopandas as gpd
from shapely.geometry import Point
import sys
import io
import numpy as np
import pandas as pd
from typing import Any, cast
from PySide6.QtWidgets import (
	QApplication, QMainWindow, QTableView, QVBoxLayout, QWidget, QSplitter,
	QHBoxLayout, QLabel, QComboBox, QFrame, QListWidget, QListWidgetItem,
	QScrollArea, QFileDialog, QMessageBox, QSlider, QLineEdit, QPushButton,
	QFormLayout, QSpinBox, QDoubleSpinBox
)
from PySide6.QtGui import QFont, QAction
from PySide6.QtWidgets import QAbstractItemView
from PySide6.QtCore import Qt, QAbstractTableModel, QModelIndex, QPersistentModelIndex, QTimer, QObject, Signal, QThread, QItemSelectionModel
from PySide6.QtGui import QCloseEvent
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
import matplotlib
matplotlib.use("QtAgg")
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
# from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt import NavigationToolbar2QT as NavigationToolbar
from datamodels import database_init
from schemas import dataschema
from converter import get_args
from metric_analysis import categorize_metric, get_analysis_suggestion, group_metrics_by_category  #, MetricCategory

def _normalize_col_name(value: str) -> str:
	return "".join(ch.lower() for ch in str(value) if ch.isalnum())


def _resolve_torqlogs_columns(engine, requested_columns: list[str]) -> dict[str, str]:
	inspector = inspect(engine)
	actual_columns = [str(col["name"]) for col in inspector.get_columns("torqlogs")]
	normalized_actual = {_normalize_col_name(col): col for col in actual_columns}
	resolved: dict[str, str] = {}
	for requested in requested_columns:
		actual = normalized_actual.get(_normalize_col_name(requested))
		if actual:
			resolved[requested] = actual
	return resolved

class MapCanvas(FigureCanvas):
	def __init__(self, parent=None):
		fig, self.ax = plt.subplots(figsize=(8, 6))
		super().__init__(fig)
		self.setParent(parent)
		logger.debug("MapCanvas initialized")

	def plot_trip(self, df):
		self.ax.clear()
		if not df.empty:
			self.ax.scatter(df['Longitude'], df['Latitude'], s=2, c='blue')
			self.ax.set_title("Trip Map")
			self.ax.set_xlabel("Longitude")
			self.ax.set_ylabel("Latitude")
		else:
			self.ax.set_title("No GPS data")
		self.draw()
		logger.debug(f"Trip plotted on map with {len(df)} points")

class TimeSeriesCanvas(FigureCanvas):
	def __init__(self, parent=None):
		fig, self.ax = plt.subplots(figsize=(6, 4))
		fig.subplots_adjust(bottom=0.22, top=0.90, left=0.13, right=0.97)
		super().__init__(fig)
		self.setParent(parent)

class BasemapWorker(QObject):
	finished = Signal(object, object, int)
	error = Signal(str, int)

	def __init__(self, bounds, zoom: int, request_id: int):
		super().__init__()
		self.bounds = bounds
		self.zoom = zoom
		self.request_id = request_id
		logger.debug(f"BasemapWorker initialized with bounds={bounds}, zoom={zoom}, request_id={request_id}")

	def run(self):
		try:
			west, east, south, north = self.bounds
			source = None
			try:
				source = cast(Any, ctx.providers).OpenStreetMap.Mapnik
			except Exception:
				source = None
			kwargs: dict[str, Any] = {"zoom": cast(Any, self.zoom)}
			if source is not None:
				kwargs["source"] = source
			img, ext = ctx.bounds2img(west, south, east, north, **kwargs)
			self.finished.emit(img, ext, self.request_id)
			logger.debug(f"BasemapWorker finished fetching basemap for request_id={self.request_id}")
		except Exception as e:
			self.error.emit(f'{e} {type(e)}', self.request_id)


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
			self.finished.emit(df_trips)
		except Exception as e:
			self.error.emit(f"Failed to load torqtrips: {e} ({type(e)})")
		finally:
			if engine is not None:
				engine.dispose()


class PositionTableModel(QAbstractTableModel):
	def __init__(self, source_df: pd.DataFrame):
		super().__init__()
		self._source = source_df
		self._columns = ["pos_type", "pos_id", "latitude", "longitude", "count", "label"]
		self._view_order = list(source_df.index)

	def rowCount(self, parent: QModelIndex | QPersistentModelIndex = QModelIndex()) -> int:
		return len(self._view_order)

	def columnCount(self, parent: QModelIndex | QPersistentModelIndex = QModelIndex()) -> int:
		return len(self._columns)

	def data(self, index: QModelIndex | QPersistentModelIndex, role: int = Qt.ItemDataRole.DisplayRole) -> object:
		if not index.isValid() or role != Qt.ItemDataRole.DisplayRole:
			return None
		source_row = self._view_order[index.row()]
		col = self._columns[index.column()]
		value = self._source.at[source_row, col]
		return str(value)

	def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.ItemDataRole.DisplayRole) -> object:
		if role != Qt.ItemDataRole.DisplayRole:
			return None
		if orientation == Qt.Orientation.Horizontal:
			return self._columns[section]
		return str(section)

	def sort(self, column: int, order: Qt.SortOrder = Qt.SortOrder.AscendingOrder) -> None:
		col = self._columns[column]
		ascending = order == Qt.SortOrder.AscendingOrder
		self.layoutAboutToBeChanged.emit()
		tmp = self._source.loc[self._view_order, [col]].copy()
		tmp["_src"] = self._view_order
		tmp.sort_values(by=col, ascending=ascending, inplace=True, kind="mergesort")
		self._view_order = [int(x) for x in tmp["_src"].tolist()]
		self.layoutChanged.emit()

	def source_row_for_view_row(self, view_row: int) -> int | None:
		if view_row < 0 or view_row >= len(self._view_order):
			return None
		return int(self._view_order[view_row])

	def view_row_for_source_row(self, source_row: int) -> int | None:
		try:
			return self._view_order.index(source_row)
		except ValueError:
			return None

	def source_rows(self) -> list[int]:
		return [int(x) for x in self._view_order]

	def set_view_order(self, source_rows: list[int]):
		self.layoutAboutToBeChanged.emit()
		valid = set(int(idx) for idx in self._source.index.tolist())
		self._view_order = [int(idx) for idx in source_rows if int(idx) in valid]
		self.layoutChanged.emit()


class PositionLoadWorker(QObject):
	finished = Signal(object)
	error = Signal(str)

	def __init__(self, db_url: str):
		super().__init__()
		self.db_url = db_url

	@staticmethod
	def _to_web_mercator(df: pd.DataFrame) -> pd.DataFrame:
		if df.empty:
			df["x"] = []
			df["y"] = []
			return df
		lon = pd.to_numeric(df["longitude"], errors="coerce").clip(-180, 180)
		lat = pd.to_numeric(df["latitude"], errors="coerce").clip(-85.05112878, 85.05112878)
		x = lon * 20037508.34 / 180.0
		rad = np.deg2rad(lat)
		y = np.log(np.tan(np.pi / 4.0 + rad / 2.0)) * 6378137.0
		df["x"] = x
		df["y"] = y
		return df

	def run(self):
		engine = None
		query = """
		SELECT 'start' AS pos_type,
			startid AS pos_id,
			latstart AS latitude,
			lonstart AS longitude,
			count,
			label
		FROM startpos
		UNION ALL
		SELECT 'end' AS pos_type,
			endid AS pos_id,
			latend AS latitude,
			lonend AS longitude,
			count,
			label
		FROM endpos
		ORDER BY pos_type, pos_id
		"""
		try:
			engine = create_engine(self.db_url)
			df = pd.read_sql(query, engine)
			if not df.empty:
				df = df.copy()
				df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
				df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
				df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)
				df["label"] = df["label"].fillna("")
				df = df.dropna(subset=["latitude", "longitude"]).reset_index(drop=True)
				df = self._to_web_mercator(df)
			else:
				df = pd.DataFrame(
					columns=["pos_type", "pos_id", "latitude", "longitude", "count", "label", "x", "y"]
				)
			self.finished.emit(df)
		except Exception as e:
			self.error.emit(f"Could not load position data: {e} ({type(e)})")
		finally:
			if engine is not None:
				engine.dispose()


class PositionManagerWindow(QMainWindow):
	def __init__(self, engine, parent=None):
		super().__init__(parent)
		self.engine = engine
		self.setWindowTitle("Position Manager")
		self.resize(1240, 780)

		self._selected_row_index: int | None = None
		self._selected_row_indices: list[int] = []
		self._scatter_index_map: dict[Any, list[int]] = {}
		self._table_model: PositionTableModel | None = None
		self._full_bounds: tuple[float, float, float, float] | None = None
		self._basemap_artist = None
		self._selected_markers: list[Any] = []
		self._point_label_artists: list[Any] = []
		self._show_point_labels = True
		self._basemap_mem_cache: dict[str, tuple[bytes, tuple[float, float, float, float]]] = {}
		self._load_thread: QThread | None = None
		self._load_worker: PositionLoadWorker | None = None
		self._basemap_thread: QThread | None = None
		self._basemap_worker: BasemapWorker | None = None
		self._basemap_request_id = 0
		self._pending_basemap_key: str | None = None
		self._queued_basemap_request: tuple[tuple[float, float, float, float], int] | None = None
		self._pending_close = False
		self._restore_after_reload: dict[str, Any] | None = None
		self._skip_sort_once = False
		self._min_zoom_span_m = 25.0
		self._current_basemap_zoom = 8
		self._min_count_filter = 0
		self.df_positions = pd.DataFrame(
			columns=["pos_type", "pos_id", "latitude", "longitude", "count", "label", "x", "y"]
		)

		central = QWidget()
		main_layout = QVBoxLayout(central)
		splitter = QSplitter(Qt.Orientation.Horizontal)

		left_panel = QWidget()
		left_layout = QVBoxLayout(left_panel)
		self.map_fig, self.map_ax = plt.subplots(figsize=(8, 6))
		self.map_canvas = FigureCanvas(self.map_fig)
		self.map_toolbar = NavigationToolbar(self.map_canvas, self)
		left_layout.addWidget(self.map_toolbar)
		left_layout.addWidget(self.map_canvas)

		right_panel = QWidget()
		right_layout = QVBoxLayout(right_panel)

		self.positions_table = QTableView()
		self.positions_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
		self.positions_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
		right_layout.addWidget(self.positions_table, stretch=6)

		editor = QFrame()
		editor_layout = QVBoxLayout(editor)
		editor_layout.setContentsMargins(8, 8, 8, 8)

		self.selected_info = QLabel("Loading positions...")
		self.selected_info.setWordWrap(True)
		editor_layout.addWidget(self.selected_info)

		form = QFormLayout()
		self.pos_type_combo = QComboBox()
		self.pos_type_combo.addItems(["start", "end"])
		self.pos_id_spin = QSpinBox()
		self.pos_id_spin.setRange(1, 2_147_483_647)
		self.lat_spin = QDoubleSpinBox()
		self.lat_spin.setDecimals(7)
		self.lat_spin.setRange(-90.0, 90.0)
		self.lon_spin = QDoubleSpinBox()
		self.lon_spin.setDecimals(7)
		self.lon_spin.setRange(-180.0, 180.0)
		self.count_spin = QSpinBox()
		self.count_spin.setRange(0, 10_000_000)
		self.min_count_filter_spin = QSpinBox()
		self.min_count_filter_spin.setRange(0, 10_000_000)
		self.min_count_filter_spin.setValue(0)
		self.min_count_filter_spin.setToolTip("Only show rows with count >= this value")
		self.label_edit = QLineEdit()
		self.label_edit.setPlaceholderText("Location label")
		form.addRow("Type", self.pos_type_combo)
		form.addRow("ID", self.pos_id_spin)
		form.addRow("Latitude", self.lat_spin)
		form.addRow("Longitude", self.lon_spin)
		form.addRow("Count", self.count_spin)
		form.addRow("Min count (table)", self.min_count_filter_spin)
		form.addRow("Label", self.label_edit)
		editor_layout.addLayout(form)

		button_row = QHBoxLayout()
		self.refresh_btn = QPushButton("Refresh")
		self.new_btn = QPushButton("New")
		self.save_btn = QPushButton("Save")
		self.apply_label_btn = QPushButton("Apply label to selected")
		self.delete_btn = QPushButton("Delete")
		self.zoom_in_btn = QPushButton("Zoom in")
		self.zoom_out_step_btn = QPushButton("Zoom out")
		self.zoom_out_btn = QPushButton("Full zoom out")
		for btn in (self.zoom_in_btn, self.zoom_out_step_btn, self.zoom_out_btn):
			btn.setFixedSize(92, 26)
		self.toggle_labels_btn = QPushButton("Labels on")
		self.toggle_labels_btn.setCheckable(True)
		self.toggle_labels_btn.setChecked(True)
		self.sort_similar_btn = QPushButton("Sort by similar lat/lon")
		button_row.addWidget(self.refresh_btn)
		button_row.addWidget(self.new_btn)
		button_row.addWidget(self.save_btn)
		button_row.addWidget(self.apply_label_btn)
		button_row.addWidget(self.delete_btn)
		button_row.addWidget(self.zoom_in_btn)
		button_row.addWidget(self.zoom_out_step_btn)
		button_row.addWidget(self.zoom_out_btn)
		button_row.addWidget(self.toggle_labels_btn)
		button_row.addWidget(self.sort_similar_btn)
		button_row.addStretch()
		editor_layout.addLayout(button_row)

		right_layout.addWidget(editor, stretch=2)

		splitter.addWidget(left_panel)
		splitter.addWidget(right_panel)
		splitter.setSizes([760, 480])
		main_layout.addWidget(splitter)
		self.setCentralWidget(central)

		self._pick_cid = self.map_canvas.mpl_connect("pick_event", self._on_pick_point)
		self.refresh_btn.clicked.connect(self.load_positions)
		self.new_btn.clicked.connect(self._start_new_entry)
		self.save_btn.clicked.connect(self.save_entry)
		self.apply_label_btn.clicked.connect(self.apply_label_to_selected)
		self.delete_btn.clicked.connect(self.delete_entry)
		self.min_count_filter_spin.valueChanged.connect(self._on_min_count_filter_changed)
		self.zoom_in_btn.clicked.connect(self._zoom_in)
		self.zoom_out_step_btn.clicked.connect(self._zoom_out)
		self.zoom_out_btn.clicked.connect(self._zoom_full)
		self.toggle_labels_btn.toggled.connect(self._on_toggle_labels)
		self.sort_similar_btn.clicked.connect(self._sort_table_by_similar_latlon)

		self.load_positions()

	@staticmethod
	def _table_info(pos_type: str) -> tuple[str, str, str, str]:
		if pos_type == "start":
			return ("startpos", "startid", "latstart", "lonstart")
		return ("endpos", "endid", "latend", "lonend")

	@staticmethod
	def _bounds_key(bounds: tuple[float, float, float, float], zoom: int) -> str:
		xmin, xmax, ymin, ymax = bounds
		# Quantize to improve cache hit ratio across tiny pan/zoom jitter.
		q = 5.0
		xmin_q = round(xmin / q) * q
		xmax_q = round(xmax / q) * q
		ymin_q = round(ymin / q) * q
		ymax_q = round(ymax / q) * q
		return f"posmgr|{zoom}|{xmin_q:.1f}|{xmax_q:.1f}|{ymin_q:.1f}|{ymax_q:.1f}"

	@staticmethod
	def _normalize_bounds(bounds: tuple[float, float, float, float]) -> tuple[float, float, float, float]:
		x0, x1, y0, y1 = bounds
		xmin, xmax = (x0, x1) if x0 <= x1 else (x1, x0)
		ymin, ymax = (y0, y1) if y0 <= y1 else (y1, y0)
		return xmin, xmax, ymin, ymax

	def _current_view_bounds(self) -> tuple[float, float, float, float]:
		x0, x1 = self.map_ax.get_xlim()
		y0, y1 = self.map_ax.get_ylim()
		return self._normalize_bounds((float(x0), float(x1), float(y0), float(y1)))

	def _recommended_zoom_for_bounds(self, bounds: tuple[float, float, float, float]) -> int:
		xmin, xmax, ymin, ymax = bounds
		span = max(xmax - xmin, ymax - ymin)
		if span > 20_000_000:
			return 3
		if span > 10_000_000:
			return 4
		if span > 5_000_000:
			return 5
		if span > 2_500_000:
			return 6
		if span > 1_000_000:
			return 7
		if span > 300_000:
			return 8
		if span > 120_000:
			return 9
		if span > 50_000:
			return 10
		if span > 20_000:
			return 11
		if span > 8_000:
			return 12
		if span > 3_000:
			return 13
		if span > 1_200:
			return 14
		if span > 500:
			return 15
		if span > 250:
			return 16
		return 17

	def _refresh_basemap_for_current_view(self, target_zoom: int | None = None):
		bounds = self._current_view_bounds()
		zoom = target_zoom if target_zoom is not None else self._recommended_zoom_for_bounds(bounds)
		zoom = max(3, min(18, int(zoom)))
		self._current_basemap_zoom = zoom
		self._start_async_basemap(bounds, zoom)

	@staticmethod
	def _thread_is_running(thread: QThread | None) -> bool:
		if thread is None:
			return False
		try:
			return thread.isRunning()
		except RuntimeError:
			return False

	def _stop_thread(self, attr_name: str, wait_ms: int = 1200) -> bool:
		thread = cast(QThread | None, getattr(self, attr_name, None))
		if thread is None:
			return True
		try:
			if not thread.isRunning():
				setattr(self, attr_name, None)
				return True
			thread.requestInterruption()
			thread.quit()
			stopped = thread.wait(wait_ms)
			if stopped:
				setattr(self, attr_name, None)
				return True
			logger.warning(f"Thread {attr_name} still running after {wait_ms}ms; waiting for natural completion")
			return False
		except RuntimeError:
			setattr(self, attr_name, None)
			return True

	def _load_cached_basemap(self, cache_key: str) -> tuple[bytes, tuple[float, float, float, float]] | None:
		if cache_key in self._basemap_mem_cache:
			return self._basemap_mem_cache[cache_key]
		q = text(
			"""
			SELECT image_png, ext_west, ext_east, ext_south, ext_north
			FROM mapimagecache
			WHERE selection_key = :selection_key AND zoom = :zoom AND colormap = :colormap
			LIMIT 1
			"""
		)
		with self.engine.connect() as conn:
			row = conn.execute(q, {
				"selection_key": cache_key,
				"zoom": -2,
				"colormap": "posmgr",
			}).first()
		if row and all(v is not None for v in row[1:5]):
			payload = (bytes(row[0]), (float(row[1]), float(row[2]), float(row[3]), float(row[4])))
			self._basemap_mem_cache[cache_key] = payload
			return payload
		return None

	def _save_cached_basemap(self, cache_key: str, img, ext: tuple[float, float, float, float]):
		buf = io.BytesIO()
		mpimg.imsave(buf, img, format="png")
		image_bytes = buf.getvalue()
		self._basemap_mem_cache[cache_key] = (image_bytes, ext)
		upsert_sql = text(
			"""
			INSERT INTO mapimagecache (fileid, selection_key, zoom, colormap, image_png, ext_west, ext_east, ext_south, ext_north, created_at, updated_at)
			VALUES (NULL, :selection_key, :zoom, :colormap, :image_png, :ext_west, :ext_east, :ext_south, :ext_north, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
			ON CONFLICT(selection_key, zoom, colormap)
			DO UPDATE SET image_png = excluded.image_png,
				ext_west = excluded.ext_west,
				ext_east = excluded.ext_east,
				ext_south = excluded.ext_south,
				ext_north = excluded.ext_north,
				updated_at = CURRENT_TIMESTAMP
			"""
		)
		with self.engine.begin() as conn:
			conn.execute(upsert_sql, {
				"selection_key": cache_key,
				"zoom": -2,
				"colormap": "posmgr",
				"image_png": image_bytes,
				"ext_west": ext[0],
				"ext_east": ext[1],
				"ext_south": ext[2],
				"ext_north": ext[3],
			})

	def _start_async_basemap(self, bounds: tuple[float, float, float, float], zoom: int):
		cache_key = self._bounds_key(bounds, zoom)

		cached = self._load_cached_basemap(cache_key)
		if cached:
			img, ext = cached
			self._draw_basemap_from_bytes(img, ext)
			return

		if self._thread_is_running(self._basemap_thread):
			# Keep latest viewport request and execute it after current worker finishes.
			self._queued_basemap_request = (bounds, zoom)
			return

		self._basemap_request_id += 1
		request_id = self._basemap_request_id
		self._pending_basemap_key = cache_key

		thread = QThread()
		worker = BasemapWorker(bounds, zoom, request_id)
		worker.moveToThread(thread)
		thread.started.connect(worker.run)
		worker.finished.connect(self._on_basemap_loaded)
		worker.error.connect(self._on_basemap_error)
		worker.finished.connect(thread.quit)
		worker.error.connect(thread.quit)
		thread.finished.connect(worker.deleteLater)
		thread.finished.connect(thread.deleteLater)
		thread.finished.connect(self._on_basemap_thread_finished)
		self._basemap_thread = thread
		self._basemap_worker = worker
		thread.start()

	def _on_basemap_thread_finished(self):
		self._basemap_thread = None
		self._basemap_worker = None
		if self._queued_basemap_request is not None:
			bounds, zoom = self._queued_basemap_request
			self._queued_basemap_request = None
			self._start_async_basemap(bounds, zoom)
			return
		if self._pending_close and not self._thread_is_running(self._load_thread):
			self._pending_close = False
			self.close()

	def _on_basemap_loaded(self, img, ext, request_id: int):
		if request_id != self._basemap_request_id:
			return
		a, b, c, d = ext
		ext_typed: tuple[float, float, float, float] = (float(a), float(b), float(c), float(d))
		if self._pending_basemap_key is not None:
			self._save_cached_basemap(self._pending_basemap_key, img, ext_typed)
		self._current_basemap_zoom = max(3, min(18, self._current_basemap_zoom))
		self._draw_basemap_array(img, ext_typed)

	def _on_basemap_error(self, error: str, request_id: int):
		if request_id != self._basemap_request_id:
			return
		logger.warning(f"PositionManager basemap load failed: {error}")
		# Fallback path: try a synchronous paint if async worker/provider failed.
		if self._full_bounds is not None:
			try:
				ctx.add_basemap(self.map_ax, crs="EPSG:3857")
				self.map_canvas.draw()
			except Exception as sync_err:
				logger.warning(f"PositionManager sync basemap fallback failed: {sync_err} ({type(sync_err)})")

	def _draw_basemap_array(self, img, ext: tuple[float, float, float, float]):
		if self._basemap_artist is not None:
			try:
				self._basemap_artist.remove()
			except Exception:
				pass
		self._basemap_artist = self.map_ax.imshow(img, extent=ext, interpolation="bilinear", zorder=0)
		self.map_canvas.draw()

	def _draw_basemap_from_bytes(self, image_bytes: bytes, ext: tuple[float, float, float, float]):
		img = mpimg.imread(io.BytesIO(image_bytes), format="png")
		self._draw_basemap_array(img, ext)

	def _set_table_model(self):
		filtered_df = self.df_positions
		if self._min_count_filter > 0 and not self.df_positions.empty:
			filtered_df = self.df_positions[self.df_positions["count"] >= self._min_count_filter]
		self._table_model = PositionTableModel(filtered_df)
		self.positions_table.setModel(self._table_model)
		self.positions_table.horizontalHeader().setStretchLastSection(True)
		if self._skip_sort_once:
			self.positions_table.setSortingEnabled(False)
			self._skip_sort_once = False
		else:
			self.positions_table.setSortingEnabled(True)
		self.positions_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
		self.positions_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
		if self.positions_table.selectionModel() is not None:
			self.positions_table.selectionModel().selectionChanged.connect(self._on_table_selection_changed)

	def _on_min_count_filter_changed(self, value: int):
		self._min_count_filter = int(value)
		self._set_table_model()
		if self._selected_row_indices:
			self._select_rows_by_indices(self._selected_row_indices, select_table=True, zoom_to_points=False)

	def load_positions(self):
		self.selected_info.setText("Loading position data...")
		if self._thread_is_running(self._load_thread):
			self.selected_info.setText("Load already in progress")
			return

		thread = QThread()
		worker = PositionLoadWorker(self.engine.url.render_as_string(hide_password=False))
		worker.moveToThread(thread)
		thread.started.connect(worker.run)
		worker.finished.connect(self._on_positions_loaded)
		worker.error.connect(self._on_positions_error)
		worker.finished.connect(thread.quit)
		worker.error.connect(thread.quit)
		thread.finished.connect(worker.deleteLater)
		thread.finished.connect(thread.deleteLater)
		thread.finished.connect(self._on_load_thread_finished)
		self._load_thread = thread
		self._load_worker = worker
		thread.start()

	def _on_load_thread_finished(self):
		self._load_thread = None
		self._load_worker = None
		if self._pending_close and not self._thread_is_running(self._basemap_thread):
			self._pending_close = False
			self.close()

	def _on_positions_loaded(self, df: pd.DataFrame):
		self.df_positions = df.reset_index(drop=True)
		self._selected_row_index = None
		self._selected_row_indices = []
		self._clear_selection_markers()
		self._set_table_model()
		self._plot_positions()

		restore_state = self._restore_after_reload
		self._restore_after_reload = None
		if restore_state:
			bounds = cast(tuple[float, float, float, float] | None, restore_state.get("bounds"))
			saved_zoom = int(restore_state.get("zoom", self._current_basemap_zoom))
			selected_keys = cast(list[dict[str, Any]] | None, restore_state.get("selected_keys"))

			if bounds:
				self.map_ax.set_xlim(bounds[0], bounds[1])
				self.map_ax.set_ylim(bounds[2], bounds[3])
				self.map_canvas.draw_idle()
				self._refresh_basemap_for_current_view(target_zoom=saved_zoom)

			if selected_keys and not self.df_positions.empty:
				matched_indices: list[int] = []
				for key in selected_keys:
					k_type = str(key.get("pos_type", ""))
					k_id = int(key.get("pos_id", 0))
					if not k_type or k_id <= 0:
						continue
					matched = self.df_positions[
						(self.df_positions["pos_type"] == k_type) &
						(self.df_positions["pos_id"] == k_id)
					]
					if not matched.empty:
						matched_indices.append(int(matched.index[0]))
				if matched_indices:
					self._select_rows_by_indices(matched_indices, select_table=True, zoom_to_points=False)
					return

		self._start_new_entry()

	def _on_positions_error(self, error_message: str):
		logger.error(error_message)
		QMessageBox.warning(self, "Load Failed", error_message)
		self.selected_info.setText("Failed to load positions")

	def _plot_positions(self):
		self.map_ax.clear()
		self._scatter_index_map.clear()
		self._basemap_artist = None
		self._clear_point_labels()

		if self.df_positions.empty:
			self._full_bounds = None
			self.map_ax.set_title("No start/end points available")
			self.map_canvas.draw_idle()
			return

		start_df = self.df_positions[self.df_positions["pos_type"] == "start"]
		end_df = self.df_positions[self.df_positions["pos_type"] == "end"]

		if not start_df.empty:
			sizes = start_df["count"].clip(lower=1).astype(float) * 3.0 + 18.0
			sc_start = self.map_ax.scatter(start_df["x"], start_df["y"], s=sizes, c="tab:blue", alpha=0.85, label="startpos", picker=6, zorder=2)
			self._scatter_index_map[sc_start] = [int(i) for i in start_df.index.tolist()]

		if not end_df.empty:
			sizes = end_df["count"].clip(lower=1).astype(float) * 3.0 + 18.0
			sc_end = self.map_ax.scatter(end_df["x"], end_df["y"], s=sizes, c="tab:red", alpha=0.85, label="endpos", picker=6, zorder=2)
			self._scatter_index_map[sc_end] = [int(i) for i in end_df.index.tolist()]

		self._draw_point_labels()

		xmin = float(self.df_positions["x"].min())
		xmax = float(self.df_positions["x"].max())
		ymin = float(self.df_positions["y"].min())
		ymax = float(self.df_positions["y"].max())
		dx = max(1.0, xmax - xmin)
		dy = max(1.0, ymax - ymin)
		pad_x = dx * 0.06
		pad_y = dy * 0.06
		self._full_bounds = (xmin - pad_x, xmax + pad_x, ymin - pad_y, ymax + pad_y)

		self.map_ax.set_xlim(self._full_bounds[0], self._full_bounds[1])
		self.map_ax.set_ylim(self._full_bounds[2], self._full_bounds[3])
		self.map_ax.set_title("Position Manager: Start/End points")
		self.map_ax.legend(loc="upper right")
		self.map_ax.set_axis_off()

		zoom = self._recommended_zoom_for_bounds(self._full_bounds)
		self._current_basemap_zoom = zoom
		self._start_async_basemap(self._full_bounds, zoom)
		self.map_canvas.draw_idle()

	def _on_pick_point(self, event):
		artist = event.artist
		if artist not in self._scatter_index_map:
			return
		picked = list(event.ind)
		if not picked:
			return
		local_idx = picked[0]
		mapped_rows = self._scatter_index_map.get(artist, [])
		if local_idx >= len(mapped_rows):
			return
		row_index = mapped_rows[local_idx]
		additive = bool(getattr(getattr(event, "mouseevent", None), "key", None) in ("control", "ctrl", "shift"))
		if additive and self._selected_row_indices:
			rows = list(dict.fromkeys(self._selected_row_indices + [row_index]))
			self._select_rows_by_indices(rows, select_table=True, zoom_to_points=False)
		else:
			self._select_rows_by_indices([row_index], select_table=True, zoom_to_points=True)

	def _on_table_selection_changed(self, selected, deselected):
		if self.positions_table.selectionModel() is None or self._table_model is None:
			return
		rows = self.positions_table.selectionModel().selectedRows()
		if not rows:
			self._selected_row_indices = []
			self._clear_selection_markers()
			self.selected_info.setText("Select a start/end point from the map or table")
			return

		source_rows: list[int] = []
		for row in rows:
			source_row = self._table_model.source_row_for_view_row(row.row())
			if source_row is not None:
				source_rows.append(source_row)
		if not source_rows:
			return
		self._select_rows_by_indices(source_rows, select_table=False, zoom_to_points=(len(source_rows) == 1))

	def _clear_selection_markers(self):
		if not self._selected_markers:
			return
		for marker in self._selected_markers:
			try:
				marker.remove()
			except Exception:
				pass
		self._selected_markers = []

	def _clear_point_labels(self):
		if not self._point_label_artists:
			return
		for artist in self._point_label_artists:
			try:
				artist.remove()
			except Exception:
				pass
		self._point_label_artists = []

	def _draw_point_labels(self):
		self._clear_point_labels()
		if not self._show_point_labels or self.df_positions.empty:
			return
		for row in self.df_positions.itertuples(index=False):
			pid = int(getattr(row, "pos_id", 0))
			raw_label = str(getattr(row, "label", "")).strip()
			text_value = f"{pid}: {raw_label}" if raw_label else f"{pid}"
			txt = self.map_ax.annotate(
				text_value,
				(getattr(row, "x"), getattr(row, "y")),
				xytext=(0, -10),
				textcoords="offset points",
				ha="center",
				va="top",
				fontsize=7,
				color="black",
				bbox={"boxstyle": "round,pad=0.15", "facecolor": "white", "alpha": 0.55, "edgecolor": "none"},
				zorder=3,
			)
			self._point_label_artists.append(txt)

	def _on_toggle_labels(self, checked: bool):
		self._show_point_labels = bool(checked)
		self.toggle_labels_btn.setText("Labels on" if checked else "Labels off")
		self._draw_point_labels()
		self.map_canvas.draw_idle()

	def _sort_table_by_similar_latlon(self):
		if self._table_model is None:
			return
		source_rows = self._table_model.source_rows()
		if not source_rows:
			return

		tmp = self.df_positions.loc[source_rows, ["latitude", "longitude"]].copy()
		tmp["lat_bucket"] = tmp["latitude"].round(3)
		tmp["lon_bucket"] = tmp["longitude"].round(3)
		tmp["_src"] = tmp.index
		tmp.sort_values(by=["lat_bucket", "lon_bucket", "latitude", "longitude"], inplace=True, kind="mergesort")
		self._table_model.set_view_order([int(v) for v in tmp["_src"].tolist()])
		if self._selected_row_indices:
			self._select_rows_by_indices(self._selected_row_indices, select_table=True, zoom_to_points=False)

	def _draw_selection_markers(self, row_indices: list[int]):
		self._clear_selection_markers()
		for idx in row_indices:
			if idx not in self.df_positions.index:
				continue
			row = self.df_positions.loc[idx]
			if isinstance(row, pd.DataFrame):
				row = row.iloc[0]
			row_data = cast(dict[str, Any], row.to_dict())
			x = float(row_data.get("x", 0.0))
			y = float(row_data.get("y", 0.0))
			marker = self.map_ax.scatter([x], [y], s=170, facecolors="none", edgecolors="yellow", linewidths=2.0, zorder=4)
			self._selected_markers.append(marker)
		self.map_canvas.draw_idle()

	def _select_rows_by_indices(self, row_indices: list[int], select_table: bool, zoom_to_points: bool):
		clean_rows = [int(i) for i in row_indices if i in self.df_positions.index]
		if not clean_rows:
			return
		clean_rows = list(dict.fromkeys(clean_rows))
		self._selected_row_indices = clean_rows
		self._selected_row_index = clean_rows[0]

		if select_table and self.positions_table.selectionModel() is not None and self._table_model is not None:
			selection_model = self.positions_table.selectionModel()
			selection_model.clearSelection()
			for idx, source_row in enumerate(clean_rows):
				view_row = self._table_model.view_row_for_source_row(source_row)
				if view_row is None:
					continue
				model_index = self.positions_table.model().index(view_row, 0)
				flags = QItemSelectionModel.SelectionFlag.Select | QItemSelectionModel.SelectionFlag.Rows
				selection_model.select(model_index, flags)
				if idx == 0:
					self.positions_table.scrollTo(model_index)

		rows_data = []
		for ridx in clean_rows:
			row = self.df_positions.loc[ridx]
			if isinstance(row, pd.DataFrame):
				row = row.iloc[0]
			rows_data.append(cast(dict[str, Any], row.to_dict()))

		if len(rows_data) == 1:
			row_data = rows_data[0]
			self.pos_type_combo.setCurrentText(str(row_data.get("pos_type", "start")))
			self.pos_id_spin.setValue(int(row_data.get("pos_id", 1)))
			self.lat_spin.setValue(float(row_data.get("latitude", 0.0)))
			self.lon_spin.setValue(float(row_data.get("longitude", 0.0)))
			self.count_spin.setValue(int(row_data.get("count", 0)))
			self.label_edit.setText(str(row_data.get("label", "")))
			self.selected_info.setText(
				f"Selected {str(row_data.get('pos_type', ''))} point #{int(row_data.get('pos_id', 0))}  |  "
				f"lat={float(row_data.get('latitude', 0.0)):.6f}, lon={float(row_data.get('longitude', 0.0)):.6f}, count={int(row_data.get('count', 0))}"
			)
			if zoom_to_points:
				self._zoom_to_point(float(row_data.get("x", 0.0)), float(row_data.get("y", 0.0)))
		else:
			labels = [str(row_entry.get("label", "")) for row_entry in rows_data]
			common_label = labels[0] if labels and all(label_value == labels[0] for label_value in labels) else ""
			self.label_edit.setText(common_label)
			self.selected_info.setText(f"Selected {len(rows_data)} points. Edit label and click 'Apply label to selected'.")

		self._draw_selection_markers(clean_rows)

	def _select_row_by_index(self, row_index: int, select_table: bool, zoom_to_point: bool):
		self._select_rows_by_indices([row_index], select_table=select_table, zoom_to_points=zoom_to_point)

	def _draw_selection_marker(self, x: float, y: float):
		# Backward compatible wrapper.
		self._draw_selection_markers(self._selected_row_indices or [self._selected_row_index] if self._selected_row_index is not None else [])

	def _zoom_to_point(self, x: float, y: float):
		if self._full_bounds is None:
			return
		xmin, xmax, ymin, ymax = self._full_bounds
		target_span = max(self._min_zoom_span_m, max(xmax - xmin, ymax - ymin) * 0.06)
		cx0, cx1 = self.map_ax.get_xlim()
		cy0, cy1 = self.map_ax.get_ylim()
		current_span = max(abs(cx1 - cx0), abs(cy1 - cy0))

		# If user is already zoomed in further than the auto target,
		# keep their current zoom level and only recenter to selected point.
		span = current_span if current_span <= target_span else target_span
		span = max(self._min_zoom_span_m, span)
		self.map_ax.set_xlim(x - span / 2.0, x + span / 2.0)
		self.map_ax.set_ylim(y - span / 2.0, y + span / 2.0)
		self.map_canvas.draw_idle()
		self._refresh_basemap_for_current_view()

	def _zoom_in(self):
		x0, x1 = self.map_ax.get_xlim()
		y0, y1 = self.map_ax.get_ylim()
		cx = (x0 + x1) / 2.0
		cy = (y0 + y1) / 2.0
		span = max(abs(x1 - x0), abs(y1 - y0)) * 0.5
		span = max(self._min_zoom_span_m, span * 0.65)
		self.map_ax.set_xlim(cx - span / 2.0, cx + span / 2.0)
		self.map_ax.set_ylim(cy - span / 2.0, cy + span / 2.0)
		self.map_canvas.draw_idle()
		self._refresh_basemap_for_current_view(target_zoom=self._current_basemap_zoom + 1)

	def _zoom_out(self):
		x0, x1 = self.map_ax.get_xlim()
		y0, y1 = self.map_ax.get_ylim()
		cx = (x0 + x1) / 2.0
		cy = (y0 + y1) / 2.0
		current_span = max(abs(x1 - x0), abs(y1 - y0))
		span = current_span * 1.45
		if self._full_bounds is not None:
			full_span = max(abs(self._full_bounds[1] - self._full_bounds[0]), abs(self._full_bounds[3] - self._full_bounds[2]))
			span = min(full_span, span)
		span = max(self._min_zoom_span_m, span)
		self.map_ax.set_xlim(cx - span / 2.0, cx + span / 2.0)
		self.map_ax.set_ylim(cy - span / 2.0, cy + span / 2.0)
		self.map_canvas.draw_idle()
		self._refresh_basemap_for_current_view(target_zoom=self._current_basemap_zoom - 1)

	def _zoom_full(self):
		if self._full_bounds is None:
			return
		self.map_ax.set_xlim(self._full_bounds[0], self._full_bounds[1])
		self.map_ax.set_ylim(self._full_bounds[2], self._full_bounds[3])
		self.map_canvas.draw_idle()
		self._refresh_basemap_for_current_view(target_zoom=self._recommended_zoom_for_bounds(self._full_bounds))

	def _start_new_entry(self):
		self._selected_row_index = None
		self._selected_row_indices = []
		self.pos_type_combo.setCurrentText("start")
		self.pos_id_spin.setValue(1)
		self.lat_spin.setValue(0.0)
		self.lon_spin.setValue(0.0)
		self.count_spin.setValue(0)
		self.label_edit.clear()
		self._clear_selection_markers()
		self.selected_info.setText("Create new start/end position entry")

	def apply_label_to_selected(self):
		selected_rows = self._selected_row_indices[:]
		if not selected_rows and self._selected_row_index is not None:
			selected_rows = [self._selected_row_index]
		if not selected_rows:
			QMessageBox.information(self, "No Selection", "Select one or more entries first.")
			return

		label = self.label_edit.text().strip()
		label_value = label if label else None
		rows_data: list[dict[str, Any]] = []
		for ridx in selected_rows:
			if ridx not in self.df_positions.index:
				continue
			row = self.df_positions.loc[ridx]
			if isinstance(row, pd.DataFrame):
				row = row.iloc[0]
			rows_data.append(cast(dict[str, Any], row.to_dict()))

		if not rows_data:
			QMessageBox.information(self, "No Selection", "No valid rows selected.")
			return

		try:
			with self.engine.begin() as conn:
				for row_data in rows_data:
					pos_type = str(row_data.get("pos_type", ""))
					pos_id = int(row_data.get("pos_id", 0))
					if pos_id <= 0 or pos_type not in ("start", "end"):
						continue
					table, id_col, _, _ = self._table_info(pos_type)
					conn.execute(
						text(f'UPDATE {table} SET label = :label WHERE {id_col} = :pos_id'),
						{"label": label_value, "pos_id": pos_id},
					)
		except Exception as e:
			logger.error(f"Failed to apply label to selected points: {e} ({type(e)})")
			QMessageBox.warning(self, "Update Failed", f"Could not apply label:\n{e}")
			return

		self._restore_after_reload = {
			"bounds": self._current_view_bounds(),
			"zoom": self._current_basemap_zoom,
			"selected_keys": [
				{"pos_type": str(r.get("pos_type", "")), "pos_id": int(r.get("pos_id", 0))}
				for r in rows_data
			],
		}
		self._skip_sort_once = True
		# QMessageBox.information(self, "Updated", f"Applied label to {len(rows_data)} selected points.")
		self.load_positions()

	def save_entry(self):
		pos_type = self.pos_type_combo.currentText()
		pos_id = int(self.pos_id_spin.value())
		lat = float(self.lat_spin.value())
		lon = float(self.lon_spin.value())
		count = int(self.count_spin.value())
		label = self.label_edit.text().strip()
		label_value = label if label else None

		table, id_col, lat_col, lon_col = self._table_info(pos_type)
		old_type = None
		old_id = None
		if self._selected_row_index is not None and self._selected_row_index in self.df_positions.index:
			old_row = self.df_positions.loc[self._selected_row_index]
			if isinstance(old_row, pd.DataFrame):
				old_row = old_row.iloc[0]
			old_data = cast(dict[str, Any], old_row.to_dict())
			old_type = str(old_data.get("pos_type", ""))
			old_id = int(old_data.get("pos_id", 0))

		try:
			with self.engine.begin() as conn:
				# If entry changed identity/type, remove previous row first.
				if old_type and old_id and (old_type != pos_type or old_id != pos_id):
					old_table, old_id_col, _, _ = self._table_info(old_type)
					conn.execute(text(f'DELETE FROM {old_table} WHERE {old_id_col} = :pos_id'), {"pos_id": old_id})

				# Replace existing row for this key.
				conn.execute(text(f'DELETE FROM {table} WHERE {id_col} = :pos_id'), {"pos_id": pos_id})
				conn.execute(
					text(
						f'INSERT INTO {table} ({id_col}, {lat_col}, {lon_col}, count, label) '
						f'VALUES (:pos_id, :lat, :lon, :count, :label)'
					),
					{"pos_id": pos_id, "lat": lat, "lon": lon, "count": count, "label": label_value},
				)
		except Exception as e:
			logger.error(f"Failed to save position entry: {e} ({type(e)})")
			QMessageBox.warning(self, "Save Failed", f"Could not save entry:\n{e}")
			return

		# Preserve current viewport/zoom and the saved row identity through reload.
		self._restore_after_reload = {
			"bounds": self._current_view_bounds(),
			"zoom": self._current_basemap_zoom,
			"selected_keys": [{"pos_type": pos_type, "pos_id": pos_id}],
		}
		self._skip_sort_once = True
		# QMessageBox.information(self, "Saved", "Position entry saved.")
		self.load_positions()

	def delete_entry(self):
		if self._selected_row_index is None or self._selected_row_index not in self.df_positions.index:
			QMessageBox.information(self, "No Selection", "Select an entry first.")
			return
		row = self.df_positions.loc[self._selected_row_index]
		if isinstance(row, pd.DataFrame):
			row = row.iloc[0]
		row_data = cast(dict[str, Any], row.to_dict())
		pos_type = str(row_data.get("pos_type", ""))
		pos_id = int(row_data.get("pos_id", 0))
		table, id_col, _, _ = self._table_info(pos_type)

		confirm = QMessageBox.question(
			self,
			"Delete Entry",
			f"Delete {pos_type} entry #{pos_id}?",
			QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
			QMessageBox.StandardButton.No,
		)
		if confirm != QMessageBox.StandardButton.Yes:
			return

		try:
			with self.engine.begin() as conn:
				conn.execute(text(f'DELETE FROM {table} WHERE {id_col} = :pos_id'), {"pos_id": pos_id})
		except Exception as e:
			logger.error(f"Failed to delete position entry: {e} ({type(e)})")
			QMessageBox.warning(self, "Delete Failed", f"Could not delete entry:\n{e}")
			return

		QMessageBox.information(self, "Deleted", "Entry deleted.")
		self.load_positions()

	def closeEvent(self, event: QCloseEvent):
		load_running = self._thread_is_running(self._load_thread)
		basemap_running = self._thread_is_running(self._basemap_thread)
		if load_running or basemap_running:
			self._pending_close = True
			self.hide()
			event.ignore()
			return
		super().closeEvent(event)

def format_duration(seconds):
	if pd.isna(seconds):
		return ""
	seconds = int(seconds)
	if seconds < 60:
		return f"{seconds} s"
	elif seconds < 3600:
		minutes = seconds // 60
		secs = seconds % 60
		return f"{minutes}:{secs:02d} m"
	else:
		hours = seconds // 3600
		minutes = (seconds % 3600) // 60
		return f"{hours}h {minutes}m"

class MainWindow(QMainWindow):
	def __init__(self, args):
		super().__init__()
		self.args = args
		self.setWindowTitle("TorqFiles Viewer")
		# Set up SQLAlchemy session
		# session = get_engine_session(args)
		# self.engine = create_engine(args.dburl)
		if self.args.dbmode == 'psql':
			dburl = f"postgresql://{args.dbuser}:{args.dbpass}@{args.dbhost}/{args.dbname}"
		elif self.args.dbmode == 'sqlite':
			dburl = f"sqlite:///{args.dbfile}"
		else:
			dburl = ''
		# engine = create_engine(dburl)
		self.engine = create_engine(dburl)
		database_init(self.engine)
		self._torqlogs_norm_to_actual = self._build_torqlogs_column_map()
		self._resolved_torqlogs_columns = {
			name: actual
			for name in ['latitude', 'longitude', 'speedobdkmh']
			if (actual := self._resolve_actual_torqlogs_column(name)) is not None
		}
		self._trip_plot_cache: dict[tuple[int, str], dict[str, list]] = {}
		self._trip_geo_cache: dict[int, dict[str, list]] = {}
		self._selection_stats_cache: dict[tuple[int, ...], tuple[dict, dict[str, dict[str, float]]]] = {}
		self._map_cache_version = "v2"
		self._current_colormap = 'Set1'
		self._dot_size_scale = 1.0
		self._plot_refresh_timer = QTimer(self)
		self._plot_refresh_timer.setSingleShot(True)
		self._plot_refresh_timer.timeout.connect(self.refresh_plot)
		self._basemap_request_id = 0
		self._basemap_request_context: dict[int, dict[str, object]] = {}
		self._basemap_thread: QThread | None = None
		self._basemap_worker: BasemapWorker | None = None
		self._show_all_start_end_points = False
		self._start_end_overlay_artists: list[Any] = []
		self._start_end_overlay_labels: list[Any] = []
		self._initial_trips_thread: QThread | None = None
		self._initial_trips_worker: TripListWorker | None = None
		self._active_threads: set[QThread] = set()
		self._position_manager_window: PositionManagerWindow | None = None
		self.Session = sessionmaker(bind=self.engine)
		self.session = self.Session()
		self._ensure_map_cache_schema()

		# Set up UI
		splitter = QSplitter(Qt.Orientation.Horizontal)
		self.table = QTableView()
		self.map_canvas = MapCanvas()
		self.timeseries_canvas = TimeSeriesCanvas()
		logger.debug(f"Resolved torqlogs columns: {self._resolved_torqlogs_columns}")

		# Zoom control
		zoom_widget = QWidget()
		zoom_layout = QHBoxLayout(zoom_widget)
		zoom_label = QLabel("Zoom:")
		self.zoom_combo = QComboBox()
		zoom_levels = [str(z) for z in range(10, 19)]
		self.zoom_combo.addItems(zoom_levels)
		self.zoom_combo.setCurrentText('10')
		self.zoom_combo.setFixedWidth(60)
		self.zoom_combo.setMaximumHeight(25)
		self.zoom_combo.currentTextChanged.connect(self.on_zoom_changed)
		dot_size_label = QLabel("Dot size:")
		self.toggle_all_start_end_btn = QPushButton("All points off")
		self.toggle_all_start_end_btn.setCheckable(True)
		self.toggle_all_start_end_btn.setChecked(False)
		self.toggle_all_start_end_btn.setFixedHeight(24)
		self.toggle_all_start_end_btn.toggled.connect(self._on_toggle_all_start_end)
		self.dot_size_slider = QSlider(Qt.Orientation.Horizontal)
		self.dot_size_slider.setMinimum(25)
		self.dot_size_slider.setMaximum(300)
		self.dot_size_slider.setValue(100)
		self.dot_size_slider.setSingleStep(5)
		self.dot_size_slider.setPageStep(25)
		self.dot_size_slider.setFixedWidth(140)
		self.dot_size_slider.valueChanged.connect(self.on_dot_size_changed)
		self.dot_size_value_label = QLabel("1.00x")
		self.dot_size_value_label.setFixedWidth(44)
		zoom_layout.addWidget(zoom_label)
		zoom_layout.addWidget(self.zoom_combo)
		zoom_layout.addWidget(dot_size_label)
		zoom_layout.addWidget(self.dot_size_slider)
		zoom_layout.addWidget(self.dot_size_value_label)
		zoom_layout.addWidget(self.toggle_all_start_end_btn)
		zoom_layout.addStretch()
		zoom_layout.setSpacing(10)
		zoom_layout.setContentsMargins(10, 3, 10, 3)
		zoom_widget.setMaximumHeight(35)

		# Metric list (replaces QComboBox)
		metric_panel = QWidget()
		metric_panel_layout = QVBoxLayout(metric_panel)
		metric_panel_layout.setContentsMargins(2, 2, 2, 2)
		metric_title = QLabel("Metrics")
		metric_title_font = QFont()
		metric_title_font.setPointSize(9)
		metric_title_font.setBold(True)
		metric_title.setFont(metric_title_font)
		self.metric_list = QListWidget()
		self.metric_list.setSelectionMode(QListWidget.SelectionMode.ExtendedSelection)
		mono_font = QFont("Monospace", 8)
		self.metric_list.setFont(mono_font)
		if self._resolve_actual_torqlogs_column('speedobdkmh'):
			self.metric_list.addItem(QListWidgetItem('speedobdkmh'))
		self.metric_list.itemSelectionChanged.connect(self.on_metric_selection_changed)
		metric_panel_layout.addWidget(metric_title)
		metric_panel_layout.addWidget(self.metric_list)

		# Stats panel (scrollable)
		stats_panel = QFrame()
		stats_layout = QVBoxLayout(stats_panel)
		stats_layout.setContentsMargins(4, 4, 4, 4)
		stats_title = QLabel("Selected Trip Stats")
		stats_title_font = QFont()
		stats_title_font.setPointSize(10)
		stats_title_font.setBold(True)
		stats_title.setFont(stats_title_font)
		self.stats_label = QLabel("No trip selected")
		self.stats_label.setWordWrap(True)
		self.stats_label.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
		stats_scroll = QScrollArea()
		stats_scroll.setWidget(self.stats_label)
		stats_scroll.setWidgetResizable(True)
		stats_layout.addWidget(stats_title)
		stats_layout.addWidget(stats_scroll)

		# Right panel: plots on top, metric list + stats below, zoom at bottom
		right_panel = QWidget()
		right_layout = QVBoxLayout(right_panel)
		right_layout.setContentsMargins(0, 0, 0, 0)
		right_layout.setSpacing(2)

		plots_splitter = QSplitter(Qt.Orientation.Horizontal)
		plots_splitter.addWidget(self.map_canvas)
		plots_splitter.addWidget(self.timeseries_canvas)
		plots_splitter.setSizes([550, 450])

		lower_splitter = QSplitter(Qt.Orientation.Horizontal)
		lower_splitter.addWidget(metric_panel)
		lower_splitter.addWidget(stats_panel)
		lower_splitter.setSizes([200, 600])

		right_layout.addWidget(plots_splitter, stretch=6)
		right_layout.addWidget(lower_splitter, stretch=4)
		right_layout.addWidget(zoom_widget, stretch=0)

		# Make font a little smaller
		font = QFont()
		font.setPointSize(9)
		self.table.setFont(font)

		splitter.addWidget(self.table)
		splitter.addWidget(right_panel)
		splitter.setSizes([150, 600])  # Give more space to the map panel

		container = QWidget()
		layout = QVBoxLayout(container)
		layout.addWidget(splitter)
		self.setCentralWidget(container)

		# Render immediately, then hydrate data/metrics after first paint.
		empty_df = pd.DataFrame(columns=['fileid', 'trip_distance', 'tripdate', 'time'])
		empty_df.index.name = 'id'
		self._set_table_model(empty_df)
		QTimer.singleShot(0, self._start_async_initial_trips_load)
		QTimer.singleShot(0, self._populate_metric_columns)
		self._create_menu_bar()
		logger.debug("MainWindow initialized and UI set up")

		# self.table_model = PandasModel(self.df_files)
		# self.table.setModel(self.table_model)
		# self.table.setSortingEnabled(True)
		# # self.table.setSelectionBehavior(self.table.SelectRows)
		# self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
		# self.table.selectionModel().selectionChanged.connect(self.on_row_selected)

	def __repr__(self):
		return f"<MainWindow with {len(self.df_trips)} trips loaded>"

	def _create_menu_bar(self):
		menu_bar = self.menuBar()

		# File menu
		file_menu = menu_bar.addMenu("&File")
		open_action = QAction("&Open Database...", self)
		open_action.setShortcut("Ctrl+O")
		open_action.triggered.connect(self._open_database)
		file_menu.addAction(open_action)
		file_menu.addSeparator()
		export_action = QAction("&Export Stats...", self)
		export_action.triggered.connect(self._export_stats)
		file_menu.addAction(export_action)
		file_menu.addSeparator()
		exit_action = QAction("E&xit", self)
		exit_action.setShortcut("Ctrl+Q")
		exit_action.triggered.connect(self.close)
		file_menu.addAction(exit_action)

		# View menu — colormap submenu
		view_menu = menu_bar.addMenu("&View")
		colormap_menu = view_menu.addMenu("&Colormap")
		qualitative_maps = ['Set1', 'tab10', 'tab20', 'Dark2', 'Pastel1', 'Pastel2', 'Set2', 'Set3', 'Accent']
		sequential_maps = ['viridis', 'plasma', 'inferno', 'magma', 'Blues', 'Greens', 'Reds', 'YlOrRd']
		self._colormap_actions: dict[str, QAction] = {}
		for cmap_name in qualitative_maps + sequential_maps:
			action = QAction(cmap_name, self)
			action.setCheckable(True)
			action.setChecked(cmap_name == self._current_colormap)
			action.triggered.connect(lambda checked, name=cmap_name: self._set_colormap(name))
			colormap_menu.addAction(action)
			self._colormap_actions[cmap_name] = action

		# Cache menu
		cache_menu = menu_bar.addMenu("&Cache")
		clear_map_cache_action = QAction("Clear &Map Image Cache", self)
		clear_map_cache_action.triggered.connect(self._clear_map_image_cache)
		cache_menu.addAction(clear_map_cache_action)

		# Tools menu
		tools_menu = menu_bar.addMenu("&Tools")
		position_manager_action = QAction("&Position manager", self)
		position_manager_action.triggered.connect(self._open_position_manager)
		tools_menu.addAction(position_manager_action)

	def _open_position_manager(self):
		if self._position_manager_window is None:
			self._position_manager_window = PositionManagerWindow(self.engine, self)
		self._position_manager_window.show()
		self._position_manager_window.raise_()
		self._position_manager_window.activateWindow()

	def _open_database(self):
		path, _ = QFileDialog.getOpenFileName(self, "Open Database", "", "SQLite Database (*.db);;All Files (*)")
		if path:
			QMessageBox.information(self, "Open Database",
				f"Selected: {path}\n\nRestart with --dbfile \"{path}\" to switch databases.")

	def _export_stats(self):
		path, _ = QFileDialog.getSaveFileName(self, "Export Stats", "trip_stats.txt", "Text Files (*.txt);;All Files (*)")
		if path:
			try:
				with open(path, 'w', encoding='utf-8') as f:
					f.write(self.stats_label.text())
				logger.info(f"Stats exported to {path}")
			except Exception as e:
				logger.error(f"Export failed: {e}")

	def _clear_map_image_cache(self):
		confirm = QMessageBox.question(
			self,
			"Clear Cache",
			"Clear all rows from mapimagecache?\n\nThis will force map images to be regenerated.",
			QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
			QMessageBox.StandardButton.No,
		)
		if confirm != QMessageBox.StandardButton.Yes:
			return

		try:
			with self.engine.begin() as conn:
				conn.execute(text("DELETE FROM mapimagecache"))
			self._trip_plot_cache.clear()
			self._trip_geo_cache.clear()
			self._selection_stats_cache.clear()
			self._plot_refresh_timer.start(50)
			logger.info("Cleared mapimagecache and invalidated in-memory plot/stat caches")
			QMessageBox.information(self, "Cache Cleared", "Map image cache was cleared successfully.")
		except Exception as e:
			logger.error(f"Failed to clear mapimagecache: {e} ({type(e)})")
			QMessageBox.warning(self, "Cache Clear Failed", f"Could not clear mapimagecache:\n{e}")

	def _set_colormap(self, colormap_name: str):
		self._current_colormap = colormap_name
		for name, action in self._colormap_actions.items():
			action.setChecked(name == colormap_name)
		self._plot_refresh_timer.start(200)

	def on_zoom_changed(self, zoom_level):
		"""Called when user changes map zoom"""
		# Debounce zoom updates to avoid blocking UI with repeated basemap fetches.
		self._plot_refresh_timer.start(300)

	def on_dot_size_changed(self, value: int):
		"""Called when user adjusts map dot size multiplier."""
		self._dot_size_scale = float(value) / 100.0
		self.dot_size_value_label.setText(f"{self._dot_size_scale:.2f}x")
		self._plot_refresh_timer.start(120)

	def on_metric_selection_changed(self):
		"""Called when user changes the metric selection in the list."""
		if self._get_selected_metrics():
			self._plot_refresh_timer.start(200)

	def _on_toggle_all_start_end(self, checked: bool):
		self._show_all_start_end_points = bool(checked)
		self.toggle_all_start_end_btn.setText("All points on" if checked else "All points off")
		self._plot_refresh_timer.start(120)

	@staticmethod
	def _lonlat_to_web_mercator(lon: float, lat: float) -> tuple[float, float]:
		lat_clamped = max(-85.05112878, min(85.05112878, float(lat)))
		x = float(lon) * 20037508.34 / 180.0
		y = np.log(np.tan(np.pi / 4.0 + np.deg2rad(lat_clamped) / 2.0)) * 6378137.0
		return float(x), float(y)

	@staticmethod
	def _web_mercator_to_lonlat(x: float, y: float) -> tuple[float, float]:
		lon = (float(x) / 20037508.34) * 180.0
		lat = np.rad2deg(2.0 * np.arctan(np.exp(float(y) / 6378137.0)) - np.pi / 2.0)
		return float(lon), float(lat)

	def _clear_start_end_overlays(self):
		for artist in self._start_end_overlay_artists:
			try:
				artist.remove()
			except Exception:
				pass
		for lbl in self._start_end_overlay_labels:
			try:
				lbl.remove()
			except Exception:
				pass
		self._start_end_overlay_artists = []
		self._start_end_overlay_labels = []

	def _load_selected_file_start_end_points(self, fileids: list[int]) -> list[dict[str, Any]]:
		if not fileids:
			return []
		placeholders = ", ".join(f":fid{idx}" for idx in range(len(fileids)))
		params = {f"fid{idx}": int(fid) for idx, fid in enumerate(fileids)}
		q = text(
			f"""
			SELECT tf.fileid AS fileid, 'start' AS pos_type, sp.startid AS pos_id, sp.latstart AS lat, sp.lonstart AS lon, sp.label AS label
			FROM torqfiles tf
			LEFT JOIN startpos sp ON tf.startid = sp.startid
			WHERE tf.fileid IN ({placeholders})
			UNION ALL
			SELECT tf.fileid AS fileid, 'end' AS pos_type, ep.endid AS pos_id, ep.latend AS lat, ep.lonend AS lon, ep.label AS label
			FROM torqfiles tf
			LEFT JOIN endpos ep ON tf.endid = ep.endid
			WHERE tf.fileid IN ({placeholders})
			"""
		)
		with self.engine.connect() as conn:
			rows = conn.execute(q, params).mappings().all()
		result = []
		for row in rows:
			if row.get("lat") is None or row.get("lon") is None or row.get("pos_id") is None:
				continue
			result.append(dict(row))
		return result

	def _load_visible_start_end_points(self, bounds_mercator: tuple[float, float, float, float]) -> list[dict[str, Any]]:
		xmin, xmax, ymin, ymax = bounds_mercator
		lon_min, lat_min = self._web_mercator_to_lonlat(xmin, ymin)
		lon_max, lat_max = self._web_mercator_to_lonlat(xmax, ymax)
		lat_lo, lat_hi = (min(lat_min, lat_max), max(lat_min, lat_max))
		lon_lo, lon_hi = (min(lon_min, lon_max), max(lon_min, lon_max))
		q = text(
			"""
			SELECT 'start' AS pos_type, startid AS pos_id, latstart AS lat, lonstart AS lon, label
			FROM startpos
			WHERE latstart BETWEEN :lat_lo AND :lat_hi
			  AND lonstart BETWEEN :lon_lo AND :lon_hi
			UNION ALL
			SELECT 'end' AS pos_type, endid AS pos_id, latend AS lat, lonend AS lon, label
			FROM endpos
			WHERE latend BETWEEN :lat_lo AND :lat_hi
			  AND lonend BETWEEN :lon_lo AND :lon_hi
			"""
		)
		with self.engine.connect() as conn:
			rows = conn.execute(q, {
				"lat_lo": lat_lo,
				"lat_hi": lat_hi,
				"lon_lo": lon_lo,
				"lon_hi": lon_hi,
			}).mappings().all()
		return [dict(r) for r in rows if r.get("lat") is not None and r.get("lon") is not None and r.get("pos_id") is not None]

	def _overlay_start_end_points(self, fileids: list[int], bounds_mercator: tuple[float, float, float, float] | None):
		self._clear_start_end_overlays()
		selected_points = self._load_selected_file_start_end_points(fileids)
		all_points: list[dict[str, Any]] = []
		if self._show_all_start_end_points and bounds_mercator is not None:
			all_points = self._load_visible_start_end_points(bounds_mercator)

		seen: set[tuple[str, int]] = set()
		merged: list[tuple[dict[str, Any], bool]] = []
		for p in selected_points:
			key = (str(p.get("pos_type", "")), int(p.get("pos_id", 0)))
			if key in seen:
				continue
			seen.add(key)
			merged.append((p, True))
		for p in all_points:
			key = (str(p.get("pos_type", "")), int(p.get("pos_id", 0)))
			if key in seen:
				continue
			seen.add(key)
			merged.append((p, False))

		for point, is_selected_file in merged:
			lat = float(point.get("lat", 0.0))
			lon = float(point.get("lon", 0.0))
			x, y = self._lonlat_to_web_mercator(lon, lat)
			pos_type = str(point.get("pos_type", ""))
			pos_id = int(point.get("pos_id", 0))
			label_text = str(point.get("label", "")).strip()
			prefix = "S" if pos_type == "start" else "E"
			full_label = f"{prefix}{pos_id}: {label_text}" if label_text else f"{prefix}{pos_id}"
			color = "limegreen" if pos_type == "start" else "darkorange"
			alpha = 1.0 if is_selected_file else 0.55
			size = 90 if is_selected_file else 48
			artist = self.map_canvas.ax.scatter([x], [y], s=size, c=color, marker="D", edgecolors="black", linewidths=0.5, alpha=alpha, zorder=4)
			self._start_end_overlay_artists.append(artist)
			label_artist = self.map_canvas.ax.annotate(
				full_label,
				(x, y),
				xytext=(3, -10),
				textcoords="offset points",
				ha="left",
				va="top",
				fontsize=7,
				color="black",
				bbox={"boxstyle": "round,pad=0.12", "facecolor": "white", "alpha": 0.6, "edgecolor": "none"},
				zorder=5,
			)
			self._start_end_overlay_labels.append(label_artist)

	def _get_selected_metrics(self) -> list[str]:
		"""Return all currently selected selectable metric names."""
		return [
			item.text() for item in self.metric_list.selectedItems()
			if item.flags() & Qt.ItemFlag.ItemIsSelectable
		]

	def _get_selected_metric(self) -> str:
		"""Return the first selected metric (used for map coloring)."""
		metrics = self._get_selected_metrics()
		return metrics[0] if metrics else 'speedobdkmh'

	def _build_torqlogs_column_map(self) -> dict[str, str]:
		inspector = inspect(self.engine)
		actual_columns = [str(col["name"]) for col in inspector.get_columns("torqlogs")]
		return {_normalize_col_name(col): col for col in actual_columns}

	def _set_table_model(self, df: pd.DataFrame):
		self.df_trips = df
		self.table_model = PandasModel(self.df_trips)
		self.table.setModel(self.table_model)
		self.table.setSortingEnabled(True)
		self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
		# Allow Ctrl/Shift multi-select so multiple trips can be plotted together.
		self.table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
		self.table.selectionModel().selectionChanged.connect(self.on_row_selected)
		self.table.horizontalHeader().setStretchLastSection(True)
		logger.debug(f"Table model set with {len(df)} rows and {len(df.columns)} columns")

	def _ensure_map_cache_schema(self):
		inspector = inspect(self.engine)
		existing_columns = {str(col["name"]).lower() for col in inspector.get_columns("mapimagecache")}
		alter_statements = {
			"ext_west": 'ALTER TABLE mapimagecache ADD COLUMN ext_west DOUBLE PRECISION',
			"ext_east": 'ALTER TABLE mapimagecache ADD COLUMN ext_east DOUBLE PRECISION',
			"ext_south": 'ALTER TABLE mapimagecache ADD COLUMN ext_south DOUBLE PRECISION',
			"ext_north": 'ALTER TABLE mapimagecache ADD COLUMN ext_north DOUBLE PRECISION',
		}
		with self.engine.begin() as conn:
			for col_name, sql_stmt in alter_statements.items():
				if col_name not in existing_columns:
					conn.execute(text(sql_stmt))

	def _start_async_initial_trips_load(self):
		thread = QThread()
		worker = TripListWorker(self.engine.url.render_as_string(hide_password=False))
		worker.moveToThread(thread)

		thread.started.connect(worker.run)
		worker.finished.connect(self._on_initial_trips_loaded)
		worker.error.connect(self._on_initial_trips_error)
		worker.finished.connect(thread.quit)
		worker.error.connect(thread.quit)
		thread.finished.connect(worker.deleteLater)
		thread.finished.connect(thread.deleteLater)
		thread.finished.connect(lambda t=thread: self._active_threads.discard(t))

		self._initial_trips_worker = worker
		self._initial_trips_thread = thread
		self._active_threads.add(thread)
		thread.start()

	def _on_initial_trips_loaded(self, df_trips: pd.DataFrame):
		logger.debug(f"Loaded {len(df_trips)} trips from database")
		df_trips['tripdate'] = pd.to_datetime(df_trips['tripdate'], errors='coerce')
		df_trips['tripdate'] = df_trips['tripdate'].dt.strftime('%Y-%m-%d %H:%M')
		df_trips['time'] = df_trips['time'].apply(format_duration)
		df_trips['trip_distance'] = df_trips['trip_distance'].apply(lambda x: f"{x/1000:.1f} km" if pd.notna(x) else "")
		df_trips.set_index('id', inplace=True)
		self._set_table_model(df_trips)

	def _on_initial_trips_error(self, error_message: str):
		logger.error(error_message)

	def _populate_metric_columns(self):
		metric_columns = self._get_metric_columns_with_valid_data()
		if not metric_columns:
			return

		prev_selected: set[str] = {
			item.text() for item in self.metric_list.selectedItems()
			if item.flags() & Qt.ItemFlag.ItemIsSelectable
		}

		self.metric_list.blockSignals(True)
		self.metric_list.clear()

		grouped = group_metrics_by_category(metric_columns)
		first_selectable: QListWidgetItem | None = None
		for cat, cat_metrics in grouped.items():
			header = QListWidgetItem(f"── {cat.value} ──")
			header.setFlags(Qt.ItemFlag.NoItemFlags)
			header.setForeground(Qt.GlobalColor.darkGray)
			self.metric_list.addItem(header)
			for display_name, unit, orig_name in cat_metrics:
				item = QListWidgetItem(orig_name)
				item.setToolTip(f"{display_name} ({unit})" if unit else display_name)
				self.metric_list.addItem(item)
				if first_selectable is None:
					first_selectable = item
				if orig_name in prev_selected:
					item.setSelected(True)

		if not any(item.isSelected() for item in self.metric_list.findItems('*', Qt.MatchFlag.MatchWildcard) if item.flags() & Qt.ItemFlag.ItemIsSelectable):
			if first_selectable:
				first_selectable.setSelected(True)

		self.metric_list.blockSignals(False)
		logger.debug(f"Populated metric list with {len(metric_columns)} metrics")

	def _resolve_actual_torqlogs_column(self, requested_column: str) -> str | None:
		return self._torqlogs_norm_to_actual.get(_normalize_col_name(requested_column))

	def _get_metric_columns_with_valid_data(self) -> list[str]:
		# Fast startup path: list metrics that exist in torqlogs without full-table scans.
		requested = sorted(dataschema.keys())
		valid_metrics: list[str] = []
		for req in requested:
			actual = self._resolve_actual_torqlogs_column(req)
			if actual:
				valid_metrics.append(req)
		return valid_metrics

	def refresh_plot(self):
		"""Refresh the current plot with selected rows"""
		# Get currently selected rows and replot
		rows = sorted(set(index.row() for index in self.table.selectionModel().selectedRows()))
		if rows:
			logger.debug(f"refresh_plot triggered with {len(rows)} selected row(s): {rows[:5]}{'...' if len(rows) > 5 else ''}")
			self._plot_for_rows(rows)

	def _get_selected_fileids(self, rows):
		if not rows:
			return []
		if 'fileid' in self.df_trips.columns:
			return [int(k) for k in self.df_trips.iloc[rows]['fileid'].tolist() if pd.notna(k)]
		return []

	def _load_trip_geo_data(self, fileid: int) -> dict[str, list] | None:
		if fileid in self._trip_geo_cache:
			return self._trip_geo_cache[fileid]

		lat_col = self._resolved_torqlogs_columns.get('latitude')
		lon_col = self._resolved_torqlogs_columns.get('longitude')
		time_col = (self._resolve_actual_torqlogs_column('gpstime')
					or self._resolve_actual_torqlogs_column('devicetime'))
		if not (lat_col and lon_col):
			return None

		time_select = f', "{time_col}" AS metric_time' if time_col else ''
		order_col = f'"{time_col}"' if time_col else 'id'
		q = (
			f'SELECT "{lon_col}" AS longitude, "{lat_col}" AS latitude{time_select} '
			f'FROM torqlogs WHERE fileid = {int(fileid)} ORDER BY {order_col}'
		)
		df_geo = pd.read_sql(q, self.engine)
		if df_geo.empty:
			payload = {"x": [], "y": [], "time": []}
			self._trip_geo_cache[fileid] = payload
			return payload

		try:
			gdf = gpd.GeoDataFrame(
				df_geo,
				geometry=[Point(xy) for xy in zip(df_geo['longitude'], df_geo['latitude'])],
				crs="EPSG:4326",
			).to_crs(epsg=3857)
		except KeyError as e:
			logger.error(f"Missing expected geo columns in trip data: {e} fileid={fileid}")
			return None

		time_values: list = []
		if 'metric_time' in df_geo.columns:
			time_values = pd.to_datetime(df_geo['metric_time'], errors='coerce').tolist()

		payload = {
			"x": gdf.geometry.x.tolist(),
			"y": gdf.geometry.y.tolist(),
			"time": time_values,
		}
		self._trip_geo_cache[fileid] = payload
		return payload

	def _load_trip_plot_data(self, fileid: int, metric_name: str) -> dict[str, list] | None:
		cache_key = (fileid, metric_name)
		if cache_key in self._trip_plot_cache:
			return self._trip_plot_cache[cache_key]

		geo_payload = self._load_trip_geo_data(fileid)
		if geo_payload is None:
			return None

		speed_col_name = self._resolve_actual_torqlogs_column(metric_name)
		time_col = (self._resolve_actual_torqlogs_column('gpstime')
					or self._resolve_actual_torqlogs_column('devicetime'))
		if not speed_col_name:
			return None

		time_order = f' ORDER BY "{time_col}"' if time_col else ' ORDER BY id'
		q = (
			f'SELECT "{speed_col_name}" AS selectedmetric '
			f'FROM torqlogs WHERE fileid = {int(fileid)}{time_order}'
		)
		df_part = pd.read_sql(q, self.engine)
		if df_part.empty:
			self._trip_plot_cache[cache_key] = {"x": [], "y": [], "speed": [], "time": []}
			return self._trip_plot_cache[cache_key]
		speed_series = pd.to_numeric(df_part['selectedmetric'], errors='coerce').fillna(0)

		x_vals = geo_payload["x"]
		y_vals = geo_payload["y"]
		time_values = geo_payload["time"]
		points = min(len(x_vals), len(y_vals), len(speed_series))
		if time_values:
			points = min(points, len(time_values))

		payload: dict[str, list] = {
			"x": x_vals[:points],
			"y": y_vals[:points],
			"speed": speed_series.tolist()[:points],
			"time": time_values[:points] if time_values else [],
		}
		self._trip_plot_cache[cache_key] = payload
		logger.debug(f"Loaded trip plot data for fileid={fileid}, metric_name={metric_name}, points={len(payload['x'])}")
		return payload

	def _selection_key(self, fileids: list[int], metric_name: str) -> str:
		return f"{self._map_cache_version}|metric={metric_name}|" + ",".join(str(fid) for fid in sorted(fileids))

	def _basemap_selection_key(self, fileids: list[int]) -> str:
		# Basemap tiles are independent of metric and colormap for a fixed trip selection/zoom.
		return f"{self._map_cache_version}|basemap|" + ",".join(str(fid) for fid in sorted(fileids))

	def _timeseries_selection_key(self, fileids: list[int], metric_names: list[str]) -> str:
		metrics_part = ",".join(metric_names)
		files_part = ",".join(str(fid) for fid in sorted(fileids))
		return f"{self._map_cache_version}|timeseries|metrics={metrics_part}|{files_part}"

	def _cache_fileid(self, fileids: list[int]) -> int | None:
		return int(fileids[0]) if len(fileids) == 1 else None

	def _load_cached_map_image(self, fileids: list[int], zoom: int, colormap: str, metric_name: str) -> tuple[bytes, tuple[float, float, float, float]] | None:
		selection_key = self._selection_key(fileids, metric_name)
		basemap_key = self._basemap_selection_key(fileids)
		q = text(
			"""
			SELECT image_png, ext_west, ext_east, ext_south, ext_north
			FROM mapimagecache
			WHERE (
				(selection_key = :selection_key AND zoom = :zoom AND colormap = :colormap)
				OR
				(selection_key = :basemap_key AND zoom = :zoom)
			)
			ORDER BY CASE WHEN selection_key = :selection_key AND colormap = :colormap THEN 0 ELSE 1 END
			LIMIT 1
			"""
		)
		with self.engine.connect() as conn:
			row = conn.execute(q, {
				"selection_key": selection_key,
				"basemap_key": basemap_key,
				"zoom": zoom,
				"colormap": colormap,
			}).first()
		if row and all(v is not None for v in row[1:5]):
			return row[0], (float(row[1]), float(row[2]), float(row[3]), float(row[4]))
		return None

	def _save_cached_map_image(self, fileids: list[int], zoom: int, colormap: str, metric_name: str, ext: tuple[float, float, float, float]):
		selection_key = self._selection_key(fileids, metric_name)
		basemap_key = self._basemap_selection_key(fileids)
		fileid = self._cache_fileid(fileids)
		ext_west, ext_east, ext_south, ext_north = ext
		buf = io.BytesIO()
		# Cache only the basemap raster; scatter/labels are redrawn dynamically.
		self.map_canvas.ax.figure.canvas.draw_idle()
		img_artists = self.map_canvas.ax.images
		if not img_artists:
			return
		arr = img_artists[0].get_array()
		if arr is None:
			return
		mpimg.imsave(buf, arr, format='png')
		image_bytes = buf.getvalue()
		upsert_sql = text(
			"""
			INSERT INTO mapimagecache (fileid, selection_key, zoom, colormap, image_png, ext_west, ext_east, ext_south, ext_north, created_at, updated_at)
			VALUES (:fileid, :selection_key, :zoom, :colormap, :image_png, :ext_west, :ext_east, :ext_south, :ext_north, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
			ON CONFLICT(selection_key, zoom, colormap)
			DO UPDATE SET
				fileid = excluded.fileid,
				image_png = excluded.image_png,
				ext_west = excluded.ext_west,
				ext_east = excluded.ext_east,
				ext_south = excluded.ext_south,
				ext_north = excluded.ext_north,
				updated_at = CURRENT_TIMESTAMP
			"""
		)
		with self.engine.begin() as conn:
			conn.execute(upsert_sql, {
				"fileid": fileid,
				"selection_key": selection_key,
				"zoom": zoom,
				"colormap": colormap,
				"image_png": image_bytes,
				"ext_west": ext_west,
				"ext_east": ext_east,
				"ext_south": ext_south,
				"ext_north": ext_north,
			})
			# Also store a metric/colormap-agnostic basemap entry for cross-metric reuse.
			conn.execute(upsert_sql, {
				"fileid": fileid,
				"selection_key": basemap_key,
				"zoom": zoom,
				"colormap": "basemap",
				"image_png": image_bytes,
				"ext_west": ext_west,
				"ext_east": ext_east,
				"ext_south": ext_south,
				"ext_north": ext_north,
			})
		logger.debug(f"Saved cached map image for selection_key={selection_key}, zoom={zoom}, colormap={colormap}")

	def _load_cached_timeseries_image(self, fileids: list[int], metric_names: list[str], colormap: str) -> bytes | None:
		selection_key = self._timeseries_selection_key(fileids, metric_names)
		q = text(
			"""
			SELECT image_png
			FROM mapimagecache
			WHERE selection_key = :selection_key AND zoom = :zoom AND colormap = :colormap
			LIMIT 1
			"""
		)
		with self.engine.connect() as conn:
			row = conn.execute(q, {
				"selection_key": selection_key,
				"zoom": -1,
				"colormap": colormap,
			}).first()
		return bytes(row[0]) if row and row[0] is not None else None

	def _save_cached_timeseries_image(self, fileids: list[int], metric_names: list[str], colormap: str):
		selection_key = self._timeseries_selection_key(fileids, metric_names)
		fileid = self._cache_fileid(fileids)
		buf = io.BytesIO()
		self.timeseries_canvas.figure.savefig(buf, format='png', dpi=100)
		image_bytes = buf.getvalue()
		upsert_sql = text(
			"""
			INSERT INTO mapimagecache (fileid, selection_key, zoom, colormap, image_png, ext_west, ext_east, ext_south, ext_north, created_at, updated_at)
			VALUES (:fileid, :selection_key, :zoom, :colormap, :image_png, :ext_west, :ext_east, :ext_south, :ext_north, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
			ON CONFLICT(selection_key, zoom, colormap)
			DO UPDATE SET
				fileid = excluded.fileid,
				image_png = excluded.image_png,
				updated_at = CURRENT_TIMESTAMP
			"""
		)
		with self.engine.begin() as conn:
			conn.execute(upsert_sql, {
				"fileid": fileid,
				"selection_key": selection_key,
				"zoom": -1,
				"colormap": colormap,
				"image_png": image_bytes,
				"ext_west": None,
				"ext_east": None,
				"ext_south": None,
				"ext_north": None,
			})

	def _compute_plot_bounds(self, all_x: list[float], all_y: list[float]) -> tuple[float, float, float, float] | None:
		if not all_x or not all_y:
			return None
		xmin = min(all_x)
		xmax = max(all_x)
		ymin = min(all_y)
		ymax = max(all_y)

		dx = max(1.0, xmax - xmin)
		dy = max(1.0, ymax - ymin)
		pad_x = dx * 0.03
		pad_y = dy * 0.03
		return (xmin - pad_x, xmax + pad_x, ymin - pad_y, ymax + pad_y)

	def _effective_basemap_zoom(self, bounds: tuple[float, float, float, float], base_zoom: int) -> int:
		"""Increase basemap zoom automatically for short trip extents to reduce blur."""
		xmin, xmax, ymin, ymax = bounds
		span = max(1.0, xmax - xmin, ymax - ymin)
		boost = 0
		if span <= 1_500:
			boost = 4
		elif span <= 3_000:
			boost = 3
		elif span <= 7_000:
			boost = 2
		elif span <= 15_000:
			boost = 1
		# Respect current user-selected zoom as baseline, but improve detail for tight bounds.
		return max(1, min(18, base_zoom + boost))

	def _plot_for_rows(self, rows):
		fileids = self._get_selected_fileids(rows)
		if not fileids:
			self.stats_label.setText("No trip selected")
			return

		base_zoom = int(self.zoom_combo.currentText())
		colormap_name = self._current_colormap
		selected_metrics = self._get_selected_metrics() or ['speedobdkmh']
		selected_metric = selected_metrics[0]
		cached_payload: tuple[bytes, tuple[float, float, float, float]] | None = None

		self.map_canvas.ax.clear()

		# Get selected colormap
		cmap = plt.colormaps[colormap_name]
		logger.debug(f"Using colormap: {colormap_name}")

		# Calculate colormap cycle length based on colormap type
		if colormap_name in ['tab10']:
			cycle_length = 10
		elif colormap_name in ['tab20', 'tab20b', 'tab20c']:
			cycle_length = 20
		elif colormap_name in ['Set1']:
			cycle_length = 9
		elif colormap_name in ['Set2', 'Dark2', 'Pastel2']:
			cycle_length = 8
		elif colormap_name in ['Set3', 'Pastel1']:
			cycle_length = 12
		else:
			cycle_length = 10

		plots = []
		all_x: list[float] = []
		all_y: list[float] = []
		all_metric_values: list[float] = []
		for idx, fileid in enumerate(fileids):
			plot_data = self._load_trip_plot_data(fileid, selected_metric)
			if not plot_data:
				continue

			x_vals = plot_data["x"]
			y_vals = plot_data["y"]
			all_x.extend(x_vals)
			all_y.extend(y_vals)
			speed_vals = pd.to_numeric(pd.Series(plot_data["speed"]), errors='coerce').fillna(0)
			all_metric_values.extend(speed_vals.tolist())
			if len(x_vals) == 0:
				continue

			sizes = (speed_vals.clip(lower=1, upper=50) * self._dot_size_scale).clip(lower=1, upper=200)
			base_color = cmap(idx % cycle_length)
			speed_abs_max = float(speed_vals.abs().max())
			colors = [(
				max(0.0, min(1.0, base_color[0] + 0.5 * (v / speed_abs_max if speed_abs_max > 0 else 0))),
				max(0.0, min(1.0, base_color[1] + 0.5 * (v / speed_abs_max if speed_abs_max > 0 else 0))),
				max(0.0, min(1.0, base_color[2] + 0.5 * (v / speed_abs_max if speed_abs_max > 0 else 0))),
				base_color[3]) for v in speed_vals]
			sc = self.map_canvas.ax.scatter(x_vals, y_vals, s=sizes, c=colors, label=f"fileid {fileid}", zorder=2)
			plots.append(sc)

		logger.debug(f"Plotted {len(plots)} trips on map for fileids: {fileids}")
		bounds = self._compute_plot_bounds(all_x, all_y)
		effective_zoom = base_zoom
		if bounds:
			effective_zoom = self._effective_basemap_zoom(bounds, base_zoom)
			cached_payload = self._load_cached_map_image(fileids, effective_zoom, colormap_name, selected_metric)
			if effective_zoom != base_zoom:
				logger.debug(
					f"Adaptive basemap zoom: base={base_zoom}, effective={effective_zoom}, "
					f"metric={selected_metric}, fileids={fileids}"
				)
		if bounds:
			xmin, xmax, ymin, ymax = bounds
			self.map_canvas.ax.set_xlim(xmin, xmax)
			self.map_canvas.ax.set_ylim(ymin, ymax)

		self._overlay_start_end_points(fileids, bounds)

		if plots and cached_payload:
			cached_img, cached_ext = cached_payload
			img = mpimg.imread(io.BytesIO(cached_img), format='png')
			self.map_canvas.ax.imshow(img, extent=cached_ext, interpolation='bilinear', zorder=0)
		elif plots:
			if bounds:
				self._start_async_basemap(bounds, effective_zoom, fileids, colormap_name, selected_metric)
		self.map_canvas.ax.set_title(f"Trip Map - {selected_metric}")
		self.map_canvas.ax.set_xlabel("Longitude")
		self.map_canvas.ax.set_ylabel("Latitude")
		self.map_canvas.draw_idle()
		self._update_timeseries_plot(fileids, selected_metrics, colormap_name)
		self._update_stats_panel(fileids, all_metric_values, all_x, all_y, selected_metric)

	def _load_trip_metadata(self, fileids: list[int]) -> dict:
		"""Load trip metadata from torqfiles table."""
		if not fileids:
			return {}
		
		try:
			fileids_str = ",".join(str(fid) for fid in fileids[:10])
			query = f"SELECT fileid, trip_start, trip_end, trip_duration, trip_distance FROM torqfiles WHERE fileid IN ({fileids_str})"
			df = pd.read_sql(query, self.engine)
			if df.empty:
				return {}
			return df.set_index('fileid').to_dict('index')
		except Exception as e:
			logger.warning(f"Could not load trip metadata: {e}")
			return {}

	def _format_trip_stats(self, fileids: list[int], trip_count: int, point_count: int,
		metric_name: str, metric_min: float, metric_avg: float, metric_max: float,
		all_x: list[float], all_y: list[float], trip_info: dict,
		all_metric_stats: dict | None = None) -> str:
		"""Format comprehensive trip stats with all metric min/max/avg."""
		W = 52
		lines: list[str] = []

		lines.append("═" * W)
		lines.append(f"TRIP SUMMARY  — {trip_count} trip(s), {point_count} pts")
		lines.append("═" * W)

		# Trip metadata & date
		if trip_info:
			total_distance = sum(row.get('trip_distance', 0) or 0 for row in trip_info.values())
			total_duration = sum(row.get('trip_duration', 0) or 0 for row in trip_info.values())
			start_dates = [str(row.get('trip_start', '')) for row in trip_info.values() if row.get('trip_start')]
			end_dates   = [str(row.get('trip_end',   '')) for row in trip_info.values() if row.get('trip_end')]
			if start_dates:
				lines.append(f"Date:     {start_dates[0]}")
			if end_dates and end_dates != start_dates:
				lines.append(f"End:      {end_dates[0]}")
			if total_distance:
				lines.append(f"Distance: {total_distance/1000:.2f} km")
			if total_duration:
				lines.append(f"Duration: {format_duration(total_duration)}")
			if total_distance and total_duration:
				avg_spd = (total_distance / 1000) / (total_duration / 3600)
				lines.append(f"Avg spd:  {avg_spd:.1f} km/h")

		# Selected metric summary
		category, display_name, unit = categorize_metric(metric_name)
		unit_str = f" {unit}" if unit else ""
		lines.append(f"\n{'─' * W}")
		lines.append(f"Selected: {display_name}{unit_str}")
		lines.append(f"  min {metric_min:.2f}  avg {metric_avg:.2f}  max {metric_max:.2f}")
		suggestion = get_analysis_suggestion(category)
		lines.append(f"  [{suggestion['analysis_type']}]  {suggestion['visualization']}")

		# All metrics grouped by category
		if all_metric_stats:
			grouped = group_metrics_by_category(list(all_metric_stats.keys()))
			lines.append(f"\n{'─' * W}")
			lines.append("ALL METRICS  (min / avg / max):")
			for cat, cat_metrics in grouped.items():
				cat_lines: list[str] = []
				for disp, u, orig in cat_metrics:
					stats = all_metric_stats.get(orig)
					if stats:
						u_s = f" {u}" if u else ""
						cat_lines.append(
							f"  {disp:<28s}  "
							f"{stats['min']:.1f}/{stats['avg']:.1f}/{stats['max']:.1f}{u_s}"
						)
				if cat_lines:
					lines.append(f"\n{cat.value}:")
					lines.extend(cat_lines)

		lines.append("\n" + "═" * W)
		return "\n".join(lines)

	def _update_stats_panel(self, fileids: list[int], metric_values: list[float], all_x: list[float], all_y: list[float], metric_name: str):
		trip_count = len(fileids)
		point_count = len(all_x)
		metric_series = pd.Series(metric_values, dtype="float64") if metric_values else pd.Series(dtype="float64")
		metric_min = float(metric_series.min()) if not metric_series.empty else 0.0
		metric_avg = float(metric_series.mean()) if not metric_series.empty else 0.0
		metric_max = float(metric_series.max()) if not metric_series.empty else 0.0

		# Selection-level metadata and aggregate stats are expensive; cache by selected fileids.
		selection_key = tuple(sorted(fileids))
		if selection_key in self._selection_stats_cache:
			trip_info, all_metric_stats = self._selection_stats_cache[selection_key]
		else:
			trip_info = self._load_trip_metadata(fileids)
			all_metric_stats = self._load_all_metric_stats(fileids)
			self._selection_stats_cache[selection_key] = (trip_info, all_metric_stats)

		# Build stats text with trip info, metrics, and suggestions
		stats_text = self._format_trip_stats(
			fileids, trip_count, point_count, metric_name,
			metric_min, metric_avg, metric_max, all_x, all_y, trip_info, all_metric_stats
		)

		self.stats_label.setText(stats_text)

	def _update_timeseries_plot(self, fileids: list[int], metric_names: list[str], colormap_name: str):
		"""Draw one or more metrics over time for selected trips."""
		ax = self.timeseries_canvas.ax
		ax.clear()
		cached_img = self._load_cached_timeseries_image(fileids, metric_names, colormap_name)
		if cached_img is not None:
			img = mpimg.imread(io.BytesIO(cached_img), format='png')
			ax.imshow(img, extent=(0, 1, 0, 1), transform=ax.transAxes, aspect='auto', zorder=0)
			ax.set_axis_off()
			self.timeseries_canvas.draw_idle()
			logger.debug(f"Loaded cached timeseries image for metrics={metric_names}, trips={fileids}")
			return
		ax.set_axis_on()
		cmap = plt.colormaps[colormap_name]
		cycle_length = 9 if colormap_name in ['Set1'] else (8 if colormap_name in ['Set2', 'Dark2'] else 10)
		# Line styles cycle across trips when multiple trips are shown
		linestyles = ['-', '--', ':', '-.']
		multi_metric = len(metric_names) > 1
		multi_trip = len(fileids) > 1

		has_data = False
		# Color index cycles per metric so each metric gets a distinct color
		for m_idx, metric_name in enumerate(metric_names):
			for t_idx, fileid in enumerate(fileids):
				plot_data = self._load_trip_plot_data(fileid, metric_name)
				if not plot_data or not plot_data.get('speed'):
					continue
				time_vals = plot_data.get('time') or []
				metric_vals = plot_data['speed']
				# Fall back to sequential index when timestamps are unavailable or all-NaT
				use_time = bool(time_vals) and any(t is not None and not pd.isna(t) for t in time_vals[:10])
				if use_time:
					# Drop rows where the timestamp is NaT to avoid matplotlib ConversionError
					pairs = [(t, v) for t, v in zip(time_vals, metric_vals)
					         if t is not None and not pd.isna(t)]
					if pairs:
						x_vals, metric_vals = zip(*pairs)
					else:
						x_vals, metric_vals = [], []
				else:
					x_vals = list(range(len(metric_vals)))
				if not x_vals:
					continue
				color = cmap(m_idx % cycle_length)
				lstyle = linestyles[t_idx % len(linestyles)] if multi_trip else '-'
				_, display_name, unit = categorize_metric(metric_name)
				if multi_metric and multi_trip:
					label = f"{display_name} / trip {fileid}"
				elif multi_metric:
					label = display_name
				elif multi_trip:
					label = f"trip {fileid}"
				else:
					label = None
				ax.plot(x_vals, metric_vals, color=color, linestyle=lstyle,
				        linewidth=0.8, alpha=0.85, label=label)
				has_data = True

		if len(metric_names) == 1:
			_, display_name, unit = categorize_metric(metric_names[0])
			unit_str = f" ({unit})" if unit else ""
			ax.set_title(f"{display_name}{unit_str} over time", fontsize=9, pad=3)
			ax.set_ylabel(display_name, fontsize=8)
		else:
			ax.set_title("Metrics over time", fontsize=9, pad=3)
			ax.set_ylabel("Value", fontsize=8)
		ax.set_xlabel("Time", fontsize=8)
		ax.tick_params(labelsize=7)
		if (multi_metric or multi_trip) and has_data:
			ax.legend(fontsize=7)
		if has_data:
			try:
				ax.figure.autofmt_xdate(rotation=30)
			except Exception as e:
				logger.warning(f"Could not format x-axis dates: {e} ({type(e)})")
			try:
				self._save_cached_timeseries_image(fileids, metric_names, colormap_name)
			except Exception as e:
				logger.warning(f"Could not save timeseries cache: {e} ({type(e)})")
		self.timeseries_canvas.draw_idle()

	def _get_torqlogs_numeric_columns(self) -> set[str]:
		"""Return the set of actual torqlogs column names that have a numeric DB type."""
		inspector = inspect(self.engine)
		numeric_type_prefixes = (
			'int', 'float', 'real', 'double', 'numeric', 'decimal',
			'smallint', 'bigint', 'money', 'number',
		)
		numeric_cols: set[str] = set()
		for col in inspector.get_columns("torqlogs"):
			type_str = str(col["type"]).lower()
			if any(type_str.startswith(p) for p in numeric_type_prefixes):
				numeric_cols.add(str(col["name"]))
		return numeric_cols

	def _load_all_metric_stats(self, fileids: list[int]) -> dict[str, dict[str, float]]:
		"""Load min/avg/max for all available metrics for the selected fileids."""
		if not fileids:
			return {}
		metric_columns = self._get_metric_columns_with_valid_data()
		if not metric_columns:
			return {}

		numeric_cols = self._get_torqlogs_numeric_columns()

		agg_parts: list[str] = []
		col_map: list[tuple[str, str]] = []
		for metric in metric_columns:
			actual = self._resolve_actual_torqlogs_column(metric)
			if actual and actual in numeric_cols:
				agg_parts.append(
					f'MIN(CAST("{actual}" AS FLOAT)) AS "_s_{metric}_min", '
					f'AVG(CAST("{actual}" AS FLOAT)) AS "_s_{metric}_avg", '
					f'MAX(CAST("{actual}" AS FLOAT)) AS "_s_{metric}_max"'
				)
				col_map.append((metric, actual))

		if not agg_parts:
			return {}

		fileids_str = ",".join(str(fid) for fid in fileids)
		query = f"SELECT {', '.join(agg_parts)} FROM torqlogs WHERE fileid IN ({fileids_str})"
		try:
			df = pd.read_sql(query, self.engine)
			if df.empty:
				return {}
			row = df.iloc[0]
			result: dict[str, dict[str, float]] = {}
			for metric, _ in col_map:
				try:
					min_val = row.get(f"_s_{metric}_min")
					avg_val = row.get(f"_s_{metric}_avg")
					max_val = row.get(f"_s_{metric}_max")
					if min_val is not None and not pd.isna(min_val):
						result[metric] = {
							'min': float(min_val),
							'avg': float(avg_val) if avg_val is not None and not pd.isna(avg_val) else 0.0,
							'max': float(max_val) if max_val is not None and not pd.isna(max_val) else 0.0,
						}
				except Exception as e:
					logger.warning(f"Error processing stats for metric '{metric}': {e} ({type(e)})")
			return result
		except Exception as e:
			logger.warning(f"Could not load all metric stats: {e}")
			return {}

	def _start_async_basemap(self, bounds: tuple[float, float, float, float], zoom: int, fileids: list[int], colormap_name: str, metric_name: str):
		xmin, xmax, ymin, ymax = bounds
		if xmax <= xmin or ymax <= ymin:
			return

		self._basemap_request_id += 1
		request_id = self._basemap_request_id
		self._basemap_request_context[request_id] = {
			"fileids": fileids,
			"zoom": zoom,
			"colormap": colormap_name,
			"metric": metric_name,
		}

		thread = QThread()
		worker = BasemapWorker((xmin, xmax, ymin, ymax), zoom, request_id)
		worker.moveToThread(thread)

		thread.started.connect(worker.run)
		worker.finished.connect(self._on_basemap_loaded)
		worker.error.connect(self._on_basemap_error)
		worker.finished.connect(thread.quit)
		worker.error.connect(thread.quit)
		thread.finished.connect(worker.deleteLater)
		thread.finished.connect(thread.deleteLater)
		thread.finished.connect(lambda t=thread: self._active_threads.discard(t))

		self._basemap_worker = worker
		self._basemap_thread = thread
		self._active_threads.add(thread)
		logger.debug(f"Starting basemap worker thread for request_id={request_id} with bounds=({xmin}, {ymin}, {xmax}, {ymax}) and zoom={zoom}")
		thread.start()

	def _on_basemap_loaded(self, img, ext, request_id: int):
		try:
			if request_id != self._basemap_request_id:
				return
		except Exception as e:
			logger.error(f"Error in basemap loaded handler: {e} ({type(e)})")
			return
		self.map_canvas.ax.imshow(img, extent=ext, interpolation='bilinear', zorder=0)
		self.map_canvas.draw_idle()
		ctx = self._basemap_request_context.get(request_id)
		if ctx:
			a, b, c, d = ext
			ext_typed: tuple[float, float, float, float] = (float(a), float(b), float(c), float(d))
			self._save_cached_map_image(
				cast(list[int], ctx["fileids"]),
				cast(int, ctx["zoom"]),
				cast(str, ctx["colormap"]),
				cast(str, ctx["metric"]),
				ext_typed,
			)
			self._basemap_request_context.pop(request_id, None)

	def _on_basemap_error(self, err: str, request_id: int):
		try:
			if request_id != self._basemap_request_id:
				return
		except Exception as e:
			logger.error(f"Error in basemap error handler: {e} ({type(e)}) error: {err} request_id: {request_id}")
			return
		self._basemap_request_context.pop(request_id, None)
		# Tile/network failures should not break UI interaction.
		print(f"{self} Basemap load failed: {err} (request_id={request_id}) current_id={self._basemap_request_id}")

	def _shutdown_thread(self, thread: QThread | None, name: str):
		if thread is None:
			return
		try:
			if not thread.isRunning():
				return
		except RuntimeError:
			# QThread QObject can already be deleted by Qt during shutdown.
			return
		if thread.currentThread() is thread:
			return
		logger.debug(f"Stopping thread '{name}'")
		thread.requestInterruption()
		thread.quit()
		if not thread.wait(3000):
			logger.warning(f"Thread '{name}' did not stop in time; terminating")
			thread.terminate()
			thread.wait(1000)

	def closeEvent(self, event: QCloseEvent):
		self._shutdown_thread(self._basemap_thread, "basemap")
		self._shutdown_thread(self._initial_trips_thread, "initial_trips")
		for idx, t in enumerate(list(self._active_threads)):
			self._shutdown_thread(t, f"active_{idx}")
		super().closeEvent(event)

	def on_row_selected(self, selected, deselected):
		rows = sorted(set(index.row() for index in self.table.selectionModel().selectedRows()))
		if rows:
			logger.debug(f"on_row_selected with {len(rows)} selected row(s): {rows[:5]}{'...' if len(rows) > 5 else ''}")
			# Debounce bursty selection events while user is building a multi-row selection.
			self._plot_refresh_timer.start(250)
		else:
			self.stats_label.setText("No trip selected")

class PandasModel(QAbstractTableModel):
	"""Minimal Qt model for pandas DataFrame for QTableView."""
	def __init__(self, data):
		super().__init__()
		self._data = data
		logger.debug(f"PandasModel initialized with {self._data.shape[0]} rows and {self._data.shape[1]} columns")

	def sort(self, column: int, order: Qt.SortOrder = Qt.SortOrder.AscendingOrder) -> None:
		colname = self._data.columns[column]
		self.layoutAboutToBeChanged.emit()
		self._data.sort_values(by=colname, ascending=(order == Qt.SortOrder.AscendingOrder), inplace=True)
		self._data.reset_index(inplace=True)
		self._data.set_index(self._data.columns[0], inplace=True)
		self.layoutChanged.emit()

	def rowCount(self, parent: QModelIndex | QPersistentModelIndex = QModelIndex()) -> int:
		return self._data.shape[0]

	def columnCount(self, parent: QModelIndex | QPersistentModelIndex = QModelIndex()) -> int:
		return self._data.shape[1]

	def data(self, index: QModelIndex | QPersistentModelIndex, role: int = Qt.ItemDataRole.DisplayRole) -> object:
		if role == Qt.ItemDataRole.DisplayRole:
			return str(self._data.iloc[index.row(), index.column()])
		return None

	def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.ItemDataRole.DisplayRole) -> object:
		if role == Qt.ItemDataRole.DisplayRole:
			if orientation == Qt.Orientation.Horizontal:
				return self._data.columns[section]
			else:
				return str(section)
		return None



if __name__ == "__main__":
	args = get_args('guitest2')
	app = QApplication(sys.argv)
	window = MainWindow(args)
	logger.debug(f"Starting application event loop window: {window}")
	window.showMaximized()
	# window.resize(1000, 600)
	# window.show()
	sys.exit(app.exec())
