import io
import time
from typing import Any, cast

import contextily as ctx
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt import NavigationToolbar2QT as NavigationToolbar
import pandas as pd
from loguru import logger
from sqlalchemy import text
from PySide6.QtWidgets import (
	QMainWindow, QWidget, QVBoxLayout, QSplitter, QHBoxLayout, QLabel,
	QComboBox, QFrame, QTableView, QAbstractItemView, QLineEdit, QPushButton,
	QFormLayout, QSpinBox, QDoubleSpinBox, QMessageBox, QCheckBox, QCompleter, QTabWidget,
)
from PySide6.QtCore import Qt, QTimer, QThread, QItemSelectionModel, QStringListModel
from PySide6.QtGui import QCloseEvent

from .basemap_worker import BasemapWorker
from .position_load_worker import PositionLoadWorker
from .position_table_model import PositionTableModel
from .pandas_model import PandasModel
from ._helpers import _ORPHAN_QTHREADS, _release_orphan_thread


class PositionManagerWindow(QMainWindow):
	def __init__(self, engine, parent=None):
		super().__init__(parent)
		self.engine = engine
		self.setWindowTitle("Position Manager")
		self.resize(1240, 780)
		if parent:
			self.args = parent.args
		else:
			self.args = type("Args", (), {"debug": False})()

		self._selected_row_index: int | None = None
		self._selected_row_indices: list[int] = []
		self._scatter_index_map: dict[Any, list[int]] = {}
		self._table_model: PositionTableModel | None = None
		self._full_bounds: tuple[float, float, float, float] | None = None
		self._basemap_artist = None
		self._selected_markers: list[Any] = []
		self._point_label_artists: list[Any] = []
		self._show_point_labels = True
		self._show_labeled_points = True
		self._visible_row_indices: set[int] = set()
		self._basemap_mem_cache: dict[str, tuple[bytes, tuple[float, float, float, float]]] = {}
		self._load_thread: QThread | None = None
		self._load_worker: PositionLoadWorker | None = None
		self._basemap_thread: QThread | None = None
		self._basemap_worker: BasemapWorker | None = None
		self._basemap_request_id = 0
		self._pending_basemap_key: str | None = None
		self._queued_basemap_request: tuple[tuple[float, float, float, float], int] | None = None
		self._pending_close = False
		self._pending_close_started_at: float | None = None
		self._active_threads: set[QThread] = set()
		self._basemap_status_artist = None
		self._restore_after_reload: dict[str, Any] | None = None
		self._min_zoom_span_m = 25.0
		self._current_basemap_zoom = 8
		self._min_count_filter = 0
		self._current_sort_column: int = -1
		self._current_sort_order: Qt.SortOrder = Qt.SortOrder.AscendingOrder
		self._applying_sort: bool = False
		self._label_filter_active: bool = False
		self._label_filter_text: str = ""
		self._hide_labeled_active: bool = False
		self._updating_selection: bool = False
		self._pending_pick_call: tuple[list[int], bool] | None = None
		self._group_mode: str = "label"
		self._grouped_positions_df = pd.DataFrame(
			columns=["label", "start_points", "end_points", "total_points", "total_count", "avg_latitude", "avg_longitude"]
		)
		self._grouped_sources: dict[str, list[int]] = {}
		self._pick_debounce_timer: QTimer = QTimer(self)
		self._pick_debounce_timer.setSingleShot(True)
		self._pick_debounce_timer.setInterval(80)
		self._pick_debounce_timer.timeout.connect(self._flush_pending_pick)
		self.df_positions = pd.DataFrame(
			columns=['pos_type', 'pos_id', 'latitude', 'longitude', 'count', 'label', 'x', 'y']
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

		grouped_tab = QWidget()
		grouped_layout = QVBoxLayout(grouped_tab)
		grouped_layout.setContentsMargins(2, 2, 2, 2)
		grouped_toolbar = QWidget()
		grouped_toolbar_layout = QHBoxLayout(grouped_toolbar)
		grouped_toolbar_layout.setContentsMargins(0, 0, 0, 0)
		grouped_toolbar_layout.addWidget(QLabel("Group by:"))
		self.group_mode_combo = QComboBox()
		self.group_mode_combo.addItem("Label (ignore type)", "label")
		self.group_mode_combo.addItem("Start labels", "start")
		self.group_mode_combo.addItem("End labels", "end")
		self.group_mode_combo.currentIndexChanged.connect(self._on_group_mode_changed)
		grouped_toolbar_layout.addWidget(self.group_mode_combo)
		grouped_toolbar_layout.addStretch()
		self.grouped_positions_table = QTableView()
		self.grouped_positions_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
		self.grouped_positions_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
		self.grouped_positions_table.setSortingEnabled(True)
		self.grouped_positions_table.verticalHeader().setVisible(False)
		grouped_layout.addWidget(grouped_toolbar)
		grouped_layout.addWidget(self.grouped_positions_table)

		self.table_tabs = QTabWidget()
		self.table_tabs.addTab(self.positions_table, "Positions")
		self.table_tabs.addTab(grouped_tab, "Grouped labels")
		right_layout.addWidget(self.table_tabs, stretch=6)

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
		self._label_completer = QCompleter([], self)
		self._label_completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
		self._label_completer.setFilterMode(Qt.MatchFlag.MatchContains)
		self.label_edit.setCompleter(self._label_completer)
		self.label_filter_chk = QCheckBox("Filter table by label")
		self.label_filter_edit = QLineEdit()
		self.label_filter_edit.setPlaceholderText("Text to match (empty = has any label)")
		self.label_filter_edit.setEnabled(False)
		_label_filter_row = QWidget()
		_label_filter_layout = QHBoxLayout(_label_filter_row)
		_label_filter_layout.setContentsMargins(0, 0, 0, 0)
		self.hide_labeled_chk = QCheckBox("Hide labeled")
		_label_filter_layout.addWidget(self.hide_labeled_chk)
		_label_filter_layout.addWidget(self.label_filter_chk)
		_label_filter_layout.addWidget(self.label_filter_edit, 1)
		self.show_labeled_points_chk = QCheckBox("Show points with labels")
		self.show_labeled_points_chk.setChecked(True)
		form.addRow("Type", self.pos_type_combo)
		form.addRow("ID", self.pos_id_spin)
		form.addRow("Latitude", self.lat_spin)
		form.addRow("Longitude", self.lon_spin)
		form.addRow("Count", self.count_spin)
		form.addRow("Min count (table)", self.min_count_filter_spin)
		form.addRow("Label", self.label_edit)
		form.addRow("Label filter", _label_filter_row)
		form.addRow("Map", self.show_labeled_points_chk)
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
		self.reload_map_btn = QPushButton("Reload map")
		self.reload_map_btn.setFixedSize(92, 26)
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
		button_row.addWidget(self.reload_map_btn)
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
		self.show_labeled_points_chk.toggled.connect(self._on_toggle_labeled_points)
		self.zoom_in_btn.clicked.connect(self._zoom_in)
		self.zoom_out_step_btn.clicked.connect(self._zoom_out)
		self.zoom_out_btn.clicked.connect(self._zoom_full)
		self.toggle_labels_btn.toggled.connect(self._on_toggle_labels)
		self.sort_similar_btn.clicked.connect(self._sort_table_by_similar_latlon)
		self.reload_map_btn.clicked.connect(self._force_reload_basemap)
		self.positions_table.horizontalHeader().sortIndicatorChanged.connect(self._on_sort_indicator_changed)
		self.hide_labeled_chk.toggled.connect(self._on_label_filter_changed)
		self.label_filter_chk.toggled.connect(self._on_label_filter_changed)
		self.label_filter_edit.textChanged.connect(self._on_label_filter_changed)
		self.label_edit.returnPressed.connect(self._on_label_return_pressed)

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

	def _force_reload_basemap(self):
		"""Evict all PMW basemap cache entries and re-fetch from network."""
		self._basemap_mem_cache.clear()
		try:
			with self.engine.begin() as conn:
				conn.execute(text("DELETE FROM mapimagecache WHERE zoom = -2 AND colormap = 'posmgr'"))
			logger.debug("PMW: cleared basemap cache from DB")
		except Exception as e:
			logger.warning(f"PMW: could not clear basemap cache from DB: {e} ({type(e)})")
		self._cancel_basemap_thread("force_reload")
		self._queued_basemap_request = None
		self._refresh_basemap_for_current_view(target_zoom=self._current_basemap_zoom)

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
		except RuntimeError as e:
			logger.debug(f"RuntimeError calling thread.isRunning(): {e} ({type(e)})")
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
		except RuntimeError as e:
			logger.error(f"RuntimeError in _stop_thread for '{attr_name}': {e} ({type(e)})")
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

	def _set_basemap_status(self, message: str | None):
		if self._basemap_status_artist is not None:
			if getattr(self._basemap_status_artist, "axes", None) is None:
				self._basemap_status_artist = None
			else:
				try:
					self._basemap_status_artist.remove()
				except NotImplementedError:
					# Artist can already be detached after axes clear.
					pass
				except Exception as e:
					logger.debug(f"Could not remove basemap status artist: {e} ({type(e)})")
				self._basemap_status_artist = None
		if not message:
			return
		self._basemap_status_artist = self.map_ax.text(
			0.01,
			0.99,
			message,
			transform=self.map_ax.transAxes,
			ha="left",
			va="top",
			fontsize=8,
			color="black",
			bbox={"boxstyle": "round,pad=0.2", "facecolor": "white", "alpha": 0.65, "edgecolor": "none"},
			zorder=10,
		)

	def _cancel_basemap_thread(self, reason: str) -> bool:
		thread = self._basemap_thread
		if thread is None:
			return True
		try:
			if not thread.isRunning():
				self._active_threads.discard(thread)
				self._basemap_thread = None
				self._basemap_worker = None
				return True
		except RuntimeError as e:
			logger.error(f"RuntimeError checking basemap thread.isRunning() ({reason}): {e} ({type(e)})")
			self._active_threads.discard(thread)
			self._basemap_thread = None
			self._basemap_worker = None
			return True
		logger.warning(f"Cancelling PositionManager basemap worker ({reason})")
		thread.requestInterruption()
		thread.quit()
		return False

	def _on_basemap_timeout(self, request_id: int):
		if request_id != self._basemap_request_id:
			return
		if not self._thread_is_running(self._basemap_thread):
			return
		logger.warning(f"PositionManager basemap request timed out (request_id={request_id}); cancelling stuck worker")
		self._set_basemap_status("Basemap unavailable; showing points only")
		# Cancel the stuck thread so the next refresh attempt can start a new worker.
		self._cancel_basemap_thread("timeout")
		self.map_canvas.draw_idle()

	def _start_async_basemap(self, bounds: tuple[float, float, float, float], zoom: int):
		cache_key = self._bounds_key(bounds, zoom)

		cached = self._load_cached_basemap(cache_key)
		if cached:
			img, ext = cached
			self._set_basemap_status('cached')
			self._draw_basemap_from_bytes(img, ext)
			return

		if self._thread_is_running(self._basemap_thread):
			# Keep latest viewport request and execute it after current worker finishes.
			self._queued_basemap_request = (bounds, zoom)
			return

		self._basemap_request_id += 1
		request_id = self._basemap_request_id
		self._pending_basemap_key = cache_key
		self._set_basemap_status("Loading basemap...")

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
		thread.finished.connect(lambda t=thread: self._active_threads.discard(t))
		self._basemap_thread = thread
		self._basemap_worker = worker
		self._active_threads.add(thread)
		thread.start()
		QTimer.singleShot(20000, lambda rid=request_id: self._on_basemap_timeout(rid))
		if self.args.debug:
			logger.debug(f"Started basemap worker thread {thread} for request_id {request_id} with bounds {bounds} and zoom {zoom}. active threads: {len(self._active_threads)}")

	def _on_basemap_thread_finished(self):
		self._basemap_thread = None
		self._basemap_worker = None
		if self._queued_basemap_request is not None:
			bounds, zoom = self._queued_basemap_request
			self._queued_basemap_request = None
			self._start_async_basemap(bounds, zoom)
			return
		if self._pending_close and not self._any_worker_running():
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
		self._set_basemap_status(f'z: {self._current_basemap_zoom} id: {request_id}')
		self._draw_basemap_array(img, ext_typed)

	def _on_basemap_error(self, error: str, request_id: int):
		if request_id != self._basemap_request_id:
			return
		logger.warning(f"PositionManager basemap load failed: {error}")
		self._set_basemap_status(f"Basemap unavailable; showing points only: {error}")
		# Fallback path: try a synchronous paint if async worker/provider failed.
		if self._full_bounds is not None:
			try:
				ctx.add_basemap(self.map_ax, crs="EPSG:3857")
				self._set_basemap_status(f"Basemap unavailable; showing points only: {error}")
				self.map_canvas.draw_idle()
			except Exception as sync_err:
				logger.warning(f"PositionManager sync basemap fallback failed: {sync_err} ({type(sync_err)})")
				self.map_canvas.draw_idle()

	def _draw_basemap_array(self, img, ext: tuple[float, float, float, float]):
		if self._basemap_artist is not None:
			try:
				self._basemap_artist.remove()
			except Exception as e:
				logger.error(f"Failed to remove previous basemap artist: {e} ({type(e)})")
		self._basemap_artist = self.map_ax.imshow(img, extent=ext, interpolation="bilinear", zorder=0)
		self.map_canvas.draw_idle()

	def _draw_basemap_from_bytes(self, image_bytes: bytes, ext: tuple[float, float, float, float]):
		img = mpimg.imread(io.BytesIO(image_bytes), format="png")
		self._draw_basemap_array(img, ext)

	def _set_table_model(self):
		filtered_df = self._filtered_positions_df()
		self._table_model = PositionTableModel(filtered_df)
		self.positions_table.setModel(self._table_model)
		self.positions_table.horizontalHeader().setStretchLastSection(True)
		self.positions_table.setSortingEnabled(True)
		self.positions_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
		self.positions_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
		if self._current_sort_column >= 0:
			self._applying_sort = True
			try:
				self.positions_table.sortByColumn(self._current_sort_column, self._current_sort_order)
			finally:
				self._applying_sort = False
		if self.positions_table.selectionModel() is not None:
			self.positions_table.selectionModel().selectionChanged.connect(self._on_table_selection_changed)
		self._refresh_grouped_positions_table()

	def _filtered_positions_df(self) -> pd.DataFrame:
		filtered_df = self.df_positions
		if self._min_count_filter > 0 and not filtered_df.empty:
			filtered_df = filtered_df[filtered_df["count"] >= self._min_count_filter]
		if self._hide_labeled_active and not filtered_df.empty:
			filtered_df = filtered_df[filtered_df["label"].astype(str).str.strip() == ""]
		if self._label_filter_active and not filtered_df.empty:
			if self._label_filter_text:
				mask = filtered_df["label"].astype(str).str.contains(
					self._label_filter_text, case=False, na=False, regex=False
				)
				filtered_df = filtered_df[mask]
			else:
				filtered_df = filtered_df[filtered_df["label"].astype(str).str.strip() != ""]
		return filtered_df

	def _on_group_mode_changed(self, index: int):
		mode = str(self.group_mode_combo.currentData() or "label")
		self._group_mode = mode
		self._refresh_grouped_positions_table()

	def _refresh_grouped_positions_table(self):
		base = self._filtered_positions_df().copy()
		self._grouped_sources = {}
		if base.empty:
			self._grouped_positions_df = pd.DataFrame(
				columns=["label", "start_points", "end_points", "total_points", "total_count", "avg_latitude", "avg_longitude"]
			)
			self.grouped_positions_table.setModel(PandasModel(self._grouped_positions_df))
			return

		base["label_group"] = base["label"].fillna("").astype(str).str.strip()
		base.loc[base["label_group"] == "", "label_group"] = "(no label)"
		if self._group_mode == "start":
			base = base[base["pos_type"] == "start"]
		elif self._group_mode == "end":
			base = base[base["pos_type"] == "end"]

		if base.empty:
			self._grouped_positions_df = pd.DataFrame(
				columns=["label", "start_points", "end_points", "total_points", "total_count", "avg_latitude", "avg_longitude"]
			)
			self.grouped_positions_table.setModel(PandasModel(self._grouped_positions_df))
			return

		rows: list[dict[str, Any]] = []
		for label, group in base.groupby("label_group", dropna=False):
			self._grouped_sources[str(label)] = [int(v) for v in group.index.tolist()]
			rows.append(
				{
					"label": str(label),
					"start_points": int((group["pos_type"] == "start").sum()),
					"end_points": int((group["pos_type"] == "end").sum()),
					"total_points": int(len(group)),
					"total_count": round(float(pd.to_numeric(group["count"], errors="coerce").fillna(0).sum()), 2),
					"avg_latitude": round(float(pd.to_numeric(group["latitude"], errors="coerce").mean()), 6),
					"avg_longitude": round(float(pd.to_numeric(group["longitude"], errors="coerce").mean()), 6),
				}
			)

		self._grouped_positions_df = pd.DataFrame(
			rows,
			columns=["label", "start_points", "end_points", "total_points", "total_count", "avg_latitude", "avg_longitude"],
		)
		if not self._grouped_positions_df.empty:
			self._grouped_positions_df.sort_values(by="label", inplace=True)
			self._grouped_positions_df.reset_index(drop=True, inplace=True)

		model = PandasModel(self._grouped_positions_df)
		self.grouped_positions_table.setModel(model)
		self.grouped_positions_table.horizontalHeader().setStretchLastSection(True)
		self.grouped_positions_table.resizeColumnsToContents()
		if self.grouped_positions_table.selectionModel() is not None:
			self.grouped_positions_table.selectionModel().selectionChanged.connect(self._on_grouped_selection_changed)

	def _on_grouped_selection_changed(self, selected, deselected):
		if self.grouped_positions_table.selectionModel() is None or self._grouped_positions_df.empty:
			return
		rows = self.grouped_positions_table.selectionModel().selectedRows()
		if not rows:
			return
		source_rows: list[int] = []
		for row in rows:
			view_idx = row.row()
			if view_idx < 0 or view_idx >= len(self._grouped_positions_df.index):
				continue
			label = str(self._grouped_positions_df.iloc[view_idx]["label"])
			source_rows.extend(self._grouped_sources.get(label, []))
		if not source_rows:
			return
		source_rows = list(dict.fromkeys(source_rows))
		self._select_rows_by_indices(source_rows, select_table=True, zoom_to_points=(len(source_rows) == 1))

	def _on_min_count_filter_changed(self, value: int):
		self._min_count_filter = int(value)
		self._set_table_model()
		if self._selected_row_indices:
			self._select_rows_by_indices(self._selected_row_indices, select_table=True, zoom_to_points=False)

	def _on_sort_indicator_changed(self, logical_index: int, order: Qt.SortOrder) -> None:
		if self._applying_sort:
			return
		self._current_sort_column = logical_index
		self._current_sort_order = order

	def _on_label_filter_changed(self) -> None:
		self._hide_labeled_active = self.hide_labeled_chk.isChecked()
		self._label_filter_active = self.label_filter_chk.isChecked()
		self._label_filter_text = self.label_filter_edit.text().strip()
		self.label_filter_edit.setEnabled(self._label_filter_active)
		self._set_table_model()
		if self._selected_row_indices:
			self._select_rows_by_indices(self._selected_row_indices, select_table=True, zoom_to_points=False)

	def _update_label_completer(self) -> None:
		labels = sorted(set(
			str(v) for v in self.df_positions["label"].dropna()
			if str(v).strip()
		))
		self._label_completer.setModel(QStringListModel(labels, self._label_completer))

	def _on_label_return_pressed(self) -> None:
		if self._selected_row_indices:
			self.apply_label_to_selected()
		elif self._selected_row_index is not None:
			self.save_entry()

	def _on_toggle_labeled_points(self, checked: bool):
		self._show_labeled_points = bool(checked)
		self._plot_positions()
		if self._selected_row_indices:
			self._draw_selection_markers(self._selected_row_indices)

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
		thread.finished.connect(lambda t=thread: self._active_threads.discard(t))
		self._load_thread = thread
		self._load_worker = worker
		self._active_threads.add(thread)
		if self.args.debug:
			logger.debug(f"Started position load worker thread {thread}. active threads: {len(self._active_threads)}")
		thread.start()

	def _on_load_thread_finished(self):
		self._load_thread = None
		self._load_worker = None
		if self._pending_close and not self._any_worker_running():
			self._pending_close = False
			self.close()

	def _shutdown_thread(self, thread: QThread | None, name: str):
		if thread is None:
			return True
		try:
			if not thread.isRunning():
				return True
		except RuntimeError as e:
			logger.error(f"RuntimeError checking thread.isRunning() for '{name}': {e} ({type(e)})")
			return True
		if thread.currentThread() is thread:
			return False
		logger.debug(f"PositionManagerWindow requesting stop for thread '{name}'")
		thread.requestInterruption()
		thread.quit()
		return False

	def _detach_running_threads_for_close(self):
		threads: set[QThread] = set()
		if self._thread_is_running(self._basemap_thread):
			threads.add(cast(QThread, self._basemap_thread))
		if self._thread_is_running(self._load_thread):
			threads.add(cast(QThread, self._load_thread))
		for t in list(self._active_threads):
			if self._thread_is_running(t):
				threads.add(t)
		if self.args.debug:
			logger.debug(f"Detaching {len(threads)} threads")
		for t in threads:
			_ORPHAN_QTHREADS.add(t)
			try:
				t.finished.connect(lambda thr=t: _release_orphan_thread(thr))
			except RuntimeError as e:
				logger.error(f"RuntimeError connecting finished signal to orphan release: {e} ({type(e)})")
				_release_orphan_thread(t)
		self._active_threads.clear()
		self._basemap_thread = None
		self._basemap_worker = None
		self._load_thread = None
		self._load_worker = None
		self._queued_basemap_request = None

	def _any_worker_running(self) -> bool:
		if self._thread_is_running(self._basemap_thread) or self._thread_is_running(self._load_thread):
			return True
		for t in list(self._active_threads):
			if self._thread_is_running(t):
				return True
		return False

	def _retry_pending_close(self):
		if not self._pending_close:
			return
		if self._any_worker_running():
			started = self._pending_close_started_at
			if started is not None and (time.monotonic() - started) >= 15.0:
				logger.warning("PositionManagerWindow forcing close; detaching still-running worker threads")
				self._detach_running_threads_for_close()
				self._pending_close = False
				self._pending_close_started_at = None
				self.close()
				return
			QTimer.singleShot(250, self._retry_pending_close)
			return
		self._pending_close = False
		self._pending_close_started_at = None
		self.close()

	def _on_positions_loaded(self, df: pd.DataFrame):
		self.df_positions = df.reset_index(drop=True)
		self._selected_row_index = None
		self._selected_row_indices = []
		self._clear_selection_markers()
		self._set_table_model()
		self._update_label_completer()
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
		logger.error(f'{self} {error_message}')
		QMessageBox.warning(self, "Load Failed", error_message)
		self.selected_info.setText("Failed to load positions")

	def _plot_positions(self):
		prev_bounds: tuple[float, float, float, float] | None = None
		if self._full_bounds is not None:
			try:
				prev_bounds = self._current_view_bounds()
			except Exception as e:
				logger.error(f"Failed to capture previous map bounds: {e} ({type(e)})")
				prev_bounds = None

		# Remove explicit artists before clearing axes to avoid Matplotlib remove() errors.
		self._clear_selection_markers()
		self._clear_point_labels()
		self.map_ax.clear()
		self._scatter_index_map.clear()
		self._basemap_artist = None
		self._visible_row_indices = set()

		if self.df_positions.empty:
			self._full_bounds = None
			self._set_basemap_status('no data')
			self.map_ax.set_title("No start/end points available")
			self.map_canvas.draw_idle()
			return

		plot_df = self.df_positions
		if not self._show_labeled_points:
			plot_df = plot_df[plot_df["label"].astype(str).str.strip() == ""]

		if plot_df.empty:
			self._full_bounds = None
			self._set_basemap_status('no points for current filter')
			self.map_ax.set_title("No points for current map filter")
			self.map_canvas.draw_idle()
			return

		self._visible_row_indices = set(int(i) for i in plot_df.index.tolist())

		start_df = plot_df[plot_df["pos_type"] == "start"]
		end_df = plot_df[plot_df["pos_type"] == "end"]

		if not start_df.empty:
			sizes = start_df["count"].clip(lower=1).astype(float) * 3.0 + 18.0
			sc_start = self.map_ax.scatter(start_df["x"], start_df["y"], s=sizes, c="tab:blue", alpha=0.85, label="startpos", picker=6, zorder=2)
			self._scatter_index_map[sc_start] = [int(i) for i in start_df.index.tolist()]

		if not end_df.empty:
			sizes = end_df["count"].clip(lower=1).astype(float) * 3.0 + 18.0
			sc_end = self.map_ax.scatter(end_df["x"], end_df["y"], s=sizes, c="tab:red", alpha=0.85, label="endpos", picker=6, zorder=2)
			self._scatter_index_map[sc_end] = [int(i) for i in end_df.index.tolist()]

		self._draw_point_labels(plot_df)

		xmin = float(plot_df["x"].min())
		xmax = float(plot_df["x"].max())
		ymin = float(plot_df["y"].min())
		ymax = float(plot_df["y"].max())
		dx = max(1.0, xmax - xmin)
		dy = max(1.0, ymax - ymin)
		pad_x = dx * 0.06
		pad_y = dy * 0.06
		self._full_bounds = (xmin - pad_x, xmax + pad_x, ymin - pad_y, ymax + pad_y)

		if prev_bounds is not None:
			self.map_ax.set_xlim(prev_bounds[0], prev_bounds[1])
			self.map_ax.set_ylim(prev_bounds[2], prev_bounds[3])
		else:
			self.map_ax.set_xlim(self._full_bounds[0], self._full_bounds[1])
			self.map_ax.set_ylim(self._full_bounds[2], self._full_bounds[3])
		self.map_ax.set_title("Position Manager: Start/End points")
		self.map_ax.legend(loc="upper right")
		self.map_ax.set_axis_off()

		if prev_bounds is not None:
			# Reload after an edit: preserve both the previous viewport and zoom level.
			self._refresh_basemap_for_current_view(target_zoom=self._current_basemap_zoom)
		else:
			# Initial load: derive zoom from the actual data extent so we don't request
			# hundreds of tiles for a continent-wide view.
			self._refresh_basemap_for_current_view()
		# Draw points immediately; basemap can arrive later.
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
		mouse_key = str(getattr(getattr(event, "mouseevent", None), "key", "") or "").lower()
		replace_selection = mouse_key in ("alt", "meta")
		if replace_selection:
			pending_rows, pending_zoom = [row_index], True
		elif row_index in self._selected_row_indices and len(self._selected_row_indices) > 1:
			pending_rows, pending_zoom = [r for r in self._selected_row_indices if r != row_index], False
		elif row_index in self._selected_row_indices:
			pending_rows, pending_zoom = [row_index], True
		else:
			pending_rows = list(dict.fromkeys(self._selected_row_indices + [row_index]))
			pending_zoom = len(pending_rows) == 1
		# Debounce rapid picks: store the latest args and restart an 80 ms timer so that
		# only the last click in a fast sequence triggers a selection update + map zoom.
		self._pending_pick_call = (pending_rows, pending_zoom)
		self._pick_debounce_timer.start()

	def _flush_pending_pick(self):
		if self._pending_pick_call is None:
			return
		rows, zoom = self._pending_pick_call
		self._pending_pick_call = None
		self._select_rows_by_indices(rows, select_table=True, zoom_to_points=zoom)

	def _on_table_selection_changed(self, selected, deselected):
		# Ignore selection-changed signals that we ourselves triggered while updating the
		# table inside _select_rows_by_indices — prevents a re-entrant cascade.
		if self._updating_selection:
			return
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
			if marker is None or getattr(marker, "axes", None) is None:
				continue
			try:
				marker.remove()
			except NotImplementedError as e:
				logger.debug(f"Selection marker already detached during redraw: {e} ({type(e)})")
			except Exception as e:
				logger.error(f"Failed to remove selection marker: {e} ({type(e)})")
		self._selected_markers = []

	def _clear_point_labels(self):
		if not self._point_label_artists:
			return
		for artist in self._point_label_artists:
			if artist is None or getattr(artist, "axes", None) is None:
				continue
			try:
				artist.remove()
			except NotImplementedError as e:
				logger.debug(f"Point label artist already detached during redraw: {e} ({type(e)})")
			except Exception as e:
				logger.error(f"Failed to remove point label artist: {e} ({type(e)})")
		self._point_label_artists = []

	def _draw_point_labels(self, source_df: pd.DataFrame | None = None):
		self._clear_point_labels()
		if not self._show_point_labels:
			return
		df = self.df_positions if source_df is None else source_df
		if df.empty:
			return
		for row in df.itertuples(index=False):
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

		tmp = self.df_positions.loc[source_rows, ['latitude', 'longitude']].copy()
		tmp['lat_bucket'] = tmp['latitude'].round(3)
		tmp['lon_bucket'] = tmp['longitude'].round(3)
		tmp['_src'] = tmp.index
		tmp.sort_values(by=['lat_bucket', 'lon_bucket', 'latitude', 'longitude'], inplace=True, kind='mergesort')
		self._table_model.set_view_order([int(v) for v in tmp['_src'].tolist()])
		# Custom sort — clear column-sort tracking so it isn't inadvertently restored.
		self._current_sort_column = -1
		self.positions_table.horizontalHeader().setSortIndicator(-1, Qt.SortOrder.AscendingOrder)
		if self._selected_row_indices:
			self._select_rows_by_indices(self._selected_row_indices, select_table=True, zoom_to_points=False)

	def _draw_selection_markers(self, row_indices: list[int]):
		self._clear_selection_markers()
		for idx in row_indices:
			if self._visible_row_indices and idx not in self._visible_row_indices:
				continue
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
			self._updating_selection = True
			try:
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
			finally:
				self._updating_selection = False

		rows_data = []
		for ridx in clean_rows:
			row = self.df_positions.loc[ridx]
			if isinstance(row, pd.DataFrame):
				row = row.iloc[0]
			rows_data.append(cast(dict[str, Any], row.to_dict()))

		if len(rows_data) == 1:
			row_data = rows_data[0]
			self.pos_type_combo.setCurrentText(str(row_data.get('pos_type', 'start')))
			self.pos_id_spin.setValue(int(row_data.get('pos_id', 1)))
			self.lat_spin.setValue(float(row_data.get('latitude', 0.0)))
			self.lon_spin.setValue(float(row_data.get('longitude', 0.0)))
			self.count_spin.setValue(int(row_data.get('count', 0)))
			self.label_edit.setText(str(row_data.get('label', '')))
			self.selected_info.setText(
				f'Selected {str(row_data.get('pos_type', ''))} point #{int(row_data.get('pos_id', 0))}  |  '
				f'lat={float(row_data.get('latitude', 0.0)):.6f}, lon={float(row_data.get('longitude', 0.0)):.6f}, count={int(row_data.get('count', 0))}'
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
		if self._any_worker_running():
			if not self._pending_close:
				logger.warning("PositionManagerWindow close deferred until workers stop")
				self._pending_close_started_at = time.monotonic()
			elif self._pending_close_started_at is not None and (time.monotonic() - self._pending_close_started_at) >= 2.5:
				logger.warning("PositionManagerWindow close forcing detach of stuck worker threads")
				self._detach_running_threads_for_close()
				self._pending_close = False
				self._pending_close_started_at = None
				super().closeEvent(event)
				return
			self._pending_close = True
			self._cancel_basemap_thread("window-close")
			self._shutdown_thread(self._basemap_thread, "basemap")
			self._shutdown_thread(self._load_thread, "positions_load")
			for idx, t in enumerate(list(self._active_threads)):
				self._shutdown_thread(t, f"active_{idx}")
			QTimer.singleShot(200, self._retry_pending_close)
			event.ignore()
			return
		self._pending_close = False
		self._pending_close_started_at = None
		super().closeEvent(event)
