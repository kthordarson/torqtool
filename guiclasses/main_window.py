import io
from typing import Any, cast
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from loguru import logger
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from PySide6.QtWidgets import (
	QApplication, QMainWindow, QTableView, QVBoxLayout, QWidget, QSplitter,
	QHBoxLayout, QLabel, QComboBox, QScrollArea, QFileDialog, QMessageBox,
	QSlider, QLineEdit, QPushButton, QInputDialog, QAbstractItemView,
	QListWidget, QListWidgetItem,
)
from PySide6.QtGui import QFont, QAction, QCloseEvent
from PySide6.QtCore import Qt, QTimer, QThread

from datamodels import database_init
from schemas import dataschema
from metric_analysis import categorize_metric, get_analysis_suggestion, group_metrics_by_category
from .map_canvas import MapCanvas
from .time_series_canvas import TimeSeriesCanvas
from .basemap_worker import BasemapWorker
from .trip_list_worker import TripListWorker
from .position_manager_window import PositionManagerWindow
from .pandas_model import PandasModel
from ._helpers import _normalize_col_name, format_duration


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
		self._start_end_overlay_data: list[dict[str, Any]] = []
		self._mw_full_bounds: tuple[float, float, float, float] | None = None
		self._mw_current_fileids: list[int] = []
		self._mw_last_metric: str = 'speedobdkmh'
		self._valid_metric_columns_cache: list[str] | None = None
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
		self.map_canvas.mpl_connect("pick_event", self._on_map_pick)
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
		self._mw_zoom_in_btn = QPushButton("Zoom in")
		self._mw_zoom_in_btn.setFixedHeight(24)
		self._mw_zoom_in_btn.setFixedWidth(68)
		self._mw_zoom_in_btn.clicked.connect(self._mw_zoom_in)
		self._mw_zoom_out_btn = QPushButton("Zoom out")
		self._mw_zoom_out_btn.setFixedHeight(24)
		self._mw_zoom_out_btn.setFixedWidth(72)
		self._mw_zoom_out_btn.clicked.connect(self._mw_zoom_out)
		self._mw_zoom_full_btn = QPushButton("Full")
		self._mw_zoom_full_btn.setFixedHeight(24)
		self._mw_zoom_full_btn.setFixedWidth(44)
		self._mw_zoom_full_btn.clicked.connect(self._mw_zoom_full)
		zoom_layout.addWidget(self._mw_zoom_in_btn)
		zoom_layout.addWidget(self._mw_zoom_out_btn)
		zoom_layout.addWidget(self._mw_zoom_full_btn)
		self._mw_reload_map_btn = QPushButton("Reload map")
		self._mw_reload_map_btn.setFixedHeight(24)
		self._mw_reload_map_btn.setFixedWidth(84)
		self._mw_reload_map_btn.clicked.connect(self._mw_force_reload_basemap)
		zoom_layout.addWidget(self._mw_reload_map_btn)
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
		self.metric_list.itemSelectionChanged.connect(self.on_metric_selection_changed)
		metric_panel_layout.addWidget(metric_title)
		metric_panel_layout.addWidget(self.metric_list)

		# Stats panel (scrollable)
		from PySide6.QtWidgets import QFrame
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
			if artist is None or getattr(artist, "axes", None) is None:
				continue
			try:
				artist.remove()
			except NotImplementedError as e:
				logger.debug(f"Start/end overlay artist already detached during redraw: {e} ({type(e)})")
			except Exception as e:
				logger.error(f"Failed to remove start/end overlay artist: {e} ({type(e)})")
		for lbl in self._start_end_overlay_labels:
			if lbl is None or getattr(lbl, "axes", None) is None:
				continue
			try:
				lbl.remove()
			except NotImplementedError as e:
				logger.debug(f"Start/end overlay label already detached during redraw: {e} ({type(e)})")
			except Exception as e:
				logger.error(f"Failed to remove start/end overlay label: {e} ({type(e)})")
		self._start_end_overlay_artists = []
		self._start_end_overlay_labels = []
		self._start_end_overlay_data = []

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
			artist = self.map_canvas.ax.scatter([x], [y], s=size, c=color, marker="D", edgecolors="black", linewidths=0.5, alpha=alpha, zorder=4, picker=8)
			self._start_end_overlay_artists.append(artist)
			self._start_end_overlay_data.append(dict(point))
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
		if metrics:
			return metrics[0]

		for row in range(self.metric_list.count()):
			item = self.metric_list.item(row)
			if item is not None and item.flags() & Qt.ItemFlag.ItemIsSelectable:
				return item.text()

		valid_metrics = self._get_metric_columns_with_valid_data()
		return valid_metrics[0] if valid_metrics else ""

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
		thread.finished.connect(lambda: setattr(self, '_initial_trips_thread', None))

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
		prev_selected: set[str] = {
			item.text() for item in self.metric_list.selectedItems()
			if item.flags() & Qt.ItemFlag.ItemIsSelectable
		}
		self.metric_list.blockSignals(True)
		self.metric_list.clear()
		metric_columns = self._get_metric_columns_with_valid_data()
		if not metric_columns:
			empty_item = QListWidgetItem("No metrics with valid data")
			empty_item.setFlags(Qt.ItemFlag.NoItemFlags)
			empty_item.setForeground(Qt.GlobalColor.darkGray)
			self.metric_list.addItem(empty_item)
			self.metric_list.blockSignals(False)
			logger.warning("No metrics with valid non-zero data were found in torqlogs")
			return

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
		if self._valid_metric_columns_cache is not None:
			return list(self._valid_metric_columns_cache)

		requested = sorted(dataschema.keys())
		numeric_cols = self._get_torqlogs_numeric_columns()
		column_pairs: list[tuple[str, str]] = []
		for req in requested:
			actual = self._resolve_actual_torqlogs_column(req)
			if actual and actual in numeric_cols:
				column_pairs.append((req, actual))

		if not column_pairs:
			self._valid_metric_columns_cache = []
			return []

		select_parts: list[str] = []
		for idx, (_, actual_col) in enumerate(column_pairs):
			# Include only metrics with at least one non-null, non-zero numeric value.
			select_parts.append(
				f'MAX(CASE WHEN "{actual_col}" IS NOT NULL THEN ABS(CAST("{actual_col}" AS FLOAT)) END) AS "_m_{idx}"'
			)

		query = f"SELECT {', '.join(select_parts)} FROM torqlogs"
		valid_metrics: list[str] = []
		try:
			df = pd.read_sql(query, self.engine)
			if not df.empty:
				row = df.iloc[0]
				for idx, (requested_col, _) in enumerate(column_pairs):
					value = row.get(f"_m_{idx}")
					if value is None or pd.isna(value):
						continue
					if float(value) > 0.0:
						valid_metrics.append(requested_col)
		except Exception as e:
			logger.warning(f"Failed to evaluate valid metric columns: {e} ({type(e)})")

		self._valid_metric_columns_cache = valid_metrics
		return list(valid_metrics)

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
		selected_metrics = self._get_selected_metrics()
		if not selected_metrics:
			fallback_metric = self._get_selected_metric()
			selected_metrics = [fallback_metric] if fallback_metric else []
		if not selected_metrics:
			self.stats_label.setText("No valid metrics available for plotting")
			return
		selected_metric = selected_metrics[0]
		cached_payload: tuple[bytes, tuple[float, float, float, float]] | None = None

		# Remove overlay artists before clearing axes so remove() has valid artist owners.
		self._clear_start_end_overlays()
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
				if self.args.debug:
					logger.warning(f"No plot data for fileid={fileid}, metric={selected_metric}")
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
			self._mw_full_bounds = bounds
			self._mw_current_fileids = list(fileids)
			self._mw_last_metric = selected_metric

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
					if self.args.debug:
						logger.warning(f"No data for timeseries plot: fileid={fileid}, metric={metric_name}")
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
		except RuntimeError as e:
			logger.error(f"RuntimeError checking thread.isRunning() for '{name}': {e} ({type(e)})")
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

	def _mw_force_reload_basemap(self):
		"""Evict DB cache for the current trip selection and re-fetch basemap from network."""
		if not self._mw_current_fileids:
			return
		files_part = ",".join(str(fid) for fid in sorted(self._mw_current_fileids))
		pattern = f"{self._map_cache_version}|%{files_part}"
		try:
			with self.engine.begin() as conn:
				conn.execute(
					text("DELETE FROM mapimagecache WHERE selection_key LIKE :pattern"),
					{"pattern": pattern},
				)
			logger.debug(f"MW: cleared map cache for fileids={self._mw_current_fileids}")
		except Exception as e:
			logger.warning(f"MW: could not clear map cache from DB: {e} ({type(e)})")
		self.refresh_plot()

	def _mw_zoom_in(self):
		ax = self.map_canvas.ax
		x0, x1 = ax.get_xlim()
		y0, y1 = ax.get_ylim()
		cx = (x0 + x1) / 2.0
		cy = (y0 + y1) / 2.0
		span_x = (x1 - x0) * 0.65
		span_y = (y1 - y0) * 0.65
		new_half_x = max(span_x, 1.0) / 2.0
		new_half_y = max(span_y, 1.0) / 2.0
		ax.set_xlim(cx - new_half_x, cx + new_half_x)
		ax.set_ylim(cy - new_half_y, cy + new_half_y)
		self.map_canvas.draw_idle()

	def _mw_zoom_out(self):
		ax = self.map_canvas.ax
		x0, x1 = ax.get_xlim()
		y0, y1 = ax.get_ylim()
		cx = (x0 + x1) / 2.0
		cy = (y0 + y1) / 2.0
		new_half_x = (x1 - x0) * 0.725
		new_half_y = (y1 - y0) * 0.725
		if self._mw_full_bounds:
			bx0, bx1, by0, by1 = self._mw_full_bounds
			max_half_x = (bx1 - bx0) * 1.0
			max_half_y = (by1 - by0) * 1.0
			new_half_x = min(new_half_x, max_half_x)
			new_half_y = min(new_half_y, max_half_y)
		ax.set_xlim(cx - new_half_x, cx + new_half_x)
		ax.set_ylim(cy - new_half_y, cy + new_half_y)
		self.map_canvas.draw_idle()

	def _mw_zoom_full(self):
		if not self._mw_full_bounds:
			return
		xmin, xmax, ymin, ymax = self._mw_full_bounds
		ax = self.map_canvas.ax
		ax.set_xlim(xmin, xmax)
		ax.set_ylim(ymin, ymax)
		self.map_canvas.draw_idle()

	def _on_map_pick(self, event):
		if event.artist not in self._start_end_overlay_artists:
			return
		try:
			idx = self._start_end_overlay_artists.index(event.artist)
		except ValueError:
			return
		if idx >= len(self._start_end_overlay_data):
			return
		point = self._start_end_overlay_data[idx]
		pos_type = str(point.get("pos_type", ""))
		pos_id = int(point.get("pos_id", 0))
		current_label = str(point.get("label", "") or "")
		prefix = "S" if pos_type == "start" else "E"
		new_label, ok = QInputDialog.getText(
			self,
			f"Edit label — {prefix}{pos_id}",
			f"Label for {pos_type} point #{pos_id}:",
			QLineEdit.EchoMode.Normal,
			current_label,
		)
		if not ok:
			return
		if self._save_start_end_label(pos_type, pos_id, new_label):
			point["label"] = new_label.strip()
			rows = sorted(set(index.row() for index in self.table.selectionModel().selectedRows()))
			if rows:
				self._plot_refresh_timer.start(50)

	def _save_start_end_label(self, pos_type: str, pos_id: int, label: str) -> bool:
		table = "startpos" if pos_type == "start" else "endpos"
		id_col = "startid" if pos_type == "start" else "endid"
		label_value: str | None = label.strip() or None
		try:
			with self.engine.begin() as conn:
				conn.execute(
					text(f"UPDATE {table} SET label = :label WHERE {id_col} = :pos_id"),
					{"label": label_value, "pos_id": pos_id},
				)
			logger.debug(f"Saved label for {pos_type} #{pos_id}: '{label_value}'")
			return True
		except Exception as e:
			logger.error(f"Failed to save label for {pos_type} #{pos_id}: {e} ({type(e)})")
			return False

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
