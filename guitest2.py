#!/usr/bin/python3
from loguru import logger
import contextily as ctx
import geopandas as gpd
from shapely.geometry import Point
import sys
import io
import pandas as pd
from typing import Any, cast
from PySide6.QtWidgets import (
	QApplication, QMainWindow, QTableView, QVBoxLayout, QWidget, QSplitter,
	QHBoxLayout, QLabel, QComboBox, QFrame, QListWidget, QListWidgetItem,
	QScrollArea, QFileDialog, QMessageBox
)
from PySide6.QtGui import QFont, QAction
from PySide6.QtWidgets import QAbstractItemView
from PySide6.QtCore import Qt, QAbstractTableModel, QModelIndex, QPersistentModelIndex, QTimer, QObject, Signal, QThread
from PySide6.QtGui import QCloseEvent
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
import matplotlib
matplotlib.use("QtAgg")
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
# from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
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
			img, ext = ctx.bounds2img(west, south, east, north, zoom=cast(Any, self.zoom),)
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
		self._current_colormap = 'Set1'
		self._plot_refresh_timer = QTimer(self)
		self._plot_refresh_timer.setSingleShot(True)
		self._plot_refresh_timer.timeout.connect(self.refresh_plot)
		self._basemap_request_id = 0
		self._basemap_request_context: dict[int, dict[str, object]] = {}
		self._basemap_thread: QThread | None = None
		self._basemap_worker: BasemapWorker | None = None
		self._initial_trips_thread: QThread | None = None
		self._initial_trips_worker: TripListWorker | None = None
		self._active_threads: set[QThread] = set()
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
		zoom_layout.addWidget(zoom_label)
		zoom_layout.addWidget(self.zoom_combo)
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

	def _set_colormap(self, colormap_name: str):
		self._current_colormap = colormap_name
		for name, action in self._colormap_actions.items():
			action.setChecked(name == colormap_name)
		self._plot_refresh_timer.start(200)

	def on_zoom_changed(self, zoom_level):
		"""Called when user changes map zoom"""
		# Debounce zoom updates to avoid blocking UI with repeated basemap fetches.
		self._plot_refresh_timer.start(300)

	def on_metric_selection_changed(self):
		"""Called when user changes the metric selection in the list."""
		if self._get_selected_metrics():
			self._plot_refresh_timer.start(200)

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

	def _load_trip_plot_data(self, fileid: int, metric_name: str) -> dict[str, list] | None:
		cache_key = (fileid, metric_name)
		if cache_key in self._trip_plot_cache:
			return self._trip_plot_cache[cache_key]

		lat_col = self._resolved_torqlogs_columns.get('latitude')
		lon_col = self._resolved_torqlogs_columns.get('longitude')
		speed_col_name = self._resolve_actual_torqlogs_column(metric_name)
		time_col = (self._resolve_actual_torqlogs_column('gpstime')
					or self._resolve_actual_torqlogs_column('devicetime'))
		if not (lat_col and lon_col and speed_col_name):
			return None

		time_select = f', "{time_col}" AS metric_time' if time_col else ''
		time_order = f' ORDER BY "{time_col}"' if time_col else ''
		q = (
			f'SELECT "{lon_col}" AS longitude, "{lat_col}" AS latitude, '
			f'"{speed_col_name}" AS selectedmetric{time_select} FROM torqlogs WHERE fileid = {int(fileid)}{time_order}'
		)
		df_part = pd.read_sql(q, self.engine)
		if df_part.empty:
			self._trip_plot_cache[cache_key] = {"x": [], "y": [], "speed": [], "time": []}
			return self._trip_plot_cache[cache_key]
		try:
			gdf = gpd.GeoDataFrame(df_part, geometry=[Point(xy) for xy in zip(df_part['longitude'], df_part['latitude'])], crs="EPSG:4326",).to_crs(epsg=3857)
		except KeyError as e:
			logger.error(f"Missing expected columns in trip data: {e} fileid={fileid} metric_name={metric_name}")
			return None
		speed_series = pd.to_numeric(df_part['selectedmetric'], errors='coerce').fillna(0)

		time_values: list = []
		if 'metric_time' in df_part.columns:
			time_values = pd.to_datetime(df_part['metric_time'], errors='coerce').tolist()

		payload: dict[str, list] = {
			"x": gdf.geometry.x.tolist(),
			"y": gdf.geometry.y.tolist(),
			"speed": speed_series.tolist(),
			"time": time_values,
		}
		self._trip_plot_cache[cache_key] = payload
		logger.debug(f"Loaded trip plot data for fileid={fileid}, metric_name={metric_name}, points={len(payload['x'])}")
		return payload

	def _selection_key(self, fileids: list[int], metric_name: str) -> str:
		return f"metric={metric_name}|" + ",".join(str(fid) for fid in sorted(fileids))

	def _cache_fileid(self, fileids: list[int]) -> int | None:
		return int(fileids[0]) if len(fileids) == 1 else None

	def _load_cached_map_image(self, fileids: list[int], zoom: int, colormap: str, metric_name: str) -> tuple[bytes, tuple[float, float, float, float]] | None:
		selection_key = self._selection_key(fileids, metric_name)
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
				"selection_key": selection_key,
				"zoom": zoom,
				"colormap": colormap,
			}).first()
		if row and all(v is not None for v in row[1:5]):
			return row[0], (float(row[1]), float(row[2]), float(row[3]), float(row[4]))
		return None

	def _save_cached_map_image(self, fileids: list[int], zoom: int, colormap: str, metric_name: str, ext: tuple[float, float, float, float]):
		selection_key = self._selection_key(fileids, metric_name)
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
		logger.debug(f"Saved cached map image for selection_key={selection_key}, zoom={zoom}, colormap={colormap}")

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

	def _plot_for_rows(self, rows):
		fileids = self._get_selected_fileids(rows)
		if not fileids:
			self.stats_label.setText("No trip selected")
			return

		zoom = int(self.zoom_combo.currentText())
		colormap_name = self._current_colormap
		selected_metrics = self._get_selected_metrics() or ['speedobdkmh']
		selected_metric = selected_metrics[0]
		cached_payload = self._load_cached_map_image(fileids, zoom, colormap_name, selected_metric)

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

			sizes = speed_vals.clip(lower=1, upper=50)
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
		if bounds:
			xmin, xmax, ymin, ymax = bounds
			self.map_canvas.ax.set_xlim(xmin, xmax)
			self.map_canvas.ax.set_ylim(ymin, ymax)

		if plots and cached_payload:
			cached_img, cached_ext = cached_payload
			img = mpimg.imread(io.BytesIO(cached_img), format='png')
			self.map_canvas.ax.imshow(img, extent=cached_ext, interpolation='bilinear', zorder=0)
		elif plots:
			if bounds:
				self._start_async_basemap(bounds, zoom, fileids, colormap_name, selected_metric)
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

		# Load trip metadata from database
		trip_info = self._load_trip_metadata(fileids)
		all_metric_stats = self._load_all_metric_stats(fileids)

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
