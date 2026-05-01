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
	QHBoxLayout, QLabel, QComboBox, QFrame
)
from PySide6.QtGui import QFont
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
from utils import get_engine_session

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
			img, ext = ctx.bounds2img(  # type: ignore[call-arg]
				west,
				south,
				east,
				north,
				zoom=cast(Any, self.zoom),
			)
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
		# engine = create_engine(dburl)
		self.engine = create_engine(dburl)
		database_init(self.engine)
		self._torqlogs_norm_to_actual = self._build_torqlogs_column_map()
		self._resolved_torqlogs_columns = {
			name: actual
			for name in ['latitude', 'longitude', 'speedobdkmh']
			if (actual := self._resolve_actual_torqlogs_column(name)) is not None
		}
		self._trip_plot_cache: dict[tuple[int, str], dict[str, list[float]]] = {}
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
		logger.debug(f"Resolved torqlogs columns: {self._resolved_torqlogs_columns}")
		zoom_layout = QHBoxLayout()
		zoom_label = QLabel("Zoom:")
		self.zoom_combo = QComboBox()
		zoom_levels = [str(z) for z in range(10, 19)]  # Typical OSM zoom levels
		self.zoom_combo.addItems(zoom_levels)
		self.zoom_combo.setCurrentText('10')  # Default zoom
		self.zoom_combo.setFixedWidth(60)
		self.zoom_combo.setMaximumHeight(25)
		self.zoom_combo.currentTextChanged.connect(self.on_zoom_changed)

		zoom_layout.addWidget(zoom_label)
		zoom_layout.addWidget(self.zoom_combo)
		zoom_layout.addStretch()
		zoom_layout.setSpacing(10)
		zoom_layout.setContentsMargins(10, 5, 10, 5)

		# Create colormap selection controls
		colormap_layout = QHBoxLayout()
		colormap_label = QLabel("Colormap:")
		self.colormap_combo = QComboBox()
		metric_label = QLabel("Metric:")
		self.metric_combo = QComboBox()

		# Add popular qualitative colormaps
		qualitative_maps = ['Set1', 'tab10', 'tab20', 'Dark2', 'Pastel1', 'Pastel2', 'Set2', 'Set3', 'Accent']
		# Add some sequential colormaps
		sequential_maps = ['viridis', 'plasma', 'inferno', 'magma', 'Blues', 'Greens', 'Reds', 'YlOrRd']

		all_maps = qualitative_maps + sequential_maps
		self.colormap_combo.addItems(all_maps)
		self.colormap_combo.setCurrentText('Set1')  # Set default
		self.colormap_combo.currentTextChanged.connect(self.on_colormap_changed)

		metric_columns = []
		if self._resolve_actual_torqlogs_column('speedobdkmh'):
			metric_columns = ['speedobdkmh']
		self.metric_combo.addItems(metric_columns)
		if metric_columns:
			self.metric_combo.setCurrentText(metric_columns[0])
		self.metric_combo.currentTextChanged.connect(self.on_metric_changed)

		# Adjust size and appearance of the combo box
		self.colormap_combo.setFixedWidth(120)  # Set fixed width
		self.colormap_combo.setMaximumHeight(25)  # Limit height
		self.metric_combo.setFixedWidth(200)
		self.metric_combo.setMaximumHeight(25)

		colormap_layout.addLayout(zoom_layout)
		colormap_layout.addWidget(metric_label)
		colormap_layout.addWidget(self.metric_combo)
		colormap_layout.addWidget(colormap_label)
		colormap_layout.addWidget(self.colormap_combo)
		# colormap_layout.addSpacing(20)
		colormap_layout.addStretch()  # Push controls to the left

		# Adjust layout spacing and margins
		colormap_layout.setSpacing(10)  # Space between widgets
		colormap_layout.setContentsMargins(10, 5, 10, 5)  # left, top, right, bottom margins

		# Create right panel with map and controls
		right_panel = QWidget()
		right_layout = QVBoxLayout(right_panel)
		plot_and_stats = QSplitter(Qt.Orientation.Horizontal)
		stats_panel = QFrame()
		stats_layout = QVBoxLayout(stats_panel)
		stats_title = QLabel("Selected Trip Stats")
		stats_title_font = QFont()
		stats_title_font.setPointSize(10)
		stats_title_font.setBold(True)
		stats_title.setFont(stats_title_font)
		self.stats_label = QLabel("No trip selected")
		self.stats_label.setWordWrap(True)
		self.stats_label.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
		stats_layout.addWidget(stats_title)
		stats_layout.addWidget(self.stats_label)
		stats_layout.addStretch()

		# Add map canvas first (give it more space)
		plot_and_stats.addWidget(self.map_canvas)
		plot_and_stats.addWidget(stats_panel)
		plot_and_stats.setSizes([700, 260])
		right_layout.addWidget(plot_and_stats, stretch=10)  # Give map area most of the space

		# Add colormap controls at the bottom (minimal space)
		colormap_widget = QWidget()
		colormap_widget.setLayout(colormap_layout)
		colormap_widget.setMaximumHeight(40)  # Limit height of control panel
		right_layout.addWidget(colormap_widget, stretch=1)   # Give controls 1 part of space

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
		logger.debug("MainWindow initialized and UI set up")

		# self.table_model = PandasModel(self.df_files)
		# self.table.setModel(self.table_model)
		# self.table.setSortingEnabled(True)
		# # self.table.setSelectionBehavior(self.table.SelectRows)
		# self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
		# self.table.selectionModel().selectionChanged.connect(self.on_row_selected)

	def __repr__(self):
		return f"<MainWindow with {len(self.df_trips)} trips loaded>"
	
	def on_colormap_changed(self, colormap_name):
		"""Called when user changes the colormap selection"""
		# Debounce to avoid repeated heavy redraws on rapid UI changes.
		self._plot_refresh_timer.start(200)

	def on_zoom_changed(self, zoom_level):
		"""Called when user changes map zoom"""
		# Debounce zoom updates to avoid blocking UI with repeated basemap fetches.
		self._plot_refresh_timer.start(300)

	def on_metric_changed(self, metric_name):
		"""Called when user changes plotted metric"""
		self._plot_refresh_timer.start(200)

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

		current = self.metric_combo.currentText()
		self.metric_combo.blockSignals(True)
		self.metric_combo.clear()
		self.metric_combo.addItems(metric_columns)
		if current in metric_columns:
			self.metric_combo.setCurrentText(current)
		else:
			self.metric_combo.setCurrentText(metric_columns[0])
		self.metric_combo.blockSignals(False)
		logger.debug(f"Populated metric columns: {len(metric_columns)}")

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

	def _load_trip_plot_data(self, fileid: int, metric_name: str) -> dict[str, list[float]] | None:
		cache_key = (fileid, metric_name)
		if cache_key in self._trip_plot_cache:
			return self._trip_plot_cache[cache_key]

		lat_col = self._resolved_torqlogs_columns.get('latitude')
		lon_col = self._resolved_torqlogs_columns.get('longitude')
		speed_col_name = self._resolve_actual_torqlogs_column(metric_name)
		if not (lat_col and lon_col and speed_col_name):
			return None

		q = (
			f'SELECT "{lon_col}" AS longitude, "{lat_col}" AS latitude, '
			f'"{speed_col_name}" AS selectedmetric FROM torqlogs WHERE fileid = {int(fileid)}'
		)
		df_part = pd.read_sql(q, self.engine)
		if df_part.empty:
			self._trip_plot_cache[cache_key] = {"x": [], "y": [], "speed": []}
			return self._trip_plot_cache[cache_key]
		try:
			gdf = gpd.GeoDataFrame(df_part, geometry=[Point(xy) for xy in zip(df_part['longitude'], df_part['latitude'])], crs="EPSG:4326",).to_crs(epsg=3857)
		except KeyError as e:
			logger.error(f"Missing expected columns in trip data: {e} fileid={fileid} metric_name={metric_name}")
			return None
		speed_series = pd.to_numeric(df_part['selectedmetric'], errors='coerce').fillna(0)

		payload: dict[str, list[float]] = {
			"x": gdf.geometry.x.tolist(),
			"y": gdf.geometry.y.tolist(),
			"speed": speed_series.tolist(),
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
		mpimg.imsave(buf, img_artists[0].get_array(), format='png')
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
		colormap_name = self.colormap_combo.currentText()
		selected_metric = self.metric_combo.currentText()
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
			speed_max = speed_vals.max()
			colors = [(
				min(1, base_color[0] + 0.5 * (v / speed_max if speed_max > 0 else 0)),
				min(1, base_color[1] + 0.5 * (v / speed_max if speed_max > 0 else 0)),
				min(1, base_color[2] + 0.5 * (v / speed_max if speed_max > 0 else 0)),
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
		self._update_stats_panel(fileids, all_metric_values, all_x, all_y, selected_metric)

	def _update_stats_panel(self, fileids: list[int], metric_values: list[float], all_x: list[float], all_y: list[float], metric_name: str):
		trip_count = len(fileids)
		point_count = len(all_x)
		metric_series = pd.Series(metric_values, dtype="float64") if metric_values else pd.Series(dtype="float64")
		metric_min = float(metric_series.min()) if not metric_series.empty else 0.0
		metric_avg = float(metric_series.mean()) if not metric_series.empty else 0.0
		metric_max = float(metric_series.max()) if not metric_series.empty else 0.0
		bounds = self._compute_plot_bounds(all_x, all_y)
		if bounds:
			xmin, xmax, ymin, ymax = bounds
			bounds_line = f"Bounds: x[{xmin:.0f}, {xmax:.0f}] y[{ymin:.0f}, {ymax:.0f}]"
		else:
			bounds_line = "Bounds: n/a"

		fileid_text = ", ".join(str(fid) for fid in fileids[:10])
		if len(fileids) > 10:
			fileid_text += ", ..."

		self.stats_label.setText(
			f"Trips selected: {trip_count}\n"
			f"Fileids: {fileid_text}\n"
			f"Plotted points: {point_count}\n"
			f"Metric: {metric_name}\n"
			f"Min / Avg / Max: {metric_min:.2f} / {metric_avg:.2f} / {metric_max:.2f}\n"
			f"{bounds_line}"
		)

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
			self._save_cached_map_image(
				cast(list[int], ctx["fileids"]),
				cast(int, ctx["zoom"]),
				cast(str, ctx["colormap"]),
				cast(str, ctx["metric"]),
				tuple(float(v) for v in ext),
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
