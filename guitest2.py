#!/usr/bin/python3
import contextily as ctx
import geopandas as gpd
from shapely.geometry import Point
import sys
import io
import pandas as pd
from typing import Any, cast
from PySide6.QtWidgets import (
	QApplication, QMainWindow, QTableView, QVBoxLayout, QWidget, QSplitter,
	QHBoxLayout, QLabel, QComboBox
)
from PySide6.QtGui import QFont
from PySide6.QtWidgets import QAbstractItemView
from PySide6.QtCore import Qt, QAbstractTableModel, QModelIndex, QPersistentModelIndex, QTimer, QObject, Signal, QThread
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import matplotlib
matplotlib.use("QtAgg")
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
# from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from datamodels import database_init

DB_PATH = "sqlite:///torqdata.db"


def _normalize_col_name(value: str) -> str:
	return "".join(ch.lower() for ch in str(value) if ch.isalnum())


def _resolve_torqlogs_columns(engine, requested_columns: list[str]) -> dict[str, str]:
	with engine.connect() as conn:
		rows = conn.execute(text("PRAGMA table_info(torqlogs)")).all()
	actual_columns = [row[1] for row in rows]
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


class BasemapWorker(QObject):
	finished = Signal(object, object, int)
	error = Signal(str, int)

	def __init__(self, bounds, zoom: int, request_id: int):
		super().__init__()
		self.bounds = bounds
		self.zoom = zoom
		self.request_id = request_id

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
		except Exception as e:
			self.error.emit(f'{e} {type(e)}', self.request_id)

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
	def __init__(self):
		super().__init__()
		self.setWindowTitle("TorqFiles Viewer")
		# Set up SQLAlchemy session
		self.engine = create_engine(DB_PATH)
		database_init(self.engine)
		self._resolved_torqlogs_columns = _resolve_torqlogs_columns(
			self.engine,
			['latitude', 'longitude', 'speedobdkmh']
		)
		self._trip_plot_cache: dict[int, dict[str, list[float]]] = {}
		self._plot_refresh_timer = QTimer(self)
		self._plot_refresh_timer.setSingleShot(True)
		self._plot_refresh_timer.timeout.connect(self.refresh_plot)
		self._basemap_request_id = 0
		self._basemap_request_context: dict[int, dict[str, object]] = {}
		self._basemap_thread: QThread | None = None
		self._basemap_worker: BasemapWorker | None = None
		self.Session = sessionmaker(bind=self.engine)
		self.session = self.Session()

		# Set up UI
		splitter = QSplitter(Qt.Orientation.Horizontal)
		self.table = QTableView()
		self.map_canvas = MapCanvas()

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

		# Add popular qualitative colormaps
		qualitative_maps = ['Set1', 'tab10', 'tab20', 'Dark2', 'Pastel1', 'Pastel2', 'Set2', 'Set3', 'Accent']
		# Add some sequential colormaps
		sequential_maps = ['viridis', 'plasma', 'inferno', 'magma', 'Blues', 'Greens', 'Reds', 'YlOrRd']

		all_maps = qualitative_maps + sequential_maps
		self.colormap_combo.addItems(all_maps)
		self.colormap_combo.setCurrentText('Set1')  # Set default
		self.colormap_combo.currentTextChanged.connect(self.on_colormap_changed)

		# Adjust size and appearance of the combo box
		self.colormap_combo.setFixedWidth(120)  # Set fixed width
		self.colormap_combo.setMaximumHeight(25)  # Limit height

		colormap_layout.addLayout(zoom_layout)
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

		# Add map canvas first (give it more space)
		right_layout.addWidget(self.map_canvas, stretch=10)  # Give map 10 parts of space

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

		# Load TorqFiles into a pandas DataFrame
		# self.df_files = pd.read_sql(self.session.query(TorqFile).statement, self.engine)
		# self.df_files = pd.read_sql(self.session.query(TorqFile).statement, self.engine, parse_dates=False)
		self.df_files = pd.read_sql("SELECT fileid,trip_start,trip_duration FROM torqfiles", self.engine)
		self.df_files['trip_start'] = pd.to_datetime(self.df_files['trip_start'], errors='coerce')
		self.df_files['trip_start'] = self.df_files['trip_start'].dt.strftime('%Y-%m-%d %H:%M')
		self.df_files['trip_duration'] = self.df_files['trip_duration'].apply(format_duration)
		self.df_files.set_index('fileid', inplace=True)

		self.df_trips = pd.read_sql("SELECT id,fileid,trip_distance,tripdate,time FROM torqtrips", self.engine)
		self.df_trips['tripdate'] = pd.to_datetime(self.df_trips['tripdate'], errors='coerce')
		self.df_trips['tripdate'] = self.df_trips['tripdate'].dt.strftime('%Y-%m-%d %H:%M')
		self.df_trips['time'] = self.df_trips['time'].apply(format_duration)
		self.df_trips['trip_distance'] = self.df_trips['trip_distance'].apply(lambda x: f"{x/1000:.1f} km" if pd.notna(x) else "")
		self.df_trips.set_index('id', inplace=True)

		# self.table_model = PandasModel(self.df_files)
		self.table_model = PandasModel(self.df_trips)
		self.table.setModel(self.table_model)
		self.table.setSortingEnabled(True)
		self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
		self.table.selectionModel().selectionChanged.connect(self.on_row_selected)
		self.table.horizontalHeader().setStretchLastSection(True)
		self.table.resizeColumnsToContents()

		# self.table_model = PandasModel(self.df_files)
		# self.table.setModel(self.table_model)
		# self.table.setSortingEnabled(True)
		# # self.table.setSelectionBehavior(self.table.SelectRows)
		# self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
		# self.table.selectionModel().selectionChanged.connect(self.on_row_selected)

	def on_colormap_changed(self, colormap_name):
		"""Called when user changes the colormap selection"""
		# Debounce to avoid repeated heavy redraws on rapid UI changes.
		self._plot_refresh_timer.start(200)

	def on_zoom_changed(self, zoom_level):
		"""Called when user changes map zoom"""
		# Debounce zoom updates to avoid blocking UI with repeated basemap fetches.
		self._plot_refresh_timer.start(300)

	def refresh_plot(self):
		"""Refresh the current plot with selected rows"""
		# Get currently selected rows and replot
		rows = sorted(set(index.row() for index in self.table.selectionModel().selectedRows()))
		if rows:
			self._plot_for_rows(rows)

	def _get_selected_fileids(self, rows):
		if not rows:
			return []
		if 'fileid' in self.df_trips.columns:
			return [int(k) for k in self.df_trips.iloc[rows]['fileid'].tolist()]
		return []

	def _load_trip_plot_data(self, fileid: int) -> dict[str, list[float]] | None:
		if fileid in self._trip_plot_cache:
			return self._trip_plot_cache[fileid]

		lat_col = self._resolved_torqlogs_columns.get('latitude')
		lon_col = self._resolved_torqlogs_columns.get('longitude')
		speed_col_name = self._resolved_torqlogs_columns.get('speedobdkmh')
		if not (lat_col and lon_col and speed_col_name):
			return None

		q = (
			f'SELECT "{lon_col}" AS Longitude, "{lat_col}" AS Latitude, '
			f'"{speed_col_name}" AS Speed_OBDkmh FROM torqlogs WHERE fileid = {int(fileid)}'
		)
		df_part = pd.read_sql(q, self.engine)
		if df_part.empty:
			self._trip_plot_cache[fileid] = {"x": [], "y": [], "speed": []}
			return self._trip_plot_cache[fileid]

		gdf = gpd.GeoDataFrame(
			df_part,
			geometry=[Point(xy) for xy in zip(df_part['Longitude'], df_part['Latitude'])],
			crs="EPSG:4326",
		).to_crs(epsg=3857)
		speed_series = pd.to_numeric(df_part['Speed_OBDkmh'], errors='coerce').fillna(0)

		payload: dict[str, list[float]] = {
			"x": gdf.geometry.x.tolist(),
			"y": gdf.geometry.y.tolist(),
			"speed": speed_series.tolist(),
		}
		self._trip_plot_cache[fileid] = payload
		return payload

	def _selection_key(self, fileids: list[int]) -> str:
		return ",".join(str(fid) for fid in sorted(fileids))

	def _cache_fileid(self, fileids: list[int]) -> int | None:
		return int(fileids[0]) if len(fileids) == 1 else None

	def _load_cached_map_image(self, fileids: list[int], zoom: int, colormap: str) -> bytes | None:
		selection_key = self._selection_key(fileids)
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
				"zoom": zoom,
				"colormap": colormap,
			}).first()
		if row:
			return row[0]
		return None

	def _save_cached_map_image(self, fileids: list[int], zoom: int, colormap: str):
		selection_key = self._selection_key(fileids)
		fileid = self._cache_fileid(fileids)
		buf = io.BytesIO()
		self.map_canvas.figure.savefig(buf, format='png', dpi=100, bbox_inches='tight')
		image_bytes = buf.getvalue()
		upsert_sql = text(
			"""
			INSERT INTO mapimagecache (fileid, selection_key, zoom, colormap, image_png, created_at, updated_at)
			VALUES (:fileid, :selection_key, :zoom, :colormap, :image_png, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
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
				"zoom": zoom,
				"colormap": colormap,
				"image_png": image_bytes,
			})

	def _plot_for_rows(self, rows):
		fileids = self._get_selected_fileids(rows)
		if not fileids:
			return

		zoom = int(self.zoom_combo.currentText())
		colormap_name = self.colormap_combo.currentText()
		cached_img = self._load_cached_map_image(fileids, zoom, colormap_name)
		if cached_img:
			self.map_canvas.ax.clear()
			img = mpimg.imread(io.BytesIO(cached_img), format='png')
			self.map_canvas.ax.imshow(img)
			self.map_canvas.ax.set_title("Trip Map (cached)")
			self.map_canvas.ax.set_xlabel("Longitude")
			self.map_canvas.ax.set_ylabel("Latitude")
			self.map_canvas.draw_idle()
			return

		self.map_canvas.ax.clear()

		# Get selected colormap
		cmap = plt.colormaps[colormap_name]

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
		for idx, fileid in enumerate(fileids):
			plot_data = self._load_trip_plot_data(fileid)
			if not plot_data:
				continue

			x_vals = plot_data["x"]
			y_vals = plot_data["y"]
			speed_vals = pd.to_numeric(pd.Series(plot_data["speed"]), errors='coerce').fillna(0)
			if len(x_vals) == 0:
				continue

			sizes = speed_vals.clip(lower=1, upper=100)
			base_color = cmap(idx % cycle_length)
			speed_max = speed_vals.max()
			colors = [(
				min(1, base_color[0] + 0.5 * (v / speed_max if speed_max > 0 else 0)),
				min(1, base_color[1] + 0.5 * (v / speed_max if speed_max > 0 else 0)),
				min(1, base_color[2] + 0.5 * (v / speed_max if speed_max > 0 else 0)),
				base_color[3]) for v in speed_vals]
			sc = self.map_canvas.ax.scatter(x_vals, y_vals, s=sizes, c=colors, label=f"fileid {fileid}", zorder=2)
			plots.append(sc)

		if plots:
			self._start_async_basemap(zoom, fileids, colormap_name)
		self.map_canvas.ax.set_title("Trip Map")
		self.map_canvas.ax.set_xlabel("Longitude")
		self.map_canvas.ax.set_ylabel("Latitude")
		self.map_canvas.draw_idle()

	def _start_async_basemap(self, zoom: int, fileids: list[int], colormap_name: str):
		xmin, xmax = self.map_canvas.ax.get_xlim()
		ymin, ymax = self.map_canvas.ax.get_ylim()
		if xmax <= xmin or ymax <= ymin:
			return

		self._basemap_request_id += 1
		request_id = self._basemap_request_id
		self._basemap_request_context[request_id] = {
			"fileids": fileids,
			"zoom": zoom,
			"colormap": colormap_name,
		}

		thread = QThread(self)
		worker = BasemapWorker((xmin, xmax, ymin, ymax), zoom, request_id)
		worker.moveToThread(thread)

		thread.started.connect(worker.run)
		worker.finished.connect(self._on_basemap_loaded)
		worker.error.connect(self._on_basemap_error)
		worker.finished.connect(thread.quit)
		worker.error.connect(thread.quit)
		thread.finished.connect(worker.deleteLater)
		thread.finished.connect(thread.deleteLater)

		self._basemap_worker = worker
		self._basemap_thread = thread
		thread.start()

	def _on_basemap_loaded(self, img, ext, request_id: int):
		if request_id != self._basemap_request_id:
			return
		self.map_canvas.ax.imshow(img, extent=ext, interpolation='bilinear', zorder=0)
		self.map_canvas.draw_idle()
		ctx = self._basemap_request_context.get(request_id)
		if ctx:
			self._save_cached_map_image(
				cast(list[int], ctx["fileids"]),
				cast(int, ctx["zoom"]),
				cast(str, ctx["colormap"]),
			)
			self._basemap_request_context.pop(request_id, None)

	def _on_basemap_error(self, err: str, request_id: int):
		if request_id != self._basemap_request_id:
			return
		self._basemap_request_context.pop(request_id, None)
		# Tile/network failures should not break UI interaction.
		print(f"{self} Basemap load failed: {err} (request_id={request_id}) current_id={self._basemap_request_id}")

	def on_row_selected(self, selected, deselected):
		rows = sorted(set(index.row() for index in self.table.selectionModel().selectedRows()))
		if rows:
			self._plot_for_rows(rows)

class PandasModel(QAbstractTableModel):
	"""Minimal Qt model for pandas DataFrame for QTableView."""
	def __init__(self, data):
		super().__init__()
		self._data = data

	def sort(self, column: int, order: Qt.SortOrder = Qt.SortOrder.AscendingOrder) -> None:
		colname = self._data.columns[column]
		self.layoutAboutToBeChanged.emit()
		self._data.sort_values(by=colname, ascending=(order == Qt.SortOrder.AscendingOrder), inplace=True)
		self._data.reset_index(inplace=True)
		self._data.set_index(self._data.columns[0], inplace=True)
		self.layoutChanged.emit()

	def old_sort(self, column, order):
		colname = self._data.columns[column]
		self.layoutAboutToBeChanged.emit()
		self._data.sort_values(by=colname, ascending=(order == Qt.SortOrder.AscendingOrder), inplace=True, ignore_index=True)
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
	app = QApplication(sys.argv)
	window = MainWindow()
	window.showMaximized()
	# window.resize(1000, 600)
	# window.show()
	sys.exit(app.exec())
