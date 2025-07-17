import contextily as ctx
import geopandas as gpd
from shapely.geometry import Point
import sys
import pandas as pd
from PySide6.QtWidgets import (
	QApplication, QMainWindow, QTableView, QVBoxLayout, QWidget, QSplitter, 
	QHBoxLayout, QLabel, QComboBox
)
from PySide6.QtGui import QFont
from PySide6.QtWidgets import QAbstractItemView
from PySide6.QtCore import Qt, QAbstractTableModel, QModelIndex
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import matplotlib
matplotlib.use("QtAgg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas

DB_PATH = "sqlite:///torqdata.db"

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

class MainWindow(QMainWindow):
	def __init__(self):
		super().__init__()
		self.setWindowTitle("TorqFiles Viewer")
		# Set up SQLAlchemy session
		self.engine = create_engine(DB_PATH)
		self.Session = sessionmaker(bind=self.engine)
		self.session = self.Session()

		# Set up UI
		splitter = QSplitter(Qt.Horizontal)
		self.table = QTableView()
		self.map_canvas = MapCanvas()

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
		self.df_files.set_index('fileid', inplace=True)

		self.table_model = PandasModel(self.df_files)
		self.table.setModel(self.table_model)
		self.table.setSortingEnabled(True)
		self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
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
		# Refresh the current plot with new colormap
		self.refresh_plot()

	def refresh_plot(self):
		"""Refresh the current plot with selected rows"""
		# Get currently selected rows and replot
		rows = sorted(set(index.row() for index in self.table.selectionModel().selectedRows()))
		if rows:
			# Simulate selection change to refresh plot
			self.on_row_selected(None, None)

	def on_row_selected(self, selected, deselected):
		# Get all selected rows (unique row indices)
		rows = sorted(set(index.row() for index in self.table.selectionModel().selectedRows()))
		# fileids = self.df_files.iloc[rows]  # ['fileid'].tolist()
		fileids = self.df_files.index[rows].tolist()
		self.map_canvas.ax.clear()
		
		# Get selected colormap
		colormap_name = self.colormap_combo.currentText()
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
			cycle_length = 10  # Default for sequential colormaps
		
		plots = []
		for idx, fileid in enumerate(fileids):  # enumerate(self.df_files.iterrows()):
			# fileid = df_file[0]
			df_part = pd.read_sql(f"SELECT Longitude,Latitude,Speed_OBDkmh FROM torqlogs WHERE fileid={fileid}", self.engine)
			if not df_part.empty:
				# Convert to numeric first to avoid fillna downcasting warning
				gdf = gpd.GeoDataFrame(df_part, geometry=[Point(xy) for xy in zip(df_part['Longitude'], df_part['Latitude'])], crs="EPSG:4326").to_crs(epsg=3857)
				speed_col = pd.to_numeric(df_part['Speed_OBDkmh'], errors='coerce').fillna(0)
				sizes = speed_col.clip(lower=1, upper=100)
				base_color = cmap(idx % cycle_length)
				colors = [(
					min(1, base_color[0] + 0.5 * (v / speed_col.max() if speed_col.max() > 0 else 0)),
					min(1, base_color[1] + 0.5 * (v / speed_col.max() if speed_col.max() > 0 else 0)),
					min(1, base_color[2] + 0.5 * (v / speed_col.max() if speed_col.max() > 0 else 0)),
					base_color[3]) for v in speed_col]
				sc = self.map_canvas.ax.scatter(gdf.geometry.x, gdf.geometry.y, s=sizes, c=colors, label=f"fileid {fileid}")
				plots.append(sc)
				# self.map_canvas.ax.scatter(df_part['Longitude'], df_part['Latitude'],s=sizes, c=[color], label=f"fileid {fileid}")
				# color2 = cmap(idx % 2)  # tab10 has 10 distinct colors
				# self.map_canvas.ax.scatter(df_part['Longitude'], df_part['Latitude'],s=1, c=[color2], label=f"fileid {fileid}")
				# self.map_canvas.ax.scatter(df_part['Latitude'], df_part['Longitude'], s=sizes, c=[color], label=f"fileid {fileid}")
		# Add basemap if at least one trip
		if plots:
			# ctx.add_basemap(self.map_canvas.ax, crs="EPSG:3857", source=ctx.providers.OpenStreetMap.Mapnik)
			zoom = min(32, max(10, int(self.map_canvas.ax.get_xlim()[1] - self.map_canvas.ax.get_xlim()[0]) // 10000))
			ctx.add_basemap(self.map_canvas.ax, crs="EPSG:3857", source=ctx.providers.OpenStreetMap.Mapnik, zoom=zoom)
			# ctx.add_basemap(self.map_canvas.ax, crs="EPSG:3857", source=ctx.providers.OpenStreetMap.Mapnik, zoom=16)
		self.map_canvas.ax.set_title("Trip Map")
		self.map_canvas.ax.set_xlabel("Longitude")
		self.map_canvas.ax.set_ylabel("Latitude")
		# if len(fileids) > 1:
		# 	self.map_canvas.ax.legend(fontsize='small')
		self.map_canvas.draw()

class PandasModel(QAbstractTableModel):
	"""Minimal Qt model for pandas DataFrame for QTableView."""
	def __init__(self, data):
		super().__init__()
		self._data = data

	def sort(self, column, order):
		colname = self._data.columns[column]
		self.layoutAboutToBeChanged.emit()
		self._data.sort_values(by=colname, ascending=(order == Qt.AscendingOrder), inplace=True)
		self._data.reset_index(inplace=True)
		self._data.set_index('fileid', inplace=True)
		self.layoutChanged.emit()

	def old_sort(self, column, order):
		colname = self._data.columns[column]
		self.layoutAboutToBeChanged.emit()
		self._data.sort_values(by=colname, ascending=(order == Qt.AscendingOrder), inplace=True, ignore_index=True)
		self.layoutChanged.emit()

	def rowCount(self, parent=QModelIndex()):
		return self._data.shape[0]

	def columnCount(self, parent=QModelIndex()):
		return self._data.shape[1]

	def data(self, index, role=Qt.DisplayRole):
		if role == Qt.DisplayRole:
			return str(self._data.iloc[index.row(), index.column()])
		return None

	def headerData(self, section, orientation, role=Qt.DisplayRole):
		if role == Qt.DisplayRole:
			if orientation == Qt.Horizontal:
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
