import sys
import pandas as pd
from PySide6.QtWidgets import (
	QApplication, QMainWindow, QTableView, QVBoxLayout, QWidget, QSplitter
)
from PySide6.QtWidgets import QAbstractItemView
from PySide6.QtCore import Qt, QAbstractTableModel, QModelIndex
from PySide6.QtSql import QSqlDatabase, QSqlTableModel
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas

from datamodels import TorqFile, Torqlogs  # adjust import if needed

DB_PATH = "sqlite:///torqfiskur1.db"  # adjust if needed

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

		splitter.addWidget(self.table)
		splitter.addWidget(self.map_canvas)
		splitter.setSizes([200, 600])

		container = QWidget()
		layout = QVBoxLayout(container)
		layout.addWidget(splitter)
		self.setCentralWidget(container)

		# Load TorqFiles into a pandas DataFrame
		# self.df_files = pd.read_sql(self.session.query(TorqFile).statement, self.engine)
		# self.df_files = pd.read_sql(self.session.query(TorqFile).statement, self.engine, parse_dates=False)
		self.df_files = pd.read_sql("SELECT fileid,trip_start,trip_duration FROM torqfiles", self.engine)
		self.table_model = PandasModel(self.df_files)
		self.table.setModel(self.table_model)
		self.table.setSortingEnabled(True)
		# self.table.setSelectionBehavior(self.table.SelectRows)
		self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
		self.table.selectionModel().selectionChanged.connect(self.on_row_selected)

	def on_row_selected(self, selected, deselected):
		# Get all selected rows (unique row indices)
		rows = sorted(set(index.row() for index in self.table.selectionModel().selectedRows()))
		fileids = self.df_files.iloc[rows]['fileid'].tolist()
		self.map_canvas.ax.clear()
		cmap = plt.colormaps['tab10']
		# colors = plt.cm.get_cmap('tab10', len(fileids))
		# colors = plt.colormaps.get_cmap('tab10', len(fileids))
		for idx, fileid in enumerate(fileids):
			df_part = pd.read_sql(f"SELECT Latitude,Longitude,Speed_OBDkmh FROM torqlogs WHERE fileid={fileid}", self.engine)
			if not df_part.empty:
				sizes = df_part['Speed_OBDkmh'].fillna(0).clip(lower=0, upper=20) + 2
				color = cmap(idx % 10)  # tab10 has 10 distinct colors
				self.map_canvas.ax.scatter(df_part['Longitude'], df_part['Latitude'],s=sizes, c=[color], label=f"fileid {fileid}")
		self.map_canvas.ax.set_title("Trip Map")
		self.map_canvas.ax.set_xlabel("Longitude")
		self.map_canvas.ax.set_ylabel("Latitude")
		if len(fileids) > 1:
			self.map_canvas.ax.legend(fontsize='small')
		self.map_canvas.draw()

	def old_on_row_selected(self, selected, deselected):

		# singlerow version
		# if not selected.indexes():
		# 	return
		# row = selected.indexes()[0].row()
		# fileid = self.df_files.iloc[row]['fileid']
		# # Query GPS data for this fileid
		# # df_logs = pd.read_sql(self.session.query(Torqlogs.Latitude, Torqlogs.Longitude).filter(Torqlogs.fileid == fileid).statement,self.engine)
		# df_logs = pd.read_sql(f"SELECT Latitude,Longitude FROM torqlogs where fileid={fileid}", self.engine)
		# self.map_canvas.plot_trip(df_logs)

		# Get all selected rows (unique row indices)
		rows = sorted(set(index.row() for index in self.table.selectionModel().selectedRows()))
		fileids = self.df_files.iloc[rows]['fileid'].tolist()
		# Fetch and concatenate GPS data for all selected fileids
		df_logs = pd.DataFrame()
		for fileid in fileids:
			df_part = pd.read_sql(f"SELECT Latitude,Longitude FROM torqlogs WHERE fileid={fileid}", self.engine)
			df_logs = pd.concat([df_logs, df_part], ignore_index=True)
		self.map_canvas.plot_trip(df_logs)

class PandasModel(QAbstractTableModel):
	"""Minimal Qt model for pandas DataFrame for QTableView."""
	def __init__(self, data):
		super().__init__()
		self._data = data

	def sort(self, column, order):
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
	window.resize(1000, 600)
	window.show()
	sys.exit(app.exec())
