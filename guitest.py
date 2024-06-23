#!/usr/bin/python3
import sys
import os
from argparse import ArgumentParser

import PySide6
from loguru import logger
from PySide6 import QtCore, QtSql, QtGui
from PySide6.QtCharts import QChart, QChartView, QLineSeries, QScatterSeries
from PySide6.QtCore import QAbstractTableModel, Qt
from PySide6.QtGui import QFont
from PySide6.QtSql import QSqlQueryModel

# from PySide6.QtWidgets import (QApplication, QSizePolicy, QWidget)
from PySide6.QtWidgets import QApplication, QMainWindow
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import numpy as np

from datamodels import Torqlogs, TorqFile
from ui_untitled import Ui_MainWindow
from utils import get_engine_session
from converter import get_args
# x = latitude y = longitude !

## Define main window class from template


class Mymodel(QAbstractTableModel):
	pass

mymodel = Mymodel()

class TripplotModel(QtSql.QSqlQueryModel):
	def __init__(self, fileid):
		super().__init__()
		self.fileid = fileid
		self.setQuery(f'select latitude, longitude from torqlogs where fileid={self.fileid}')
		self.setHeaderData(1, QtCore.Qt.Horizontal, "latitude")
		self.setHeaderData(2, QtCore.Qt.Horizontal, "longitude")


class Torqfilemodel(QtSql.QSqlQueryModel):
	def __init__(self):
		super().__init__()
		self.setQuery('select fileid,sent_rows from torqfiles')
		self.setHeaderData(0, QtCore.Qt.Horizontal, "fileid")
		self.setHeaderData(1, QtCore.Qt.Horizontal, "entries")

class CustomSqlModel(QtSql.QSqlQueryModel):
    def data(self, index, role):
        value = super(CustomSqlModel, self).data(index, role)
        if value is not None and role == QtCore.Qt.DisplayRole:
            if index.column() == 0:
                return '#%d' % value
            elif index.column() == 2:
                return value # .upper()
        if role == QtCore.Qt.ForegroundRole and index.column() == 1:
            return QtGui.QColor(QtCore.Qt.blue)
        return value


class MainApp(QMainWindow):#QWidget, Ui_FindGitsApp):
	def __init__(self, args, c=None, parent=None):
		# self.session = session
		# super(MainApp, self).__init__()
		super(MainApp, self).__init__(parent=parent)
		self.ui = Ui_MainWindow()
		self.ui.setupUi(self)
		self.con = c
		engine, session = get_engine_session(args)
		#engine = create_engine(args.dburl, echo=False, connect_args={'check_same_thread': False})
		#Session = sessionmaker(bind=engine)
		self.session = session
		#q=QSqlQuery('select * from TorqFile')
		# self.tripmodel = QSqlTableModel()
		#self.tripmodel.setTable('TorqFile')
		# self.tripmodel.select()
		#self.create_start_stops_plot()
		#self.create_entries_plot()
		self.populate_torqfiles()
		self.ui.tableView.doubleClicked.connect(self.doubleClicked_table)
		self.trip_plot_view = QChartView()

	def populate_torqfiles(self):
		self.filemodel = QSqlQueryModel()
		self.filemodel.setQuery('select fileid,sent_rows from torqfiles')
		#data = self.session.query(TorqFile.fileid, TorqFile.sent_rows).all()
		self.filemodel.setHeaderData(0, QtCore.Qt.Horizontal, "fileid")
		self.filemodel.setHeaderData(1, QtCore.Qt.Horizontal, "entries")
		self.ui.tableView.setModel(self.filemodel)
		self.ui.tableView.resizeColumnsToContents()


	def doubleClicked_table(self):
		index = self.ui.tableView.selectedIndexes()[0]
		row = self.ui.tableView.selectedIndexes()[0].row()
		celldata = self.ui.tableView.model().data(index)
		fileid = row+1
		logs=self.session.query(Torqlogs.latitude, Torqlogs.longitude).filter(Torqlogs.fileid==fileid).all()
		logger.debug(f'{fileid=} row={row}  data={celldata} logs={len(logs)}')
		scatter = QScatterSeries()
		try:
			[scatter.append(k[0],k[1]) for k in logs]
		except TypeError as e:
			logger.warning(f'{e}')
		self.tripplotmodel = TripplotModel(fileid=fileid)
		try:
			self.ui.triplayout.removeWidget(self.trip_plot_view)
		except AttributeError as e:
			logger.warning(f'{e}')
		self.trip_plot = QChart()
		self.trip_plot_view = QChartView(self.trip_plot)
		#scatter.setModel(self.tripplotmodel)
		self.ui.triplayout.addWidget(self.trip_plot_view)
		self.setLayout(self.ui.triplayout)
		self.trip_plot.addSeries(scatter)
		scatter.setMarkerSize(5)
		self.trip_plot.legend().hide()

	def create_start_stops_plot(self):
		# self.startstopmodel = QSqlQueryModel()
		#self.startstopmodel.setQuery('select * from startstops;')
		# x = latitude y = longitude !

		data = np.array(session.execute(text('select latmin,lonmin,latmax,lonmax from startstops')).all())
		scatter = QScatterSeries()
		[scatter.append(k[0],k[1]) for k in data]
		[scatter.append(k[2],k[3]) for k in data]
		self.start_stop_plot = QChart()
		self.start_stop_plot_view = QChartView(self.start_stop_plot)
		self.ui.main_layout.addWidget(self.start_stop_plot_view)
		#self.setLayout(self.ui.main_layout)

		self.start_stop_plot.addSeries(scatter)
		# scatter.setName('start/end')
		scatter.setMarkerSize(5)
		# self.start_stop_plot.createDefaultAxes()
		# self.start_stop_plot.setTitleFont(QFont('Arial', 10))
		self.start_stop_plot.setTitle('trip start/end')
		self.start_stop_plot.legend().hide()
		# self.start_stop_plot.axes()[0].setMax(self.start_stop_plot.axes()[0].max()+1)
		# self.start_stop_plot.axes()[1].setMax(self.start_stop_plot.axes()[1].max()+1)
		# self.start_stop_plot.axes()[0].setMin(self.start_stop_plot.axes()[0].min()-1)
		# self.start_stop_plot.axes()[1].setMin(self.start_stop_plot.axes()[1].min()-1)
		#self.ui.tableView.setModel(self.tripdist_series_model)
		#self.ui.tableView.resizeColumnsToContents()

	def create_entries_plot(self):
		self.fileentries_series = QLineSeries()
		data = self.session.query(TorqFile.fileid,TorqFile.sent_rows).all()
		[self.fileentries_series.append(k[0],k[1]) for k in data]
		self.entries_chart = QChart()
		self.trip_plot_view = QChartView(self.entries_chart)
		self.ui.entrieslayout.addWidget(self.trip_plot_view)
		self.setLayout(self.ui.main_layout)

		self.entries_chart.addSeries(self.fileentries_series)
		self.fileentries_series.setName('file entries')
		self.fileentries_series.setMarkerSize(5)
		self.entries_chart.createDefaultAxes()
		self.entries_chart.setTitleFont(QFont('Arial', 10))
		self.entries_chart.setTitle('entries')
		self.entries_chart.legend().hide()
		#self.ui.tableView.setModel(self.tripdist_series_model)
		self.ui.tableView.resizeColumnsToContents()
		#self.ui.listView.setModel(self.tripdist_series_model)
		#self.ui.listView.resizeContents(25,25)



def createConnection(args):
	if args.dbmode == 'sqlite':
		con = QtSql.QSqlDatabase.addDatabase('QSQLITE')
		con.setDatabaseName(args.dbfile)
	elif args.dbmode == 'mariadb':
		con = QtSql.QSqlDatabase.addDatabase('QMARIADB')
		con.setDatabaseName(args.dbname)
		con.setHostName(args.dbhost)
		con.setUserName(args.dbuser)
		con.setPassword(args.dbpass)
	elif args.dbmode == 'psql':
		con = QtSql.QSqlDatabase.addDatabase('QPSQL')
		con.setDatabaseName(args.dbname)
		con.setHostName(args.dbhost)
		con.setUserName(args.dbuser)
		con.setPassword(args.dbpass)
	if not con.open():
		#QMessageBox.critical(None, "Cannot open database",			con.lastError().text())
		return False
	return con

# df = pd.DataFrame([k.__dict__ for k in trips])
if __name__ == '__main__':
	args = get_args(appname='testgui')
	engine, session = get_engine_session(args)
	#Session = sessionmaker(bind=engine)
	#session = Session()
	app = QApplication(sys.argv)
	c = createConnection(args)
	w = MainApp(args, c)
	w.show()
	sys.exit(app.exec())
#	if not createConnection():
#		sys.exit(1)
#	w = MainApp()
#	w.show()
#	sys.exit(app.exec())
