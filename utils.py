# utils and db things here
import os
import sys
from pathlib import Path
import inspect
import argparse
from re import search, sub
from pandas import read_csv, DataFrame
from datetime import datetime
from hashlib import md5
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, inspect, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError,DataError
import pymysql
from sqlalchemy.sql.schema import ForeignKey
from PyQt5.QtWidgets import QTextBrowser
from numpy import nan
from fieldmaps import FIELDMAPS

from PySide2.QtUiTools import QUiLoader
from PySide2.QtWidgets import QApplication, QPushButton, QLineEdit, QLabel, QPushButton, QListView, QTableWidgetItem, QListWidgetItem, QTextBrowser
from PySide2.QtCore import QFile, QObject, QFile, QFileInfo
# from PySide2.QtWidgets.QListWidgetItem
# from PyQt5 import uic
# from PyQt5.QtWidgets import QApplication
# from PySide5.QtWidgets import QApplication, QMainWindow
# from PySide5.QtCore import QFile
# from ui_mainwindow import Ui_MainWindow
Base = declarative_base()
# filetable = Table(
# 		'filelist', meta,
# 		Column('id', Integer, primary_key=True),
# 		Column('name', String(255)),
# 		Column('hash', String(255))
# 	)

def get_csv_files(searchpath:Path, recursive=True):
	# csvlist = searchpath.glob('tracklog*.csv')
	if not isinstance(searchpath, Path):
		searchpath = Path(searchpath)
	if not isinstance(searchpath, Path):
		print(f'[getcsv] err: searchpath {searchpath} is {type(searchpath)} need Path object')
		return []
	else:
		return [k for k in searchpath.glob("**/tracklog.csv")]


class Torqfile(Base):
	__tablename__ = 'torqfiles'
	fileid =  Column(Integer, primary_key=True)
	name = Column(String(255))
	hash = Column(String(255))

	def __init__(self, filename):
		self.name = filename
		self.columns = []
		self.buffer = None
		self.buffer_parsed = False
		self.hash = self.gen_md5hash()
		self.exists_in_db = False
		self.num_lines = sum(1 for line in open(self.name)) # how many lines in csv file
		# self.id = Column(Integer, primary_key=True)

	def get_columns(self):
		self.columns = read_csv_columns_raw(self.name)
		return self.columns

	def get_hash(self): # return own md5 hash
		if self.hash is None:
			self.gen_md5hash()
		return self.hash

	def gen_md5hash(self): # generate md5 hash
		hash = md5(open(self.name,'rb').read()).hexdigest()
		self.hash = hash
		return hash

	def update_columns(self, engine=None, cols=None):
		pass

	def send_data(self, engine=None, cols=None): # send own csv data to database ...
		self.buffer = self.read_csv_data()  # (filepath_or_buffer=self.name, delimiter=',', low_memory=False, encoding='cp1252')
		self.buffer['hash'] = self.hash
		# buffer = read_csv(filepath_or_buffer=self.name, delimiter=',', low_memory=False, encoding='cp1252')
		if self.buffer is None:
			print(f'[read] from {self.name} returned {type(self.buffer)}')
			return
		else:
			# self.buffer = buffer
			print(f'[send_data] {self.name} to sql size: {len(self.buffer)}')
			try:
				# todo fix
				if self.buffer is not None:
					# self.buffer.to_sql(con=engine, name='torqlogs', if_exists='append', index=True, index_label='torqentryid', chunksize=100)
					self.buffer.to_sql(con=engine, name='torqlogs', if_exists='append', index=True, chunksize=100)
					print(f'[send_data] sending done')
			except pymysql.err.OperationalError as e:
				print(f'[send_data] sending failed: OPERR {e}')
			except (pymysql.err.DataError, DataError) as e:
				print(f'[send_data] sending failed: DATAERR {e.code} {e.orig}')
			except OperationalError as e:
				print(f'[send_data] sending failed: sqlalchemy operr {e.code} {e.orig}')
			except AttributeError as e:
				print(f'[send_data] sending failed: AttributeError {e} ')

	def parse_data(self): # ...
		pass

	def read_csv_data(self): # read own data file
		buffer = None
		if self.name == '...':
			print(f'[tfile] ERR ??? name:{self.name}')
			return
		try:
			buffer = read_csv(filepath_or_buffer=self.name, delimiter=',', low_memory=False, encoding='cp1252')
			# buffer = buffer.replace('-','0', inplace=True)
			buffer = buffer.fillna(0)
			buffer.replace(' ','', inplace=True)
			buffer.replace('-','0', inplace=True)
			buffer.replace('âˆž','0', inplace=True)
			buffer.replace('Ã¢ÂˆÂž','0', inplace=True)
			# buffer = buffer.fillna(value=nan)
		except Exception as e:
			print(f'[tfile] read_csv_data err {e} {self.name}')
			# buffer = []
			return
		# cols = column_fixer(buffer.columns)
		cols = [column_fixer(k) for k in buffer.columns]
		fields=[FIELDMAPS[k] for k in cols]
		buffer.columns = fields
			# sys.exit(-1)
		#buffer = buffer
		print(f'[tfile] buffersize: {len(buffer)}')
		return buffer

	def check_db_status(self, engine=None, session=None):
		# inspector = inspect(engine)
		# records = session.query(Torqfile).filter_by(hash="b5dc57f1856857a3d9").all()
		# records = session.query(Torqfile).filter(Torqfile.hash == 'b5dc57f1856857a3d9').all()
		result = session.query(Torqfile).filter(Torqfile.hash == self.hash).all()
		if len(result) >= 1:
			self.exists_in_db = True
			return True
		else:
			self.exists_in_db = False
			return False
		# 	print(f'[chkdb] hash: {self.hash} exists in db .... res: {result} {len(result)} {type(result)}')
		# else:
		# 	print(f'[chkdb] new entry check hash: {self.hash} res: {result} {len(result)} {type(result)}')

class Torqlog(Base):
	__tablename__ = 'torqlogs'
	torqentryid =  Column(Integer, primary_key=True)
	#torqentryid =  Column(Integer, ForeignKey('Torqfile.fileid'), primary_key=True)
	hash = Column(String(255))
	index =  Column(Integer)
	AccelerationSensorTotalg = Column(Integer, default=0)
	AccelerationSensorXaxisg = Column(Integer, default=0)
	AccelerationSensorYaxisg = Column(Integer, default=0)
	AccelerationSensorZaxisg = Column(Integer, default=0)
	Actualenginetorque = Column(Integer, default=0)
	Altitudem = Column(Integer, default=0)
	AndroiddeviceBatteryLevel = Column(Integer, default=0)
	Averagetripspeedwhilststoppedormovingkm = Column(Integer, default=0)
	Averagetripspeedwhilststoppedormovingonlykm = Column(Integer, default=0)
	Bearing = Column(Integer, default=0)
	COaing = Column(Integer, default=0)
	COing = Column(Integer, default=0)
	costpermilekminst = Column(Integer, default=0)
	costpermilekm = Column(Integer, default=0)
	DeviceTime = Column(String(255), default=0)
	DistancetoemptyEstimatedkm = Column(Integer, default=0)
	DistancetravelledwithMIL = Column(String(255), default=0)
	EngineCoolantTemperatureF = Column(String(255), default=0)
	EnginekWAtthewheelskW = Column(Integer, default=0)
	EngineLoad = Column(String(255), default=0)
	EngineRPMrpm = Column(String(255), default=0)
	Fuelcosttripcost = Column(Integer, default=0)
	Fuelflowratelhr = Column(Integer, default=0)
	Fuelflowrateccmin = Column(Integer, default=0)
	Fuelpressurekpa = Column(Integer, default=0)
	FuelRailPressurekpa = Column(String(255), default=0)
	FuelRemainingCalculatedfromvehicleprofile = Column(Integer, default=0)
	Fuelusedtripl = Column(Integer, default=0)
	GPSAccuracym = Column(Integer, default=0)
	GPSAltitudem = Column(Integer, default=0)
	GPSBearing = Column(Integer, default=0)
	GPSLatitude = Column(Integer, default=0)
	GPSLongitude = Column(Integer, default=0)
	GPSSatellites = Column(Integer, default=0)
	GPSSpeedkm = Column(Integer, default=0)
	GPSTime = Column(String(255), default=0)
	GPSvsOBDSpeeddifferencekm = Column(Integer, default=0)
	GravityXG = Column(Integer, default=0)
	GravityYG = Column(Integer, default=0)
	GravityZG = Column(Integer, default=0)
	HorizontalDilutionofPrecision = Column(Integer, default=0)
	HorsepowerAtthewheelshp = Column(Integer, default=0)
	IntakeAirTemperatureF = Column(String(255), default=0)
	IntakeManifoldPressurekpa = Column(String(255), default=0)
	KilometersPerLitreInstantkpl = Column(Integer, default=0)
	KilometersPerLitreLongTermAveragekpl = Column(Integer, default=0)
	Latitude = Column(Integer, default=0)
	LitresPer100KilometerInstantl = Column(Integer, default=0)
	LitresPer100KilometerLongTermAveragel = Column(Integer, default=0)
	Longitude = Column(Integer, default=0)
	MassAirFlowRateg = Column(String(255), default=0)
	MilesPerGallonInstantmpg = Column(Integer, default=0)
	MilesPerGallonLongTermAveragempg = Column(Integer, default=0)
	SpeedOBDkm = Column(String(255), default=0)
	SpeedGPSkm = Column(Integer, default=0)
	Torqueftlb = Column(Integer, default=0)
	TripaverageKPLkpl = Column(Integer, default=0)
	TripaverageLitres = Column(Integer, default=0)
	TripaverageMPGmpg = Column(Integer, default=0)
	Tripdistancestoredinvehicleprofilekm = Column(Integer, default=0)
	TripDistancekm = Column(Integer, default=0)
	TripTimeSincejourneystarts = Column(Integer, default=0)
	Triptimewhilstmovings = Column(Integer, default=0)
	Triptimewhilststationarys = Column(Integer, default=0)
	TurboBoostVacuumGaugebar = Column(Integer, default=0)
	VoltageOBDAdapterV = Column(Integer, default=0)
	VolumetricEfficiencyCalculated = Column(Integer, default=0)



def column_fixer(inputline):
	columns = inputline.split(',') # split
	columns = [sub('\n', '', col) for col in columns] # remove \n 's
	columns = [sub('-', '', col) for col in columns] # degree symbol
	columns = [sub('â', '', col) for col in columns] # degree symbol
	columns = [sub('Â', '', col) for col in columns] # symbol cleanup
	columns = [sub('Ã¢', '', col) for col in columns] # symbol cleanup
	columns = [sub('Ã‚', '', col) for col in columns] # symbol cleanup
	columns = [sub('CO,‚', 'CO', col) for col in columns] # symbol cleanup
	columns = [sub('^\s', '', k) for k in columns] # remove extra spaces from start of col name
	# for idx, c in enumerate(columns): # more cleanup
	# 	if c == 'CO‚‚ in g/km (Average)(g/km)':
	# 		columns[idx] = 'CO in g/km (Average)(g/km)'
	# 	if c == 'CO‚‚ in g/km (Instantaneous)(g/km)':
	# 		columns[idx] = 'CO in g/km (Instantaneous)(g/km)'
		# if c == 'Device Time':
		# 	columns[idx] = 'DeviceTime'
		# if c == 'GPS Speed(km/h)':
		# 	columns[idx] = 'GPSSpeedkm'
		# if c == 'Horizontal Dilution of Precision':
		# 	columns[idx] = 'HorizontalDilutionofPrecision'
		# if c == 'Altitude(m)':
		# 	columns[idx] = 'Altitudem'
	columns = ''.join([str(k)+',' for k in columns])
	columns = columns.rstrip(',')
	#columns = columns + '\n'
	# print(f'[colfix] {len(inputline)} {len(columns)}')
	return columns

def read_csv_columns_raw(csv_filename):
	with open(csv_filename) as f:
		lineone = f.readline()
	return column_fixer(lineone)

def fix_csv_column_header(csv_filename):
	with open(csv_filename) as f:
		lines = f.readlines()
	try:
		lineone = lines[0]
	except IndexError as e:
		print(f'[fix] err {e}')
		return
	backup_file = str(csv_filename) + '.bak' # create backup
	with open(backup_file, 'w') as f:
		f.writelines(lines)

	columns = column_fixer(lineone)
	lines[0] = columns # replace column header

	with open(csv_filename, 'w') as f: # save modified csv
		f.writelines(lines)

def fix_csv_column(csvfile):
	pass

def database_init(engine):
	meta = MetaData(engine)
	print(f'[dbinit] dropping from {meta}')
	meta.drop_all(bind=engine, tables=[Torqfile.__table__, Torqlog.__table__])
	print(f'[dbinit] creating tables')
	meta.create_all(bind=engine, tables=[Torqfile.__table__, Torqlog.__table__])
	# meta.create_all(bind=engine)
	# inspector = inspect(engine)

class TorqForm(QObject):

	def __init__(self, ui_file=None, parent=None, engine=None, s_path=None):
		super(TorqForm, self).__init__(parent)
		ui_file = QFile(ui_file)
		ui_file.open(QFile.ReadOnly)

		loader = QUiLoader()
		self.window = loader.load(ui_file)
		ui_file.close()
		self.engine = engine
		Session = sessionmaker(bind=self.engine)
		self.session = Session()
		self.window.btn_readfiles.clicked.connect(self.btn_readfiles_clk)
		self.window.btn_sqlconnect.clicked.connect(self.sql_button_clk)
		self.window.torqfiles.itemClicked.connect(self.torqfiles_itmclick)
		self.window.actionExit.triggered.connect(self.shutdown)
		self.window.label_torqpath = self.window.findChild(QLabel, 'label_torqpath')
		self.window.label_dbstatus = self.window.findChild(QLabel, 'label_dbstatus')
		self.s_path = str(s_path)   # Path('c:/Users/kthor/Documents/development/torqlogs/')
		self.window.label_torqpath.setText(f'Path: {self.s_path}')
		self.db_status = self.get_db_status()
		self.window.setStatusBar(self.window.statusbar)
		self.window.statusbar.showMessage('Init...', 2000)
		self.window.show()

	def shutdown(self):
		print(f'quitting...')
		sys.exit(0)

	def get_db_status(self):
		try:
			conn = self.engine.connect()
		except OperationalError as e:
			conn = None
			self.window.label_dbstatus.setText('Database conn: Disconnected')
			self.window.debugtext.append(f'DB err {e}')
			return False
		else:
			self.window.label_dbstatus.setText('Database conn: OK')
			self.window.debugtext.append(f'DB connected {conn}')
			return True


	def sql_button_clk(self):
		try:
			conn = self.engine.connect()
		except OperationalError as e:
			conn = None
			self.window.label_dbstatus.setText(f'Database conn: Disconnected {e}')
			self.window.debugtext.append(f'DB err {e}')
		if conn is not None:
			hashlist = [k[0] for k in conn.execute('select hash from torqfiles')]
			self.window.debugtext.append(f'DB ok h: {len(hashlist)}')
			if len(hashlist) >= 1:
				self.window.label_dbstatus.setText(f'Database [{len(hashlist)}] conn: OK')
			else:
				self.window.label_dbstatus.setText(f'Database conn: errr h:{type(hashlist)} l: {len(hashlist)}')


	def torqfiles_itmclick(self, item):
		tfile = Torqfile(item.text())
		self.window.torqfile_detail.clear()
		try:
			conn = tfile.check_db_status(session=self.session)
		except OperationalError as e:
			conn = None
			details = f'db err {e}'
			detail_item = QListWidgetItem(details)
			self.window.torqfile_detail.insertItem(0, detail_item)
			self.window.label_dbstatus.setText('Database conn: Disconnected')
			self.window.debugtext.append(f'DB err {e}')
		if conn is not None:
			print(f'torqfilesitem click {item.text()} {tfile.name} {tfile.hash}')
			details = f'filename: {tfile.name}\nhash: {tfile.hash}\nlines: {tfile.num_lines}\ndbstatus: {tfile.exists_in_db}'
			detail_item = QListWidgetItem(details)
			self.window.torqfile_detail.insertItem(0, detail_item)
			# self.window.statusbar().showMessage(tr('db ok'))

	# noinspection PyTypeChecker
	def btn_readfiles_clk(self):
		self.window.torqfiles.clear()
		csvfiles = get_csv_files(searchpath=self.s_path)
		print(f'[csvfiles] {type(csvfiles)} {len(csvfiles)}')
		for idx, csvfile in enumerate(csvfiles):
			fn = str(csvfile)
			filename = QFile(fn)
			# filesize = QFileInfo(filename).size()
			filename_item = QListWidgetItem(fn)
			# sizeItem = QListWidgetItem("%d KB" % (int((filesize + 1023) / 1024)))
			# row = self.torqfiles.rowCount()
			self.window.torqfiles.insertItem(idx, filename_item)
			self.window.debugtext.append(f'item {fn} addedd. Total:{self.window.torqfiles.count()}')
		print(f'btn_readfiles_clked Total:{self.window.torqfiles.count()}')

	# def ok_handler(self):
	# 	language = 'None' if not self.line.text() else self.line.text()
	# 	print(f'lang: {language}')


def check_db(engine):
	print('Starting db check...')
	meta = MetaData(engine)
	inspector = inspect(engine)
	Session = sessionmaker(bind=engine)
	session = Session()

	tables = inspector.get_table_names()
	[print(f't: {table}') for table in tables]
	query = "SELECT  TABLE_NAME, TABLE_ROWS, DATA_LENGTH ,INDEX_LENGTH FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'torq';"
	#query = "SELECT * from torqfiles;"			
	conn = engine.connect()
	res = conn.execute(query).fetchall()
	print(len(res))
	#res = session.fetchall()
	print(res)
	dbfields = [FIELDMAPS[k] for k in FIELDMAPS]
	for field in dbfields:
		# print(f'[dbcheck] {field}')
		query = f"SELECT {field} FROM torqlogs;"
		res = conn.execute(query).fetchall()
		df = DataFrame(res)
		print(f'[dbcheck] {field} c: {df.count().values[0]}')
