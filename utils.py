# utils and db things here
from multiprocessing.sharedctypes import Value
import os
import sys
import re
from pathlib import Path
from loguru import logger
import inspect
from re import search, sub
from pandas import read_csv, DataFrame, to_datetime
from datetime import datetime
from dateutil.parser import ParserError
from sqlalchemy.exc import ProgrammingError
from hashlib import md5
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, inspect, select, BigInteger, Numeric, DateTime, text, BIGINT, BigInteger, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, DataError
import pymysql
from fieldmaps import FIELDMAPS
import json
from threading import Thread, active_count
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

MIN_FILESIZE = 4096
Base = declarative_base()

def get_csv_files(searchpath:Path, recursive=True):
	# todo fix globbing....
	# csvlist = searchpath.glob('tracklog*.csv')
	if not isinstance(searchpath, Path):
		searchpath = Path(searchpath)
	if not isinstance(searchpath, Path):
		logger.debug(f'[getcsv] err: searchpath {searchpath} is {type(searchpath)} need Path object')
		return []
	else:
		torqcsvfiles = [k for k in searchpath.glob("**/trackLog*.csv") if k.stat().st_size >= MIN_FILESIZE]
		return torqcsvfiles

class DataSender(Thread):
	def __init__(self, thread_id, torqfiles):
		Thread.__init__(self)
		self.torqfiles = torqfiles
		self.thread_id = thread_id
		self.name = f'ds-{self.thread_id}'
		self.totalfiles = len(torqfiles)
		self.sent_files = 0
		self.current_file = None
		self.kill = False
		self.finished = False
		logger.info(f'[datasender:{self.thread_id}] init totalfiles: {self.totalfiles}')

	def __repr__(self):
		return self.name

	def do_kill(self):
		logger.debug(f'[datasender:{self.thread_id}] do_kill')
		self.kill = True
		#self._stop()

	def get_remaining(self):
		return self.totalfiles - self.sent_files

	def get_status(self):
		if self.current_file:
			return f'[datasender:{self.thread_id}] cf: {self.current_file.name} fs:{self.current_file.filesize} rd:{self.current_file.read_done} buff:{self.current_file.get_buffersize()} timer: {(datetime.now() - self.current_file.send_time_start).total_seconds()} sent:{self.sent_files} total:{self.totalfiles} k:{self.kill} f:{self.finished} threadremain: {self.totalfiles - self.sent_files}'
		else:
			return f'[datasender:{self.thread_id}] no current file sent:{self.sent_files} total:{self.totalfiles} k:{self.kill} f:{self.finished} threadremain: {self.totalfiles - self.sent_files}'

	def run(self):
		while True:
			if self.kill:
				logger.info(f'[datasender:{self.thread_id}] kill signal')
				# self.join(timeout=1)
				return
			if self.totalfiles == self.sent_files or self.totalfiles == 0:
				logger.info(f'[datasender:{self.thread_id}] finished s:{self.sent_files} t:{self.totalfiles} self.torqfiles:{len(self.torqfiles)}')
				self.finished = True
				self.kill = True
				return
			send_res = None
			for torqfile in self.torqfiles:
				self.current_file = torqfile
				if self.kill:
					logger.debug(f'[datansender] killed...')
					return
				# logger.debug(f'[datasender:{self.thread_id}] status  s:{self.sent_files} t:{self.totalfiles}')
				if torqfile.send_done:
					logger.debug(f'[datasender:{self.thread_id}] skipping:{torqfile.name} res:{send_res} done_send: {torqfile.send_done} s:{self.sent_files} t:{self.totalfiles}')
					self.sent_files += 1
					self.torqfiles.remove(torqfile)
					self.totalfiles = len(self.torqfiles)
				else:
					logger.debug(f'[datasender:{self.thread_id}] id:{self.thread_id} sending:{torqfile.name} done_send: {torqfile.send_done} s:{self.sent_files} t:{self.totalfiles}')
					if not torqfile.send_failed:
						if not torqfile.read_done:
							torqfile.buffer = torqfile.read_csv_data()
							send_res = torqfile.send_data()
					if send_res == 0:
						# self.finished = True
						self.sent_files += 1
						logger.debug(f'[datasender:{self.thread_id}] send done res: {send_res} {torqfile.name} torqfile.send_done: {torqfile.send_done} sent:{self.sent_files} total:{self.totalfiles} remaining: {self.totalfiles - self.sent_files}')
						self.torqfiles.remove(torqfile)
						self.totalfiles = len(self.torqfiles)
						logger.debug(f'[datasender:{self.thread_id}] remaining {self.totalfiles}')
					else:
						logger.error(f'[datasender:{self.thread_id}] torqfile: {torqfile} err ? res: {send_res}')
					# torqfile.send_done = True


class Torqfile(Base):
	__tablename__ = 'torqfiles'
	fileid =  Column(Integer, primary_key=True)
	name = Column(String(255))
	hash = Column(String(255))
	tripid =  Column(String(255))

	def __str__(self):
		return(f'Torqfile: {self.name} hash: {self.hash}')

	def __repr__(self):
		return(f'{self.name}')

	def __init__(self, filename:Path, engine=None, fixer=False):
		self.name = str(filename)
		self.filename = filename
		self.fixer = fixer
		self.fixed = False
		if self.fixer:
			logger.info(f'will fix')
			self.buffer = self.do_self_fix()
			logger.info(f'fix done self.fixer: {self.fixer} fixed: {self.fixed} buff:{type(self.buffer)}')
		else:
			logger.warning('no fixing')
			self.buffer = None # self.read_csv_data()
		self.filesize = filename.stat().st_size
		self.engine = engine
		self.tripid = str(filename.parts[-2])
		self.columns = []
		self.buffer_parsed = False
		self.hash = self.gen_md5hash()
		self.exists_in_db = False
		# self.num_lines = sum(1 for line in open(self.name)) # how many lines in csv file
		self.send_done = False
		self.send_failed = False
		self.read_done = False
		self.init_time = datetime.now()
		self.send_time_start = datetime.now()
		# logger.debug(f'[torqfile] {self.name} init lines:{self.num_lines} ')
		# self.id = Column(Integer, primary_key=True)

	def get_columns(self):
		self.columns = read_csv_columns_raw(self.filename)
		return self.columns

	def get_buffersize(self):
		if not isinstance(self.buffer, DataFrame):
			return None
		result = None
		try:
			result = len(self.buffer)
		except TypeError as e:
			logger.error(f'[bufsize] {e} b:{type(self.buffer)}')
		return result

	def get_hash(self): # return own md5 hash
		if self.hash is None:
			self.gen_md5hash()
		return self.hash

	def gen_md5hash(self): # generate md5 hash
		hash = md5(open(self.filename,'rb').read()).hexdigest()
		self.hash = hash
		return hash

	def send_data(self): # send own csv data to database ...
		if self.send_failed:
			return -101
		if self.send_done:
			return 0
		self.send_time_start = datetime.now()
		send_result = -99
		t0 = datetime.now()
		# self.buffer = self.read_csv_data()  # (filepath_or_buffer=self.name, delimiter=',', low_memory=False, encoding='cp1252')
		# logger.debug(f'[readtime] {(datetime.now() - t0).total_seconds()} h:{self.hash} tripid:{self.tripid}')
		self.buffer['hash'] = self.hash
		self.buffer['tripid'] = self.tripid
		t1 = datetime.now()
		try:
			# logger.debug(f'[Torqfile] {self.send_time_start} trid: {self.tripid} to sql size: {len(self.buffer)}')
			self.buffer.to_sql(con=self.engine, name='torqlogs', if_exists='append', index=True, method='multi', chunksize=1000) # chunksize=5000,
			send_result = 0
			self.send_done = True
			logger.debug(f'[send_data] sqlsenddone {self.name} time: {(datetime.now() - t1).total_seconds()} tsendstart: {(datetime.now() - self.send_time_start).total_seconds()} tinit: {(datetime.now() - self.init_time).total_seconds()} trid: {self.tripid} bufflen: {len(self.buffer)} ')
		except pymysql.err.OperationalError as e:
			self.send_failed = True
			logger.error(f'[send_data] {self.name} failed: OPERR ')
			send_result = -1
		except (DataError, pymysql.err.DataError) as e:
			self.send_failed = True
			logger.error(f'[send_data] {self.name} failed: DATAERR {self.name} {type(e)} {e.args[0]}')
			send_result = -2
		except OperationalError as e:
			self.send_failed = True
			self.send_done = True
			logger.error(f'[send_data] {self.name} failed: sqlalchemy operr {e.code} {e.args[0]}')
			send_result = -3
		except AttributeError as e:
			self.send_failed = True
			logger.error(f'[send_data] {self.name} failed: AttributeError ')
			send_result = -4
		return send_result

	def do_self_fix(self):
		fixedname = self.filename.parent.joinpath('trackLog-fixed.csv')
		logger.debug(f'[fix] {self.name} start ')
		buffer = self.read_csv_data()
		fixedbuffer = self.buffer_fixer(buffer)
		self.name = fixedname.name
		self.filename = fixedname
		logger.debug(f'[fix] got buffer saving ')
		fixedbuffer.to_csv(self.filename, encoding='utf-8', index=False, )
		self.fixed = True
		self.read_done = True
		return fixedbuffer
		logger.debug(f'[fix] save done ')

	def read_csv_data(self) -> DataFrame: # read own data file
		t1 = datetime.now()
		logger.info(f'[torqfile] {self.name} read start')
		buffer = None # DataFrame()
		if self.name == '...':
			logger.error(f'[Torqfile] ERR ??? name:{self.name}')
			return None
		try:			
			buffer = read_csv(self.filename, delimiter=',', low_memory=False, encoding='cp1252', na_values=0)
			logger.info(f'[torqfile] {self.name} read csv done')
		except Exception as e:
			logger.error(f'[Torqfile] read_csv_data err {e} {self.name}')
		if not self.fixed:
			fixedbuffer = self.buffer_fixer(buffer)
			return fixedbuffer
		else:
			return buffer

	def buffer_fixer(self, buffer=None):
		t1 = datetime.now()
		logger.info(f'[torqfile] {self.name} buffer_fixer start')
		buffer.replace('-','0', inplace=True)
		buffer.replace('âˆž','0', inplace=True)
		buffer.replace('Ã¢ÂˆÂž','0', inplace=True)
		cols = [column_fixer(k) for k in buffer.columns]
		fields = [FIELDMAPS[k] for k in cols]
		# fields = [FIELDMAPS.get(k, '') for k in cols]
		buffer.columns = fields
		for f in fields:
			if f == 'GPSTime' or f == 'DeviceTime':
				try:
					buffer[f] = to_datetime(buffer[f])
				except ParserError as e:
					logger.error(f'[datetime] {self.name} err {e}')
			else:
				buffer[f].replace('', '0', inplace=True)
				buffer[f].replace('-3402823534620772000000000000000000000', '0', inplace=True)
				buffer[f].replace('-3.402823534620772e+36', '0', inplace=True)
				buffer[f].replace(-3.402823534620772e+36, '0', inplace=True)
				buffer[f].replace('612508207723425200000000000000000000000', '0', inplace=True)
				buffer[f].replace('612508207723425', '0', inplace=True)
				buffer[f].replace('612508207723425231880386882817669201920.0', '0', inplace=True)
				buffer[f].replace('612508207723425231880386882817669201920', '0', inplace=True)
				buffer[f].replace(612508207723425231880386882817669201920.0, '0', inplace=True)
				buffer[f].replace('340282346638528860000000000000000000000', '0', inplace=True)
				buffer[f].replace('-3.4028236187100775e+36', '0', inplace=True)
				buffer[f].replace(-3.4028236187100775e+36, '0', inplace=True)
				buffer[f].replace('-3.402823618710077e+36', '0', inplace=True)
				buffer[f].replace(-3.402823618710077e+36, '0', inplace=True)
				buffer[f].replace('-3402823618710077500000000000000000000', '0', inplace=True)
				buffer[f].replace(-3402823618710077500000000000000000000, '0', inplace=True)
				buffer[f].replace(3.402823466385289e+38, '0', inplace=True)
		logger.info(f'[torqfile] fix done buffersize: {len(buffer)} time: {(datetime.now() - t1).total_seconds()}')
		self.read_done = True
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

class Torqlog(Base):
	__tablename__ = 'torqlogs'
	torqentryid =  Column(Integer, primary_key=True)
	hash = Column(String(255))
	tripid =  Column(String(255))
	index =  Column(Integer)
	AccelerationSensorTotalg = Column(Numeric, default=0)
	AccelerationSensorXaxisg = Column(Numeric, default=0)
	AccelerationSensorYaxisg = Column(Numeric, default=0)
	AccelerationSensorZaxisg = Column(Numeric, default=0)
	Actualenginetorque = Column(Numeric, default=0)
	Altitudem = Column(Numeric, default=0)
	AndroiddeviceBatteryLevel = Column(Numeric, default=0)
	Averagetripspeedwhilststoppedormovingkm = Column(Numeric, default=0)
	Averagetripspeedwhilststoppedormovingonlykm = Column(Numeric, default=0)
	Bearing = Column(Numeric, default=0)
	COaing = Column(Numeric, default=0)
	COing = Column(Numeric, default=0)
	costpermilekminst = Column(Numeric, default=0)
	costpermilekm = Column(Numeric, default=0)
	DeviceTime = Column(DateTime, server_default=text('NOW()')) # Column(String(255), default=0)
	DistancetoemptyEstimatedkm = Column(Numeric, default=0)
	DistancetravelledwithMIL = Column(Numeric, default=0)
	EngineCoolantTemperatureF = Column(Numeric, default=0)
	EnginekWAtthewheelskW = Column(Numeric, default=0)
	EngineLoad = Column(Numeric, default=0)
	EngineRPMrpm = Column(Numeric, default=0)
	Fuelcosttripcost = Column(Numeric, default=0)
	Fuelflowratelhr = Column(Numeric, default=0)
	Fuelflowrateccmin = Column(Numeric, default=0)
	Fuelpressurekpa = Column(Numeric, default=0)
	FuelRailPressurekpa = Column(Numeric, default=0)
	FuelRemainingCalculatedfromvehicleprofile = Column(Numeric, default=0)
	Fuelusedtripl = Column(Numeric, default=0)
	GPSAccuracym = Column(Numeric, default=0)
	GPSAltitudem = Column(Numeric, default=0)
	GPSBearing = Column(Numeric, default=0)
	GPSLatitude = Column(Numeric, default=0)
	GPSLongitude = Column(Numeric, default=0)
	GPSSatellites = Column(Numeric, default=0)
	GPSSpeedkm = Column(Numeric, default=0)
	GPSTime = Column(DateTime, server_default=text('NOW()'))
	# GPSTime = Column(String(255), default=0)
	# Column('y', DateTime, server_default=text('NOW()'))
	GPSvsOBDSpeeddifferencekm = Column(Numeric, default=0)
	GravityXG = Column(Numeric, default=0)
	GravityYG = Column(Numeric, default=0)
	GravityZG = Column(Numeric, default=0)
	HorizontalDilutionofPrecision = Column(Numeric, default=0)
	HorsepowerAtthewheelshp = Column(Numeric, default=0)
	IntakeAirTemperatureF = Column(Numeric, default=0)
	IntakeManifoldPressurekpa = Column(Numeric, default=0)
	KilometersPerLitreInstantkpl = Column(Numeric, default=0)
	KilometersPerLitreLongTermAveragekpl = Column(Numeric, default=0)
	Latitude = Column(Numeric, default=0)
	LitresPer100KilometerInstantl = Column(Numeric, default=0)
	LitresPer100KilometerLongTermAveragel = Column(Numeric, default=0)
	Longitude = Column(Numeric, default=0)
	MassAirFlowRateg = Column(Numeric, default=0)
	MilesPerGallonInstantmpg = Column(Numeric, default=0)
	MilesPerGallonLongTermAveragempg = Column(Numeric, default=0)
	SpeedOBDkm = Column(Numeric, default=0)
	SpeedGPSkm = Column(Numeric, default=0)
	Torqueftlb = Column(Numeric, default=0)
	TripaverageKPLkpl = Column(Numeric, default=0)
	TripaverageLitres = Column(Numeric, default=0)
	TripaverageMPGmpg = Column(Numeric, default=0)
	Tripdistancestoredinvehicleprofilekm = Column(Numeric, default=0)
	TripDistancekm = Column(Numeric, default=0)
	TripTimeSincejourneystarts = Column(Numeric, default=0)
	Triptimewhilstmovings = Column(Numeric, default=0)
	Triptimewhilststationarys = Column(Numeric, default=0)
	TurboBoostVacuumGaugebar = Column(Numeric, default=0)
	VoltageOBDAdapterV = Column(Numeric, default=0)
	VolumetricEfficiencyCalculated = Column(Numeric, default=0)

def column_fixer(inputline):
	columns = inputline.split(',') # split
	columns = [sub('\n', '', col) for col in columns] # remove \n 's
	columns = [sub(' ', '', col) for col in columns] # degree symbol
	columns = [sub('-', '', col) for col in columns] # degree symbol
	columns = [sub(',', '', col) for col in columns] # degree symbol
	columns = [sub('â', '', col) for col in columns] # degree symbol
	columns = [sub('Â', '', col) for col in columns] # symbol cleanup
	columns = [sub('Ã¢', '', col) for col in columns] # symbol cleanup
	columns = [sub('Ã‚', '', col) for col in columns] # symbol cleanup
	columns = [sub('CO,‚', 'CO', col) for col in columns] # symbol cleanup
	columns = [sub(r'^\s', '', k) for k in columns] # remove extra spaces from start of col name
	columns = ''.join([str(k)+',' for k in columns])
	columns = columns.rstrip(',')
	# columns = columns.lrstrip(',')
	return columns

def read_csv_columns_raw(csv_filename):
	with open(csv_filename) as f:
		lineone = f.readline()
	return column_fixer(lineone)

def init_db(engine):
	database_init(engine)

def database_init(engine):
	meta = MetaData(engine)
	t1 = datetime.now()
	logger.debug(f'[dbinit] {(datetime.now() - t1).total_seconds()} dropping from {meta}')
	meta.drop_all(bind=engine, tables=[Torqfile.__table__, Torqlog.__table__])
	logger.debug(f'[dbinit] {(datetime.now() - t1).total_seconds()} creating tables')
	meta.create_all(bind=engine, tables=[Torqfile.__table__, Torqlog.__table__])
	logger.debug(f'[dbinit] {(datetime.now() - t1).total_seconds()} done')

def dump_db(args, engine):
	conn = engine.connect()
	t1 = datetime.now()
	logger.debug(f'[dump] {(datetime.now() - t1).total_seconds()} Getting data and dumping to: {args.dump_db}')
	data = json.load(conn.execute("SELECT * FROM torqlogs").fetchall())
	#dumpdata = json.load()
	dumpfile = open(args.dump_db, 'w')
	#dumpdata = json.dump(data, )
	logger.debug(f'[dump] {(datetime.now() - t1).total_seconds()} done')
#	with open(args.dump_db, 'w'dumpfile) as dumpfile:
#		logger.debug(f'[dump] data {len(data)} writing...')
#		dumpfile.write(dumpdata, encoding=)

def check_db(engine):
	logger.debug('Starting db check...')
	meta = MetaData(engine)
	Session = sessionmaker(bind=engine)
	session = Session()
	conn = engine.connect()
	torqfiles = conn.execute('SELECT fileid,name,hash from torqfiles').fetchall()
	dates = None
	count = 0
	firstdate = None
	lastdate = None
	dateformat = '%d-%b-%Y %H:%M:%S.%f'
	for tfile in torqfiles:
		try:
			# dates = [k for k in conn.execute(f"""select "DeviceTime" from "torqlogs" WHERE "hash"='{tfile[2]}' ORDER BY "DeviceTime" ASC""").fetchall()]
			#dates = [k for k in conn.execute(f"""select DeviceTime from torqlogs WHERE hash='{tfile[2]}' ORDER BY DeviceTime ASC""").fetchall()]
			firstdate = [k for k in conn.execute(f"""select DeviceTime from torqlogs WHERE hash='{tfile[2]}' ORDER BY DeviceTime ASC LIMIT 1""").fetchone()][0]
			lastdate = [k for k in conn.execute(f"""select DeviceTime from torqlogs WHERE hash='{tfile[2]}' ORDER BY DeviceTime DESC LIMIT 1""").fetchone()][0]
			# logger.debug(f'[l] {len(firstdate)} {len(lastdate)}')
			if len(firstdate) == 24:
				dateformat = '%d-%b-%Y %H:%M:%S.%f'
			if len(firstdate) == 20:
				dateformat = '%d-%b-%Y %H:%M:%S'
		except ProgrammingError as e:
			logger.error(f'[checkdb] dates ProgrammingError {e}')
			dates = None
		first = datetime.strptime(firstdate, dateformat)
		last = datetime.strptime(lastdate, dateformat)
		datediff = last - first
		# logger.debug(f'[checkdb] f: {firstdate} l: {lastdate} f:{first} l:{last} l: {len(firstdate)} {len(lastdate)}')
		count += 1
		logger.debug(f'[checkdb] {count}/{len(torqfiles)} file: {tfile[-2]} duration: {datediff}')
	logger.debug(f'[checkdone] closing session')
	session.close()
	logger.debug(f'[checkdone] dispose engine')
	engine.dispose()
	# session.close()

def mainfile(args, engine):
	column_list = []
	torqfiles = []
	Session = sessionmaker(bind=engine)
	session = Session()
	try:
		conn = engine.connect()
	except OperationalError as e:
		logger.error(f'[sql] err {e}')
		sys.exit(-1)
	try:
		hashlist = [k[0] for k in conn.execute('select hash from torqfiles')]
	except ProgrammingError as e:
		logger.error(f'[torq] err {e.code} {e.orig}')
		database_init(engine)
		hashlist = []
	tfile = Torqfile(args.file, engine)
	if tfile.hash in hashlist: # already have file in database, skip
			logger.debug(f'[torqtool] {tfile.name} already exists in database, skipping')
			return 0
	else:
		found_cols = tfile.get_columns()
		column_list.append(found_cols)
		session.add(tfile)
		logger.debug(f'[csvfile] {args.file} cols: {len(found_cols)} / {len(column_list)} ')
		session.commit()
		maincolumn_list = make_column_list(column_list) # maincolum_list = master list of columns
		tfile.update_columns(engine=engine, cols=maincolumn_list)
		tfile.send_data()

def make_column_list(columnlist):
	templist = []
	for list  in columnlist:
		for col in list:
			if col in templist:
				pass
			else:
				templist.append(col)
				# logger.debug(f'[templist] {len(templist)} added {col}')
	templist = sorted(set(templist))
	with open('tempfields.txt', 'a') as f:
		f.writelines(templist)
	return templist

def helper_func(args):
	t1 = datetime.now()
	torqfile = args[0]
	engine = args[1]
	maincolumn_list = args[2]
	logger.debug(f'[helper] {torqfile} {engine} {maincolumn_list}')
	torqfile.update_columns(engine=engine, cols=maincolumn_list)
	logger.debug(f'[helper] update columns done')
	torqfile.send_data(engine=engine, cols=maincolumn_list)
	logger.debug(f'[helper] send done')
	return str(datetime.now() - t1)

class DataWorker(Thread):
	def __init__(self, name, torqfile, engine, maincolumn_list):
		Thread.__init__(self)
		self.name = name
		self.torqfile = torqfile
		self.engine = engine
		self.maincolumn_list = maincolumn_list

	def run(self):
		t1 = datetime.now()
		logger.debug(f'[DataWorker][{(datetime.now() - t1).total_seconds()}] {self.name} {self.torqfile} {self.engine} ')
		self.torqfile.update_columns(engine=self.engine, cols=self.maincolumn_list)
		logger.debug(f'[DataWorker][{(datetime.now() - t1).total_seconds()}] update columns done')
		self.torqfile.send_data(engine=self.engine, cols=self.maincolumn_list)
		logger.debug(f'[DataWorker][{(datetime.now() - t1).total_seconds()}] send done')
	def join(self, **kwargs):
		self.kill = True
		super().join()

class MainThread(Thread):
	def __init__(self, name):
		Thread.__init__(self)
		self.name = name
		self.kill = False
	def run(self):
		while True:
			if self.kill:
				return

class DataMain(Thread):
	def __init__(self, name):
		Thread.__init__(self)
		self.name = name
		self.kill = False
		self.dataq = Queue()
		self.t_counter = 0
		self.w_count = 0
		self.csv_count = 0
	def run(self):
		t1 = datetime.now()
		while True:
			try:
				torqfile, column_list, engine, csv_count = self.dataq.get_nowait()
				self.csv_count = csv_count
				if torqfile:
					logger.debug(f'[DataMain-{self.t_counter}:{self.csv_count}:{self.w_count}] [{(datetime.now() - t1).total_seconds()}] [{active_count()}] got t: {torqfile} ')
					self.t_counter += 1
					# logger.debug(f'[DataMain]: {active_count()}')
					try:
						res = self.send_data(torqfile, column_list, engine)
					except RuntimeError as e:
						logger.error(f'[DataMain][{(datetime.now() - t1).total_seconds()}] ERROR {e}')
						res = None
					if res:
						logger.debug(f'[DataMain-{self.t_counter}:{self.csv_count}:{self.w_count}] [{(datetime.now() - t1).total_seconds()}] [{active_count()}] q result from {torqfile} is {res}')
						self.dataq.task_done()
			except Empty:
				pass
			if self.kill:
				return

	def join(self, **kwargs):
		self.kill = True
		super().join()

	def add_data(self, data):
		pass

	def send_data(self, torqfile, maincolumn_list, engine):
		self.w_count += 1
		worker = DataWorker(name=torqfile.name, torqfile=torqfile, engine=engine, maincolumn_list=maincolumn_list)
		logger.debug(f'[DataMain] W {self.w_count} {worker}')
		worker.daemon = True
		worker.run()
		return self.w_count

	def send_data_old(self, torqfile, maincolumn_list, engine):
		with ProcessPoolExecutor(max_workers=5) as executor:
			t1 = datetime.now()
			logger.debug(f'[DataMain] [{(datetime.now() - t1).total_seconds()}] sending data {torqfile} {engine}')
			#name = '[start_workers2]' + str(sleep_time)
			args = [(torqfile, engine, maincolumn_list)]
			results = executor.map(helper_func, args)
			for result in results:
				logger.debug(f'[DataMain] [{(datetime.now() - t1).total_seconds()}] res: {result}')


def parse_csvfile(csv_filename):
	if len(csv_filename.parent.name) == 13: # torq creates folders based on unix time with milliseconds
		dateguess = datetime.utcfromtimestamp(int(csv_filename.parent.name)/1000)
	else: # normal....
		dateguess = datetime.utcfromtimestamp(int(csv_filename.parent.name))
	# logger.debug(datetime.utcfromtimestamp(16294475123))
	return f'{dateguess}'

def fix_timedate_gps(buffer):
	# fix timedate formatting
	return buffer

def fix_timedate_device(buffer):
	# fix timedate formatting
	return buffer