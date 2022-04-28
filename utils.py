# utils and db things here
from lib2to3.pgen2.token import OP
from multiprocessing.sharedctypes import Value
from multiprocessing import Process
import os
import sys
import re
from pathlib import Path
from loguru import logger
logger.add('tool.log')
import inspect
from re import T, search, sub
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
badvals_str = ['3.402823466385289e+38', '-3402823534620772000000000000000000000','-3.402823534620772e+36','-3.4028236187100775e+36','-3.402823618710077e+36','-3402823618710077500000000000000000000','612508207723425200000000000000000000000','612508207723425231880386882817669201920','340282346638528860000000000000000000000']
badvals = [3.402823466385289e+38, -3402823534620772000000000000000000000,-3.402823534620772e+36,-3.4028236187100775e+36,-3.402823618710077e+36,-3402823618710077500000000000000000000,612508207723425200000000000000000000000,612508207723425231880386882817669201920,340282346638528860000000000000000000000]

def get_csv_files(searchpath:Path, recursive=True):
	# todo fix globbing....
	# csvlist = searchpath.glob('tracklog*.csv')
	if not isinstance(searchpath, Path):
		searchpath = Path(searchpath)
	if not isinstance(searchpath, Path):
		logger.debug(f'[getcsv] err: searchpath {searchpath} is {type(searchpath)} need Path object')
		return []
	else:
		torqcsvfiles = [k for k in searchpath.glob("**/trackLog.csv") if k.stat().st_size >= MIN_FILESIZE]
		return torqcsvfiles

class DataProcessor(Process):
	def __init__(self, thread_id, torqfiles, max_workers):
		Process.__init__(self)
		self.torqfiles = torqfiles
		self.thread_id = thread_id
		self.max_workers = max_workers
		self.name = f'ds-{self.thread_id}'
		self.totalfiles = len(torqfiles)
		self.remainingfiles = self.totalfiles
		self.sent_files = 0
		self.kill = False
		self.finished = False
		self.fixers_done = False
		self.sender_status = 'init'
		if self.totalfiles == 0:
			logger.warning(f'[ds] no files')
			self.kill = True

	def __repr__(self):
		return self.name

	def do_kill(self):
		logger.debug(f'[datasender:{self.thread_id}] do_kill f:{self.finished} fd:{self.fixers_done} fs:{self.sender_status} tf:{self.totalfiles} sf:{self.sent_files}')
		self.kill = True
		self.sender_status = 'killed'
		#self._stop()

	def get_remaining(self):
		return self.remainingfiles

	def get_filestatus(self):
		logger.info(f'[datasender:{self.thread_id}] filestatus sent:{self.sent_files} total:{self.totalfiles} f:{self.finished} threadremain: {self.totalfiles - self.sent_files} f:{self.sender_status} fd:{self.fixers_done}')
		for tfile in self.torqfiles:
			logger.info(f'[tfile] {tfile}')

	def get_status(self):
		self.get_filestatus()
		return f'[datasender:{self.thread_id}] no cf sent:{self.sent_files} total:{self.totalfiles} threadremain: {self.totalfiles - self.sent_files} f:{self.sender_status} fd:{self.fixers_done} rem:{self.remainingfiles}'
		#if self.current_file:
		#	return f'[datasender:{self.thread_id}] cf: {self.current_file.name} fs:{self.current_file.filesize} rd:{self.current_file.read_done} buff:{self.current_file.get_buffersize()} sent:{self.sent_files} total:{self.totalfiles} f:{self.finished} threadremain: {self.totalfiles - self.sent_files} f:{self.sender_status} fd:{self.fixers_done}'
		#else:
		#	return f'[datasender:{self.thread_id}] no cf sent:{self.sent_files} total:{self.totalfiles} threadremain: {self.totalfiles - self.sent_files} f:{self.sender_status} fd:{self.fixers_done} rem:{self.remainingfiles}'

	def run_fixers(self):
		if self.fixers_done:
			self.sender_status = 'idle'
			return
		needfixing = []
		for torqfile in self.torqfiles:
			# logger.debug(f'[r] {torqfile} f:{torqfile.fixed}')
			if not torqfile.fixed:
				needfixing.append(torqfile)
				dbgmsg = f'fixing {torqfile} nf:{len(needfixing)} tf:{len(self.torqfiles)}'
				logger.debug(dbgmsg)

		if len(needfixing) == 0:
			logger.warning(f'[f] returning')
			self.sender_status = 'idle'
			self.fixers_done = True
			return
		futures = []
		with ProcessPoolExecutor(max_workers=self.max_workers) as executor: # max_workers=self.max_workers
			self.sender_status = 'busy'
			for torqfile in needfixing:
				futures.append(executor.submit(torqfile.do_self_fix(), 0))
		for fut in futures:
			res = fut
		# self.fixers_done = True
		self.sender_status = 'idle'
		logger.info(f'[f] fix finished fs:{self.sender_status} fd:{self.fixers_done} nf:{len(needfixing)} tf:{len(self.torqfiles)}')

	def run(self):
		if self.totalfiles == 0:
			logger.warning(f'[ds] no files for thread..sk:{self.kill}')
			self.kill = True
			self.finished = True
			return
		logger.debug(f'[ds] thread starting totalfiles: {len(self.torqfiles)}')
		send_res = -9999
		cnt = 0
		if self.remainingfiles == 0:
			logger.info(f'[ds] thread finished sent:{self.sent_files} tf:{len(self.torqfiles)} rem:{self.remainingfiles}')
			self.finished = True
			self.kill = True
			return
		for torqfile in self.torqfiles:
			if not torqfile.fixed:
				logger.debug(f'sending {torqfile} to fixer')
				torqfile.do_self_fix()
				torqfile.fixed = True
				torqfile.read_done = True
				logger.debug(f'done fixing {torqfile} ')
			if torqfile.fixed and not torqfile.send_done:
				cnt += 1
				logger.debug(f'sending {torqfile} bf:{len(torqfile.buffer)}')
				send_res = torqfile.send_data()
				logger.debug(f'done sending {torqfile} bf:{len(torqfile.buffer)} res:{send_res}' )
				if send_res == 0:
					torqfile.send_done = True
					self.sent_files += 1
					self.remainingfiles -= 1
					logger.debug(f'[ds] done sending {torqfile} bf:{len(torqfile.buffer)} sent:{self.sent_files} rem:{self.get_remaining()} {self.remainingfiles}')
					self.sender_status = 'idle'
					if self.remainingfiles == 0:
						return
		if self.sent_files == len(self.torqfiles) or self.remainingfiles == 0:
			logger.info(f'[ds] thread finished sent:{self.sent_files} tf:{len(self.torqfiles)} rem:{self.remainingfiles}')
			self.sender_status = 'idle'
			self.finished = True
			self.kill = True
			return


class Torqfile(Base):
	__tablename__ = 'torqfiles'
	fileid =  Column(Integer, primary_key=True)
	name = Column(String(255))
	hash = Column(String(255))
	tripid =  Column(String(255))

	def __str__(self):
		return(f'T: {self.name} b:{len(self.buffer)} f:{self.fixed} rd:{self.read_done} sd:{self.send_done}')

	def __repr__(self):
		return(f'{self.name}')

	def __init__(self, filename:Path, engine=None, fixer=True, sqlchunksize=1000):
		self.name = str(filename)
		self.filename = filename
		self.fixer = fixer
		self.fixed = False
		self.read_done = False
		self.buffer = DataFrame()
		self.filesize = filename.stat().st_size
		self.engine = engine
		self.tripid = str(filename.parts[-2])
		self.columns = []
		self.hash = self.gen_md5hash()
		self.exists_in_db = False
		self.send_done = False
		self.send_failed = False
		self.init_time = datetime.now()
		self.send_time_start = datetime.now()
		self.sqlchunksize = sqlchunksize

	def get_columns(self):
		self.columns = read_csv_columns_raw(self.filename)
		return self.columns

	def get_buffersize(self):
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
			logger.error(f'[T] {self.name} send failed')
			return -101
		if self.send_done:
			logger.warning(f'[T] {self.name} send already done....')
			return 0
		self.send_time_start = datetime.now()
		send_result = -99
		t0 = datetime.now()
		self.buffer['hash'] = self.hash
		self.buffer['tripid'] = self.tripid
		t1 = datetime.now()
		if len(self.buffer) == 0:
			logger.error(f'[T] {self.name} buffer empty not sending ')
			self.send_failed = True
			return -13
		try:
			# logger.debug(f'[T] {self.filename} tosql {self.send_time_start} sql bufsize: {len(self.buffer)} fixer:{self.fixer} fixed:{self.fixed}')
			self.buffer.to_sql(con=self.engine, name='torqlogs', if_exists='append', index=False, method='multi', chunksize=self.sqlchunksize) # chunksize=5000,
			send_result = 0
			self.send_done = True
			logger.debug(f'[T] {self.filename} tosqldone time: {(datetime.now() - t1).total_seconds()} tsendstart: {(datetime.now() - self.send_time_start).total_seconds()} tinit: {(datetime.now() - self.init_time).total_seconds()}  bufflen: {len(self.buffer)} self.sqlchunksize: {self.sqlchunksize}')
		except pymysql.err.OperationalError as e:
			self.send_failed = True
			logger.error(f'[T] {self.filename} failed: OPERR ')
			send_result = -1
		except ProgrammingError as e:
			self.send_failed = True
			logger.error(f'[T] {self.filename} failed: progerr {e} ')
			send_result = -1
		except (DataError, pymysql.err.DataError) as e:
			self.send_failed = True
			logger.error(f'[T] {self.filename} failed: DATAERR {self.name} {type(e)} {e.args[0]}')
			send_result = -2
		except OperationalError as e:
			self.send_failed = True
			self.send_done = True
			logger.error(f'[T] {self.filename} failed: sqlalchemy operr {e.code} {e.args[0]}')
			send_result = -3
		except AttributeError as e:
			self.send_failed = True
			logger.error(f'[T] {self.filename} failed: AttributeError ')
			send_result = -4
		return send_result

	def do_self_fix(self):
		# fixedname = self.filename.parent.joinpath('trackLog-fixed.csv')
		logger.debug(f'[tfix] {self.filename} start sf:{self.fixed} rd:{self.read_done}')
		# buffer = self.read_csv_data()
		t0 = datetime.now()
		buffer = read_csv(self.filename, delimiter=',', low_memory=False, encoding='cp1252', na_values=0)
		t_read = datetime.now() - t0
		t1 = datetime.now()
		fixedbuffer = self.buffer_fixer(buffer)
		t_fix = datetime.now() - t1
		# self.name = fixedname.name
		# self.filename = fixedname
		# logger.debug(f'[fix] {self.filename} saving ')
		# fixedbuffer.to_csv(self.filename, encoding='utf-8', index=False, )
		self.fixed = True
		self.read_done = True
		logger.debug(f'[tfix] {self.filename} done sf:{self.fixed} rd:{self.read_done} readtime: {t_read} fixtime: {t_fix} buf: {len(buffer)} buffixed: {len(fixedbuffer)}')
		return fixedbuffer

	def read_csv_data_old(self) -> DataFrame: # read own data file
		t1 = datetime.now()
		logger.info(f'[T] {self.name} read start b:{len(self.buffer)} fixer:{self.fixer} fixed:{self.fixed}')
		fixedbuffer = DataFrame()
		# buffer = DataFrame()
		if self.name == '...':
			logger.error(f'[T] ERR ??? name:{self.name}')
			return None
		if not self.fixed or len(self.buffer) == 0:
			if len(self.buffer) == 0:
				# logger.warning(f'[t] {self.name} buffer empty? sf:{self.fixer} f:{self.fixed}')
				fixedbuffer = self.buffer_fixer(read_csv(self.filename, delimiter=',', low_memory=False, encoding='cp1252', na_values=0))
				self.read_done = True
				self.fixed = True
			if self.fixer and not self.fixed:
				logger.warning(f'[t] {self.name} fixing buffer sf:{self.fixer} f:{self.fixed}')
				fixedbuffer = self.buffer_fixer(self.buffer)
				self.read_done = True
				self.fixed = True
				# logger.info(f'[t] {self.name} fixed sf:{self.fixer} f:{self.fixed} fixb:{len(fixedbuffer)}')
		else:
			if len(self.buffer) == 0:
				logger.warning(f'[t] {self.name} no buffer ?')
				buffer = read_csv(self.filename, delimiter=',', low_memory=False, encoding='cp1252', na_values=0)
				fixedbuffer = self.buffer_fixer(buffer)
		logger.info(f'[T] {self.name} read csv done sf:{self.fixer} f:{self.fixed} bf:{len(fixedbuffer)}')
		return fixedbuffer

	def buffer_fixer(self, buffer=None):
		t0 = datetime.now()
		if self.fixed:
			logger.warning(f'[bf] {self.filename} already fixed? self.buffer: {len(self.buffer)} buff:{len(buffer)} sf:{self.fixed} rd:{self.read_done}')
			return buffer
		# logger.info(f'[bf] {self.filename} buffer_fixer start')
		buffer.replace('-','0', inplace=True)
		buffer.replace('âˆž','0', inplace=True)
		buffer.replace('Ã¢ÂˆÂž','0', inplace=True)
		cols = [column_fixer(k) for k in buffer.columns]
		fields = [FIELDMAPS[k] for k in cols]
		# fields = [FIELDMAPS.get(k, '') for k in cols]
		buffer.columns = fields
		# logger.debug(f'[fix] {self.filename} t0 {t0}')
		try:
			if buffer.get('GPSTime') is not None:
				buffer['GPSTime'].replace('-', buffer['GPSTime'][0], inplace=True)
				buffer['GPSTime'].replace('0', buffer['GPSTime'][0], inplace=True)
				buffer['GPSTime'] = to_datetime(buffer['GPSTime'], errors='raise', infer_datetime_format=True)
			if buffer.get('GPS Time') is not None:
				buffer['GPS Time'] = to_datetime(buffer['GPS Time'], errors='raise', infer_datetime_format=True)
		except (ParserError, KeyError) as e:
			logger.error(f'[bferr] gpstime {self.filename} {e}')
		try:
			if buffer.get('DeviceTime') is not None:
				buffer['DeviceTime'] = to_datetime(buffer['DeviceTime'], errors='raise', infer_datetime_format=True)
			if buffer.get('Device Time') is not None:
				buffer['Device Time'] = to_datetime(buffer['Device Time'], errors='raise', infer_datetime_format=True)
		except (ParserError, KeyError) as e:
			logger.error(f'[bferr] devicetime {self.filename} {e}')
		for f in fields:
			for badv in badvals_str:
				buffer[f].replace(badv, 0, inplace=True)
			for badv in badvals:
				buffer[f].replace(badv, 0, inplace=True)
		self.fixed = True
		self.buffer = buffer
		return buffer

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
	fixed_cols = column_fixer(lineone)
	return fixed_cols

def init_db(engine):
	database_init(engine)

def database_init(engine):
	meta = MetaData(engine)
	t1 = datetime.now()
	logger.debug(f'[dbinit] {(datetime.now() - t1).total_seconds()} dropping from {meta}')
	try:
		meta.drop_all(bind=engine, tables=[Torqfile.__table__, Torqlog.__table__], checkfirst=False)
	except OperationalError as e:
		logger.error(e)
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

def get_torqlog_table(engine):
	# meta = MetaData(engine)
	# Session = sessionmaker(bind=engine)
	# session = Session()
	conn = engine.connect()
	torqfiles = conn.execute('SELECT * FROM torqlogs').fetchall()
	engine.dispose()
	return len(torqfiles)

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


def parse_csvfile(csv_filename):
	if len(csv_filename.parent.name) == 13: # torq creates folders based on unix time with milliseconds
		dateguess = datetime.utcfromtimestamp(int(csv_filename.parent.name)/1000)
	else: # normal....
		dateguess = datetime.utcfromtimestamp(int(csv_filename.parent.name))
	# logger.info(f'[dateguess] fn:{csv_filename} lfn:{len(csv_filename.parent.name)} dg:{dateguess} ')
	return f'{dateguess}'

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


def SendProcess(torqfile): # send own csv data to database ...
	if torqfile.send_failed:
		logger.error(f'[T] {torqfile.name} send failed')
		return -101
	if torqfile.send_done:
		logger.warning(f'[T] {torqfile.name} send already done....')
		return 0
	torqfile.send_time_start = datetime.now()
	send_result = -99
	t0 = datetime.now()
	torqfile.buffer['hash'] = torqfile.hash
	torqfile.buffer['tripid'] = torqfile.tripid
	t1 = datetime.now()
	if len(torqfile.buffer) == 0:
		logger.error(f'[T] {torqfile.name} buffer empty not sending ')
		torqfile.send_failed = True
		return -13
	try:
		# logger.debug(f'[T] {torqfile.filename} tosql {torqfile.send_time_start} sql bufsize: {len(torqfile.buffer)} fixer:{torqfile.fixer} fixed:{torqfile.fixed}')
		torqfile.buffer.to_sql(con=torqfile.engine, name='torqlogs', if_exists='append', index=False, method='multi', chunksize=torqfile.sqlchunksize) # chunksize=5000,
		send_result = 0
		torqfile.send_done = True
		logger.debug(f'[T] {torqfile.filename} tosqldone time: {(datetime.now() - t1).total_seconds()} tsendstart: {(datetime.now() - torqfile.send_time_start).total_seconds()} tinit: {(datetime.now() - torqfile.init_time).total_seconds()}  bufflen: {len(torqfile.buffer)} torqfile.sqlchunksize: {torqfile.sqlchunksize}')
	except pymysql.err.OperationalError as e:
		torqfile.send_failed = True
		logger.error(f'[T] {torqfile.filename} failed: OPERR ')
		send_result = -1
	except (DataError, pymysql.err.DataError) as e:
		torqfile.send_failed = True
		logger.error(f'[T] {torqfile.filename} failed: DATAERR {torqfile.name} {type(e)} {e.args[0]}')
		send_result = -2
	except OperationalError as e:
		torqfile.send_failed = True
		torqfile.send_done = True
		logger.error(f'[T] {torqfile.filename} failed: sqlalchemy operr {e.code} {e.args[0]}')
		send_result = -3
	except AttributeError as e:
		torqfile.send_failed = True
		logger.error(f'[T] {torqfile.filename} failed: AttributeError ')
		send_result = -4
	return send_result
