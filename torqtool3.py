from aiomultiprocess import Pool
import asyncio

from multiprocessing import cpu_count
from lib2to3.pgen2.token import OP
import os
from concurrent.futures import ProcessPoolExecutor
from sqlalchemy.orm import sessionmaker
import signal
import time
from functools import wraps
import argparse
import signal
import functools
import time
import asyncio
from pathlib import Path
from pandas import read_csv, DataFrame, to_datetime, Series
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from dateutil.parser import ParserError

from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, inspect, select, Numeric, DateTime, text, BIGINT, BigInteger, Float
from hashlib import md5
from sqlalchemy.ext.declarative import declarative_base
import pymysql
from sqlalchemy.exc import OperationalError, DataError
from utils import read_csv_columns_raw, column_fixer, FIELDMAPS, badvals, badvals_str, get_csv_files, Torqlog
from threading import Thread, active_count
Base = declarative_base()

from loguru import logger

def chunks(l, n):
	"""Yield n number of sequential chunks from l."""
	d, r = divmod(len(l), n)
	for i in range(n):
		si = (d + 1) * (i if i < r else r) + d * (0 if i < r else i - r)
		yield l[si:si + (d + 1 if i < r else d)]

class DataSender(Thread):
	def __init__(self, torqfiles=None, engine=None, senderid=None):
		Thread.__init__(self)
		self.name = f'sender-{senderid}'
		self.torqfiles = torqfiles
		self.engine = engine
		self.Session = sessionmaker(bind=self.engine)
		self.session = self.Session()
		self.session.expire_on_commit = False
		self.conn = self.engine.connect()
		self.sqlchunksize = 10000
		self.send_done = False
		self.send_count = 0
		self.remaining_files = len(self.torqfiles)
		self.kill = False

	def __repr__(self):
		return self.name

	def __str__(self):
		return self.name

	def get_status(self):
		logger.debug(f'[s] t:{len(self.torqfiles)} sc:{self.send_count} r:{self.remaining_files}')

	def get_remaining(self):
		return self.remaining_files
	
	def run(self):			
		for torqfile in self.torqfiles:
			if self.kill:
				logger.info(f'[s] thread killed')
				return
			torqfile.buffer['hash'] = torqfile.hash
			torqfile.buffer['tripid'] = torqfile.tripid
			torqfile.buffer.to_sql(con=self.engine, name='torqlogs', if_exists='append', index=False, method='multi', chunksize=self.sqlchunksize)
			try:
				torqfile.trip_profile.to_sql(con=self.engine, name='torqtrips', if_exists='append', index=False, method='multi', chunksize=self.sqlchunksize)
			except OperationalError as e:
				logger.error(f'{self} err {e}')
			# self.session.add(torqfile.trip_profile)
			# self.session.commit()
			self.send_count += 1
			self.remaining_files -= 1
			logger.debug(f'[{self.name}] sent {torqfile} sc:{self.send_count} st:{len(self.torqfiles)} r:{self.remaining_files}')
		self.send_done = True
		return

class TripProfile(Base):
	__tablename__ = 'torqtrips'
	tripid =  Column(BigInteger, primary_key=True)
	filename = Column(String(255))
	fuelCost = Column(Integer)
	fuelUsed = Column(Integer)
	time = Column(Integer)
	distanceWhilstConnectedToOBD = Column(Integer)
	distance = Column(Integer)
	profile = Column(String(255))
	tripdate = Column(DateTime, server_default=text('NOW()'))

	def __init__(self, filename=None):
		self.filename = filename

class Torqfile(Base):
	__tablename__ = 'torqfiles'
	fileid =  Column(Integer, primary_key=True)
	name = Column(String(255))
	hash = Column(String(255))
	tripid =  Column(String(255))
	torqprofile = Column(String(255))

	def __str__(self):
		return(f'[torqfile] {self.name}')

	def __repr__(self):
		return(f'{self.name}')

	def __init__(self, filename:Path):
		self.name = str(filename)
		self.filename = filename
		self.fixed = False
		self.read_done = False
		self.buffer = DataFrame()
		self.trip_profile = DataFrame()
		self.filesize = filename.stat().st_size
		self.tripid = str(filename.parts[-2])
		self.columns = []
		self.hash = self.gen_md5hash()
		self.exists_in_db = False
		self.send_done = False
		self.send_failed = False
		self.init_time = datetime.now()
		self.send_time_start = datetime.now()
		self.sqlchunksize = 10000

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

	def get_profile(self):
		return self.trip_profile

	def read_profile(self):
		p_filename = os.path.join(self.filename.parent, 'profile.properties')
		with open(p_filename, 'r') as f:
			pdata_ = f.readlines()
		if len(pdata_) == 8:
			pdata = [l.strip('\n') for l in pdata_ if not l.startswith('#')]
			tripdate = to_datetime(pdata_[1][1:])
			trip_profile = dict([k.split('=') for k in pdata])
			trip_profile['filename'] = p_filename
			trip_profile['tripid'] = int(self.tripid)
			trip_profile['tripdate'] = tripdate
			self.trip_profile = DataFrame([trip_profile])
			# self.trip_profile = DataFrame(Series(trip_profile))
			# logger.debug(f'[p] {len(self.trip_profile)} ')

	def buffread(self):
		self.buffer = read_csv(self.filename, delimiter=',', low_memory=False, encoding='cp1252', na_values=0)		
		self.buffer_fixer()
		self.read_done = True
		self.read_profile()
		return self
	
	def buffer_fixer(self):
		t0 = datetime.now()
		if self.fixed:
			logger.warning(f'[bf] {self.filename} already fixed? self.self.buffer: {len(self.self.buffer)} buff:{len(self.buffer)} sf:{self.fixed} rd:{self.read_done}')
			return None
		if len(self.buffer) == 0:
			self.buffread()
			if len(self.buffer) == 0:
				# logger.error(f'{self} buff {len(self.buffer)}')
				return
		# logger.info(f'[bf] {self.filename} self.buffer_fixer start')
		self.buffer.replace('-','0', inplace=True)
		self.buffer.replace('âˆž','0', inplace=True)
		self.buffer.replace('Ã¢ÂˆÂž','0', inplace=True)
		cols = [column_fixer(k) for k in self.buffer.columns]
		fields = [FIELDMAPS[k] for k in cols]
		# fields = [FIELDMAPS.get(k, '') for k in cols]
		self.buffer.columns = fields
		# logger.debug(f'[fix] {self.filename} t0 {t0}')
		try:
			if self.buffer.get('GPSTime') is not None:
				self.buffer['GPSTime'].replace('-', self.buffer['GPSTime'][0], inplace=True)
				self.buffer['GPSTime'].replace('0', self.buffer['GPSTime'][0], inplace=True)
				self.buffer['GPSTime'] = to_datetime(self.buffer['GPSTime'], errors='raise', infer_datetime_format=True)
			if self.buffer.get('GPS Time') is not None:
				self.buffer['GPS Time'] = to_datetime(self.buffer['GPS Time'], errors='raise', infer_datetime_format=True)
		except (ParserError, KeyError) as e:
			logger.error(f'[bferr] gpstime {self.filename} {e}')
		try:
			if self.buffer.get('DeviceTime') is not None:
				self.buffer['DeviceTime'] = to_datetime(self.buffer['DeviceTime'], errors='raise', infer_datetime_format=True)
			if self.buffer.get('Device Time') is not None:
				self.buffer['Device Time'] = to_datetime(self.buffer['Device Time'], errors='raise', infer_datetime_format=True)
		except (ParserError, KeyError) as e:
			logger.error(f'[bferr] devicetime {self.filename} {e}')
		for f in fields:
			for badv in badvals_str:
				self.buffer[f].replace(badv, 0, inplace=True)
			for badv in badvals:
				self.buffer[f].replace(badv, 0, inplace=True)
		self.fixed = True		
		# logger.debug(f'fix done {self} {datetime.now() - t0}')
		return len(self.buffer)

async def readtask(loop, executor_processes, csv):
	torqfile = await loop.run_in_executor(executor_processes, Torqfile(filename=csv).buffread)
	return torqfile

async def readtasknew(filename):	
	torqfile = Torqfile(filename=filename).buffread()
	return torqfile

def check_threads(threads):
	return True in [t.is_alive() for t in threads]

def database_init(engine):
	meta = MetaData(engine)
	t1 = datetime.now()
	logger.debug(f'[dbinit] {(datetime.now() - t1).total_seconds()} dropping from {meta}')
	try:
		meta.drop_all(bind=engine, tables=[Torqfile.__table__, Torqlog.__table__, TripProfile.__table__], checkfirst=False)
	except OperationalError as e:
		logger.error(e)
	meta.create_all(bind=engine, tables=[Torqfile.__table__, Torqlog.__table__, TripProfile.__table__])
	logger.debug(f'[dbinit] {(datetime.now() - t1).total_seconds()} done')


async def main(args):
	t0 = datetime.now()
	TORQDBHOST = 'elitedesk' # os.getenv('TORQDBHOST')
	TORQDBUSER = 'torq' # os.getenv('TORQDBUSER')
	TORQDBPASS = 'dzt3f5jCvMlbUvRG'
	# TORQDBPASS = os.getenv('TORQDBPASS')
	TORQDATABASE = 'torqdev3'
	engine = create_engine(f"mysql+pymysql://{TORQDBUSER}:{TORQDBPASS}@{TORQDBHOST}/{TORQDATABASE}?charset=utf8mb4", pool_size=20, max_overflow=0)# , isolation_level='AUTOCOMMIT')
	# engine = create_engine("mysql://" + loadConfigVar("user") + ":" + loadConfigVar("password") + "@" + loadConfigVar("host") + "/" + loadConfigVar("schema"),pool_size=20, max_overflow=0)
	if args.init_db:
		logger.debug(f'[mainpath] Calling init_db ... ')
		database_init(engine)
	maxworkers = cpu_count()
	conn = engine.connect()
	hashlist = [k[0] for k in conn.execute('select hash from torqfiles')]
	filelist = []
	csv_file_list = get_csv_files(searchpath=args.path)
	logger.debug(f'read start time: {(datetime.now() -t0).seconds} csv:{len(csv_file_list)}')
	for csv in csv_file_list:
		csvhash = md5(open(csv,'rb').read()).hexdigest()
		if csvhash in hashlist:
			logger.warning(f'[{csv}] already in database')
		else:
			filelist.append(csv)
			tripid = str(csv.parts[-2])
			sqlhash = f'INSERT INTO torqfiles (name, hash, tripid) VALUES ("{csv}", "{csvhash}", "{tripid}")'
			conn.execute(sqlhash)
	tf = []
	logger.debug(f't: {(datetime.now() -t0).seconds} ')
	async with Pool() as pool:
		async for t in pool.map(readtasknew, filelist):
			tf.append(t)
	logger.debug(f'read done time: {(datetime.now() -t0).seconds} tf:{len(tf)}')
	senderthreads = []
	chunkedlist = [k for k in chunks(tf, maxworkers)]
	for thread in range(len(chunkedlist)):
		s = DataSender(torqfiles=chunkedlist[thread], engine=engine, senderid=thread)
		senderthreads.append(s)
		s.start()
	logger.debug(f'started senders:{len(senderthreads)} time: {(datetime.now() -t0).seconds}')

	while check_threads(senderthreads):
		pass
	
if __name__ == '__main__':
	parser = argparse.ArgumentParser(description="torqtool")
	parser.add_argument("--path", nargs="?", default=".", help="path to csv files", action="store")
	parser.add_argument("--file", nargs="?", default=".", help="path to single csv file", action="store")
	parser.add_argument("--gui", default=False, help="Run gui", action="store_true", dest='gui')
	parser.add_argument("--check-db", default=False, help="check database", action="store_true", dest='check_db')
	parser.add_argument("--init-db", default=False, help="init database", action="store_true", dest='init_db')
	parser.add_argument("--fixcsv", default=False, help="repair csv", action="store_true", dest='fixcsv')
	parser.add_argument("--dump-db", nargs="?", default=None, help="dump database to file", action="store")
	parser.add_argument("--check-file", default=False, help="check database", action="store_true", dest='check_file')
	parser.add_argument("--webstart", default=False, help="start web listener", action="store_true", dest='web')
	parser.add_argument("--sqlchunksize", nargs="?", default="1000", help="sql chunk", action="store")
	parser.add_argument("--max_workers", nargs="?", default="4", help="max_workers", action="store")
	parser.add_argument("--chunks", nargs="?", default="4", help="chunks", action="store")
	args = parser.parse_args()
	t0 = datetime.now()
	asyncio.run(main(args))
	logger.info(f'[main] done time: {datetime.now() - t0}')
