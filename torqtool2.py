import asyncio
from concurrent.futures import ProcessPoolExecutor
import signal
import time
from functools import wraps
import argparse
import signal
import functools
import time
import asyncio
from pathlib import Path
from pandas import read_csv, DataFrame, to_datetime
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from dateutil.parser import ParserError

from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, inspect, select, BigInteger, Numeric, DateTime, text, BIGINT, BigInteger, Float
from hashlib import md5
from sqlalchemy.ext.declarative import declarative_base
import pymysql
from sqlalchemy.exc import OperationalError, DataError
from utils import read_csv_columns_raw, column_fixer, FIELDMAPS, badvals, badvals_str, get_csv_files, init_db
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
		self.sqlchunksize = 10000
		self.send_done = False
		self.send_count = 0
		self.remaining_files = len(self.torqfiles)
		self.kill = False

	def __repr__(self):
		return self.name

	def __str__(self):
		return self.name

	def get_remaining(self):
		return self.remaining_files
	
	def run(self):	
		
		for torqfile in self.torqfiles:
			torqfile.buffer.to_sql(con=self.engine, name='torqlogs', if_exists='append', index=False, method='multi', chunksize=self.sqlchunksize)
			self.send_count += 1
			self.remaining_files -= 1
			logger.debug(f'[{self.name}] sent {torqfile} {self.send_count} / {len(self.torqfiles)} / {self.remaining_files}')
		self.send_done = True
		return

class Torqfile():
	def __str__(self):
		return(f'T: {self.name} b:{len(self.buffer)} f:{self.fixed} rd:{self.read_done} sd:{self.send_done}')
	def __repr__(self):
		return(f'{self.name}')
	def __init__(self, filename:Path):
		self.name = str(filename)
		self.filename = filename
		self.fixed = False
		self.read_done = False
		self.buffer = DataFrame()
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

	
	def buffread(self):
		self.buffer = read_csv(self.filename, delimiter=',', low_memory=False, encoding='cp1252', na_values=0)		
		self.buffer_fixer()
		self.read_done = True
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
		logger.debug(f'fix done {self} {datetime.now() - t0}')
		return len(self.buffer)

async def readtask(loop, executor_processes, csv):
	torqfile = await loop.run_in_executor(executor_processes, Torqfile(filename=csv).buffread)
	return torqfile

def check_threads(threads):
	return True in [t.is_alive() for t in threads]

async def main(args):
	TORQDBHOST = 'elitedesk' # os.getenv('TORQDBHOST')
	TORQDBUSER = 'torq' # os.getenv('TORQDBUSER')
	TORQDBPASS = 'dzt3f5jCvMlbUvRG'
	# TORQDBPASS = os.getenv('TORQDBPASS')
	TORQDATABASE = 'torqdev'
	engine = create_engine(f"mysql+pymysql://{TORQDBUSER}:{TORQDBPASS}@{TORQDBHOST}/{TORQDATABASE}?charset=utf8mb4")# , isolation_level='AUTOCOMMIT')
	if args.init_db:
		logger.debug(f'[mainpath] Calling init_db ... ')
		init_db(engine)
 	
	executor_processes = ProcessPoolExecutor(max_workers=8)
	loop_ = asyncio.get_event_loop()
	tasks = []
	csv_file_list = get_csv_files(searchpath=args.path)
	for csv in csv_file_list:
		tasks.append(readtask(loop_, executor_processes, csv))
	torqfiles = await asyncio.gather(*tasks)
	logger.debug(f'tf: {len(torqfiles)}')
	senderthreads = []
	chunkedlist = [k for k in chunks(torqfiles, 4)]
	for thread in range(len(chunkedlist)):
		senderthread = DataSender(torqfiles=chunkedlist[thread], engine=engine, senderid=thread)
		senderthreads.append(senderthread)
		senderthread.start()
		logger.debug(f'[{senderthread}] started threadfiles:{len(senderthread.torqfiles)}')

	#for torqfile in torqfiles:
	#	senderthreads.append(DataSender(torqfile=torqfile, engine=engine))
	# for sender in senderthreads:
	# 	sender.daemon = True
	# 	sender.start()
	# 	logger.debug(f'sender {sender} started')
	while check_threads(senderthreads):
		pass
	logger.debug('done')
	
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
	asyncio.run(main(args))
