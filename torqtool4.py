
from pangres import aupsert
from pangres.exceptions import DuplicateLabelsException
import asyncio

from multiprocessing import cpu_count

import os
from concurrent.futures import ProcessPoolExecutor
from functools import wraps
import argparse
import functools
import asyncio
from pathlib import Path
from pandas import read_csv, DataFrame, to_datetime, Series

from datetime import datetime
from dateutil.parser import ParserError

from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, inspect, select, Numeric, DateTime, text, BIGINT, BigInteger, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import OperationalError, DataError

from hashlib import md5
from utils import read_csv_columns_raw, column_fixer, FIELDMAPS, badvals, badvals_str, get_csv_files, Torqlog, Torqfile, TripProfile
from threading import Thread, active_count

Base = declarative_base()

from loguru import logger

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
	Base.metadata.create_all(bind=engine, tables=[Torqfile.__table__, Torqlog.__table__, TripProfile.__table__])
	logger.debug(f'[dbinit] {(datetime.now() - t1).total_seconds()} done')

async def async_database_init(engine):
	meta = MetaData(engine)
	t1 = datetime.now()
	async with engine.begin() as conn:
		await conn.run_sync(Base.metadata.drop_all)
	async with engine.begin() as conn:
		await conn.run_sync(Base.metadata.create_all)
	logger.debug(f'[dbinit] {(datetime.now() - t1).total_seconds()} done')

def chunks(l, n):
	"""Yield n number of sequential chunks from l."""
	d, r = divmod(len(l), n)
	for i in range(n):
		si = (d + 1) * (i if i < r else r) + d * (0 if i < r else i - r)
		yield l[si:si + (d + 1 if i < r else d)]

def read_and_send(csvfile=None, engine=None, session=None, csvhash=None, tablename=None):
	# localloop = asyncio.get_event_loop()
	t0 = datetime.now()
	tripid = str(csvfile.parts[-2])
	sqlhash = f'INSERT INTO torqfiles (name, hash, tripid) VALUES ("{csvfile}", "{csvhash}", "{tripid}")'
	engine.execute(sqlhash)
	tfile = Torqfile(filename=csvfile)
	tfile.buffer = read_csv(tfile.filename, delimiter=',', low_memory=False, encoding='cp1252', na_values=0)
	# logger.debug(f'[rs] {self.filename.name} b:{len(self.buffer)} time:{t0}')
	tfile.buffer_fixer()
	tfile.read_done = True
	tfile.buffer['tripid'] = tripid
	tfile.read_profile()
	tfile.buffer['hash'] = csvhash
	tfile.hash = csvhash
	
	tfile.trip_profile['tripid'] = tripid
	try:
		tfile.buffer.to_sql(con=engine, name='torqlogs', if_exists='append', index=False, method='multi', chunksize=tfile.sqlchunksize)
	except OperationalError as e:
		logger.error(f'[err] in buffer {csvfile} {e.code} {e.args[0]}')
	try:
		tfile.trip_profile.to_sql(con=session, name='torqtrips', if_exists='append', index=False, method='multi', chunksize=tfile.sqlchunksize)
	except OperationalError as e:
		logger.error(f'[err] in profile {csvfile} {e.code} {e.args[0]}')
	logger.debug(f'[rs] {tfile} b:{len(tfile.buffer)} donetime:{datetime.now()-t0}')
	return

async def torqtask(loop=None, executor_processes=None, buffer=None, engine=None,  session=None, csvhash=None, csvfile=None):
	await loop.run_in_executor(None, functools.partial(read_and_send, engine=engine, session=session,csvhash=csvhash, csvfile=csvfile))
	return


async def main(args):
	t0 = datetime.now()
	TORQDBHOST = 'elitedesk' # os.getenv('TORQDBHOST')
	TORQDBUSER = 'torq' # os.getenv('TORQDBUSER')
	TORQDBPASS = 'dzt3f5jCvMlbUvRG'
	TORQDATABASE = 'torqdev5'
	engine = create_engine(f"mysql+pymysql://{TORQDBUSER}:{TORQDBPASS}@{TORQDBHOST}/{TORQDATABASE}?charset=utf8mb4", pool_size=20, max_overflow=0)# , isolation_level='AUTOCOMMIT')
	# engine = create_async_engine(f"mysql+asyncmy://{TORQDBUSER}:{TORQDBPASS}@{TORQDBHOST}/{TORQDATABASE}?charset=utf8mb4", pool_size=20, max_overflow=0)# , isolation_level='AUTOCOMMIT')
	if args.init_db:
		logger.debug(f'[mainpath] Calling init_db ... ')
		database_init(engine)
	maxworkers = cpu_count()
	tasks = []
	conn = engine.connect()
	# asyncsession = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
	# asyncsession = AsyncSession(engine, expire_on_commit=False)
	executor_processes = ProcessPoolExecutor(max_workers=maxworkers)
	loop_ = asyncio.get_event_loop()
	# async_connection = asyncsession.connection()
	hashlist = [k for k in conn.execute('select hash from torqfiles')]
	filelist = []
	torqfiles = None
	csv_file_list = get_csv_files(searchpath=args.path)
	logger.debug(f'read start time: {(datetime.now() -t0).seconds} csv:{len(csv_file_list)} h:{len(hashlist)}')
	for csv in csv_file_list:
		csvhash = md5(open(csv,'rb').read()).hexdigest()
		if csvhash in hashlist:
			logger.warning(f'[{csv}] already in database')
		else:
			filelist.append(csv)
			tt = torqtask(loop=loop_, executor_processes=executor_processes, csvfile=csv, engine=engine, session=conn, csvhash=csvhash)
			tasks.append(tt)
	#conn.commit()
	# logger.debug(f'torqtask start time: {(datetime.now() -t0).seconds} tasks:{len(tasks)}')
	torqfiles = await asyncio.gather(*tasks)
	logger.debug(f'torqtask done time: {(datetime.now() -t0).seconds} tf:{type(torqfiles)}')
	# senderthreads = []
	# chunkedlist = [k for k in chunks(torqfiles, maxworkers)]
	# tasks = []
	# for t in torqfiles:
	# 	tasks.append(sendtask(loop_, executor_processes, t.buffer, engine, 'torqlogs'))
	# 	tasks.append(sendtask(loop_, executor_processes, t.trip_profile, engine, 'torqtrips'))
	# logger.debug(f'start sendtask time: {(datetime.now() -t0).seconds} tasks:{len(tasks)}')
	# sendres = await asyncio.gather(*tasks)
	# logger.debug(f'sendtask done time: {(datetime.now() -t0).seconds} ')
	#logger.debug(f'started senders:{len(senderthreads)} time: {(datetime.now() -t0).seconds}')

	#while check_threads(senderthreads):
	#	pass
	# logger.info(f'[sendmain] done time: {datetime.now() - t0}')

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
