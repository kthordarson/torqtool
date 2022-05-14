import asyncio
from genericpath import exists
from sqlite3 import InternalError
import psycopg2
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
from sqlalchemy.exc import OperationalError, ProgrammingError, InternalError, NoReferencedTableError, ResourceClosedError, PendingRollbackError
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, inspect, select, Numeric, DateTime, text, BIGINT, BigInteger, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import OperationalError, DataError
import sqlalchemy.pool as pool
from hashlib import md5
# from utils import read_csv_columns_raw, column_fixer, FIELDMAPS, badvals, badvals_str, get_csv_files, Torqlog, Torqfile, TripProfile
from utils import get_csv_files, torqbuffer_fixer, read_torq_profile
from datamodels import TorqEntry, TorqFile, TorqProfile
Base = declarative_base()

from loguru import logger

def check_threads(threads):
	return True in [t.is_alive() for t in threads]

def database_init(engine):
	meta = MetaData(engine)
	t1 = datetime.now()
	logger.debug(f'[dbinit] {(datetime.now() - t1).total_seconds()} dropping from {meta}')
	try:
		tables=[TorqEntry.__table__, TorqFile.__table__, TorqProfile.__table__]
		for t in tables:
			logger.debug(f'[d] dropping {t}')
			t.drop(bind=engine, checkfirst=False)
			#meta.drop_all(t, bind=engine, checkfirst=False)
	except (OperationalError, NoReferencedTableError) as e:
		logger.error(e)
	try:
		tables=[TorqEntry.__table__, TorqFile.__table__, TorqProfile.__table__]
		meta.drop_all(bind=engine, tables=tables, checkfirst=False)
	except Exception as e:
		logger.error(e)
	try:
		Base.metadata.create_all(bind=engine, tables=[TorqEntry.__table__, TorqFile.__table__, TorqProfile.__table__], checkfirst=False)
	except Exception as e:
		logger.error(e)
	logger.debug(f'[dbinit] {(datetime.now() - t1).total_seconds()} done')

def chunks(l, n):
	"""Yield n number of sequential chunks from l."""
	d, r = divmod(len(l), n)
	for i in range(n):
		si = (d + 1) * (i if i < r else r) + d * (0 if i < r else i - r)
		yield l[si:si + (d + 1 if i < r else d)]

def read_and_send(csvfile=None, engine=None, session=None, Session=None, csvhash=None, tablename=None, conn=None):
	session = Session()
	t0 = datetime.now()
	tripid = str(csvfile.parts[-2])
	torqbuffer = read_csv(csvfile, delimiter=',', low_memory=False, index_col=False, thousands='.', na_values=0, verbose=False)#, na_values=0, verbose=False, thousands='.', index_col=False, skiprows=1) # , encoding='cp1252'
	tprof = read_torq_profile(csvfile, tripid)
	tentry = TorqEntry(filename=csvfile, tripid=tripid)
	tentry.set_data(torqbuffer)
	torqfile = TorqFile()
	torqfile.name = csvfile
	torqfile.hash = csvhash
	torqfile.tripid = tripid
	session.add(tentry)
	session.add(tprof)
	session.add(torqfile)
	try:
		session.commit()
	except (OperationalError, DataError, ResourceClosedError, PendingRollbackError) as e:
		logger.error(f'[sql] {csvfile} {e.code} {e.args[0]}')
		session.rollback()
	logger.debug(f'[rs] {tentry}  donetime:{datetime.now()-t0}')
	return

async def torqtask(loop=None, executor_processes=None, buffer=None, engine=None,  session=None, Session=None, csvhash=None, csvfile=None, conn=None):
	await loop.run_in_executor(None, functools.partial(read_and_send, engine=engine, session=session,Session=Session, csvhash=csvhash, csvfile=csvfile, conn=conn))
	return

async def main(args):
	t0 = datetime.now()
	TORQDBHOST = 'elitedesk' # os.getenv('TORQDBHOST')
	TORQDBUSER = 'torq' # os.getenv('TORQDBUSER')
	TORQDBPASS = 'dzt3f5jCvMlbUvRG'
	TORQDATABASE = 'torq5'
	# engine = create_engine(f"mysql+pymysql://{TORQDBUSER}:{TORQDBPASS}@{TORQDBHOST}/{TORQDATABASE}?charset=utf8mb4", pool_size=20, max_overflow=0)# , isolation_level='AUTOCOMMIT')
	# engine = create_engine(f"postgresql://postgres:foobar9999@elitedesk/torqdev")
	# conn_ = psycopg2.connect("postgresql://postgres:foobar9999@elitedesk/torqdev")
	param_dic = {
		'host' : 'elitedesk',
		'database' : 'torq5',
		'user' : 'torq',
		# 'password' : 'foobar9999',
		'password' : 'dzt3f5jCvMlbUvRG',
		'pool_size': 200,
		'max_overflow':0
	}

	#connect = "postgresql+psycopg2://%s:%s@%s:5432/%s" % (
	connect = "mysql+pymysql://%s:%s@%s/%s" % (
    param_dic['user'],
    param_dic['password'],
    param_dic['host'],
    param_dic['database'])

	# engine = create_engine(connect)
	engine = create_engine(f"mysql+pymysql://{TORQDBUSER}:{TORQDBPASS}@{TORQDBHOST}/{TORQDATABASE}?charset=utf8mb4", pool_size=200, max_overflow=0)# , isolation_level='AUTOCOMMIT')
	maxworkers = cpu_count()
	tasks = []
	conn = None
	Session = sessionmaker(bind=engine)
	session = Session()
	session.autoflush = True

	if args.init_db:
		logger.debug(f'[mainpath] Calling init_db ... ')
		try:
			# sql = 'DROP TABLE IF EXISTS torqfiles;'
			sql = 'DROP TABLE IF EXISTS  torq5.torqfiles;'
			session.execute(sql)
			sql = 'DROP TABLE IF EXISTS  torq5.torqlogs;'
			session.execute(sql)
			sql = 'DROP TABLE IF EXISTS  torq5.torqtrips;'
			session.execute(sql)
			
		except Exception as e:
			logger.error(f'[database_init] {e}')
		database_init(engine)


	executor_processes = ProcessPoolExecutor(max_workers=maxworkers)
	loop_ = asyncio.get_event_loop()
	hashres = session.execute(select(TorqFile)).fetchall()
	hashlist = [k[0].hash for k in hashres]
# 	hashlist = [k for k in conn.execute('select hash from torqfiles')]
	filelist = []
	csv_file_list = get_csv_files(searchpath=args.path)
	logger.debug(f'read start time: {(datetime.now() -t0).seconds} csv:{len(csv_file_list)} h:{len(hashlist)}')
	for csv in csv_file_list:
		csvhash = md5(open(csv,'rb').read()).hexdigest()
		if csvhash in hashlist:
			logger.warning(f'[{csv}] already in database')
		else:
			filelist.append(csv)
		tt = torqtask(loop=loop_, executor_processes=executor_processes, csvfile=csv, engine=None, session=None, Session=Session, csvhash=csvhash, conn=conn)
		tasks.append(tt)
	# conn.commit()
	if len(tasks) >= 1:
		torqfiles = await asyncio.gather(*tasks)
	else:
		logger.info(f'No tasks')
	logger.debug(f'torqtask done time: {(datetime.now() -t0).seconds} ')

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
