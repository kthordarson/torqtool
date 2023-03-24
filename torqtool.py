#!/usr/bin/python3
import argparse
import asyncio
import functools
import sys
from pathlib import Path
import pymysql
from sqlalchemy.exc import InternalError
from psycopg2.errors import InvalidTextRepresentation
from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor, as_completed)
from threading import Thread
from datetime import datetime, timedelta
from timeit import default_timer as timer
from hashlib import md5
from multiprocessing import cpu_count

from loguru import logger
from pandas.errors import EmptyDataError
from pandas import (DataFrame, Index, Series, concat, to_datetime, read_sql)
from pandas import read_csv as read_csv_pandas
import polars as pl
from polars import read_csv as read_csv_polars
from sqlalchemy import create_engine, text
from sqlalchemy.exc import (ArgumentError, CompileError, DataError, IntegrityError, OperationalError, ProgrammingError)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker)
from sqlalchemy.orm.exc import DetachedInstanceError
from datamodels import get_trip_profile, send_torqfiles, send_torq_trip, database_init, sqlite_db_init, send_trip_profile, send_torqtrips,database_dropall
from datamodels import Torqdata, TorqFile, Torqtrips
from updatetripdata import  send_torqdata
from utils import checkcsv, get_csv_files, convert_datetime
from datamap import entry_datamap
BADVALS = ['-', 'NaN', '0', 'â', r'0']

CPU_COUNT = cpu_count()

def read_buff(tf_csvfile, tf_fileid, tf_tripid):
	start = timer()
	csvfilefixed = tf_csvfile
	datefields = ['gpstime', 'devicetime']
	try:
		#torqbuffer = read_csv_pandas(csvfilefixed, delimiter=',', na_values=BADVALS, low_memory=False, parse_dates=datefields, converters={'gpstime': convert_datetime}, dtype=entry_datamap, on_bad_lines='skip')
		torqbuffer = read_csv_polars(csvfilefixed, ignore_errors=True)
	except ValueError as e:
		logger.error(f'[read_buff] {e} tf_csvfile={tf_csvfile} csvfilefixed={csvfilefixed}')
		return None
	# torqbuffer.fillna(0, inplace=True)
	# insert fileid and tripid
	#torqbuffer.insert(1, "fileid", [tf_fileid for k in range(len(torqbuffer))])
	#torqbuffer.insert(2, "tripid", [tf_tripid for k in range(len(torqbuffer))])
	fileid_series = pl.Series("fileid", [tf_fileid for k in range(len(torqbuffer))])
	tripid_series = pl.Series("tripid", [tf_tripid for k in range(len(torqbuffer))])
	torqbuffer.insert_at_idx(1, fileid_series)
	torqbuffer.insert_at_idx(2, tripid_series)

	end = timer()
	resultbuffer = {
		'torqbuffer' : torqbuffer,
		'fileid' : tf_fileid,
		'tripid' : tf_tripid,
		'tf_csvfile' : tf_csvfile,
	}
	return resultbuffer

#def data_sender(buffer, session,)

def sqlsender(buffer=None, dburl=None):
	engine = create_engine(dburl, echo=False)
	Session = sessionmaker(bind=engine)
	session = Session()

	results = {
		'fileid': buffer['fileid'],
		'tripid': buffer['tripid'],
		'tf_csvfile': buffer['tf_csvfile'],
		'status': 'unknown'
	}
	try:
		buffer['torqbuffer'].to_pandas().to_sql('torqlogs', con=engine, if_exists='append', index=False)
		results['status'] = 'success'
	except (OperationalError, ProgrammingError) as e:
		# todo handle db locks
		# [tosql] code=e3q8 args=(sqlite3.OperationalError) database is locked r={'fileid': 156, 'tripid': 156, 'status': 'unknown'}
		logger.error(f'[tosql] code={e.code} args={e.args[0]} r={results} tf_csvfile={buffer["tf_csvfile"]}')  # error:{e}
		results['status'] = 'error'
	except InternalError as e:
		logger.error(f'[tosql] InternalError {e} r={results} tf_csvfile={buffer["tf_csvfile"]}')
		results['status'] = 'error'
	except (InvalidTextRepresentation,IntegrityError) as e:
		logger.warning(f'[tosql] {type(e)} code={e.code} args={e.args[0]} r={results} tf_csvfile={buffer["tf_csvfile"]}')
		results['status'] = 'error'
		# logger.warning(f'[tosql] {e.statement} {e.params}')
		# logger.warning(f'[tosql] {e}')
	except (pymysql.err.DataError, DataError) as e:
		# r={'fileid': 156, 'tripid': 156, 'status': 'unknown'}
		tf_err = session.query(TorqFile).filter(TorqFile.id == results['fileid']).first()
		errmsg = e.args[0]
		err_row = errmsg.split('row')[-1].strip()
		err_row = errmsg.split(',')[1].split('at row')[1].strip().strip('")')
		err_col = errmsg.split(',')[1].split('at row')[0].split("'")[1]
		logger.warning(f'\n[tosql] code={e.code}\nargs={e.args[0]}\nr={results}\nerr_row: {err_row}\nerr_col:{err_col}\ntorqfile={tf_err} tf_csvfile={buffer["tf_csvfile"]}\n')  # error:{e}
		#logger.warning(f'[tosql] dataerr code:{e.code} err:{errmsg} err_row: {err_row} err_col:{err_col} r={results}')  # row:{err_row} {buffer.iloc[err_row]}')
		# buffer = buffer.drop(columns=[err_col])
		buffer['torqbuffer'] = buffer['torqbuffer'].drop(columns=[err_col])
		buffer['torqbuffer'].to_pandas().to_pandas().to_sql('torqlogs', con=engine, if_exists='append', index=False)
		results['status'] = 'warning'
	except TypeError as e:
		errmsg = e.args[0]
		err_row = errmsg.split('row')[-1].strip()
		logger.error(f'[tosql] code:{e.code} err:{errmsg} row:{err_row} {buffer.iloc[err_row]} r={results} tf_csvfile={buffer["tf_csvfile"]}')
		results['status'] = 'error'
	return results



def torq_worker(tf, dburl):
	buffer = None
	results = None
	datares = None
	try:
		buffer = read_buff(tf.csvfilefixed, tf.id, tf.tripid)
	except ValueError as e:
		logger.error(e)
		return None
	try:
		results = sqlsender(buffer, dburl)
	except ValueError as e:
		logger.error(e)
	try:
		datares = send_torqdata(tf.id, dburl)
	except ValueError as e:
		logger.error(f'{type(e)} {e} in send_torqdata')

	res = {
		'tf': tf,
		'bufferlen': len(buffer),
		'results': results,
		'datares': datares if datares else None
	}
	return res

def main(args):
	# 1. scan args.path for csv files
	# 2. check if csv files are in db
	# 3. if not in db, foreach run fixer, create TorqFile and send to db
	# 4.
	# 5. read profile.properties from csvfile folder, foreach, create Torqtrips and send to db
	# 6. foreach new TorqFile, read fixed csv, create TorqLogs and send to db
	# 7.
	# 8. send csvdata to db
	# todo: create worker thread for each file, worker reads and processes file and sends to db.
	t0 = datetime.now()
	dburl = None
	if args.dbmode == 'mysql':
		dburl = f"mysql+pymysql://{args.dbuser}:{args.dbpass}@{args.dbhost}/{args.dbname}?charset=utf8mb4"
		engine = create_engine(dburl)
		Session = sessionmaker(bind=engine)
		session = Session()
	elif args.dbmode == 'postgresql':
		dburl = f"postgresql://{args.dbuser}:{args.dbpass}@{args.dbhost}/{args.dbname}"
		engine = create_engine(dburl)
		Session = sessionmaker(bind=engine)
		session = Session()
	elif args.dbmode == 'sqlite':
		dburl = f'sqlite:///torqfiskurdb'
		engine = create_engine(dburl, echo=False, connect_args={'check_same_thread': False})
		Session = sessionmaker(bind=engine)
		session = Session()
		# session.execute(text('PRAGMA foreign_keys=OFF'))
		sqlite_db_init(engine)
	database_init(engine)
	if args.database_dropall:
		try:
			database_dropall(engine)
		except OperationalError as e:
			logger.error(f'[database_init] {e}')

	filelist = get_csv_files(searchpath=Path(args.path), dbmode=args.dbmode)
	newfilelist = []
	newfilelist = send_torqfiles(filelist, session)
	if newfilelist is None:
		logger.warning(f'[main]	send_torqfiles returned None')
		sys.exit(1)
	elif len(newfilelist) == 0:
		logger.warning(f'[main]	0 files from send_torqfiles....')
		sys.exit(1)
	else:
		# get files from db that are not read
		t0 = datetime.now()
		tripstart = timer()
		dbtorqfiles = session.query(TorqFile).filter(TorqFile.read_flag == 0).all()
		for torqfile in dbtorqfiles:
			send_torqtrips(torqfile, session)
		tripend = timer()
		logger.debug(f'[send_torqtrips] done t0={datetime.now()-t0} time={timedelta(seconds=tripend - tripstart)} starting read_process for {len(dbtorqfiles)} files mode={args.threadmode}')
		tasks = []
		if args.threadmode == 'ppe':
			with ProcessPoolExecutor(max_workers=CPU_COUNT) as executor:
				for idx, tf in enumerate(dbtorqfiles):
					t = session.query(TorqFile).filter(TorqFile.id == tf.id).first()
					tasks.append(executor.submit(torq_worker,t, dburl))
				logger.debug(f't={datetime.now() - t0} tasks={len(tasks)} mode={args.threadmode}')
		elif args.threadmode == 'tpe':
			with ThreadPoolExecutor(max_workers=CPU_COUNT) as executor:
				for idx, tf in enumerate(dbtorqfiles):
					t = session.query(TorqFile).filter(TorqFile.id == tf.id).first()
					tasks.append(executor.submit(torq_worker,t, dburl))
				logger.debug(f't={datetime.now() - t0} tasks={len(tasks)} mode={args.threadmode}')
		main_results = []
		for res in as_completed(tasks):
			try:
				r = res.result()
			except ProgrammingError as e:
				logger.error(f'[!] ProgrammingError {e} res:{res}')
			except EmptyDataError as e:
				logger.error(f'[!] EmptyDataError {e} res:{res}')
			except TypeError as e:
				logger.error(f'[!] TypeError {e} res:{res}')
			except ValueError as e:
				logger.error(f'[!] TypeError {e} res:{res}')
			else:
				main_results.append(r)
		logger.debug(f'[*] done t={datetime.now() - t0} mr={len(main_results)} threadmode={args.threadmode}')


if __name__ == '__main__':
	parser = argparse.ArgumentParser(description="torqtool")
	parser.add_argument("--path", nargs="?", default=".", help="path to csv files", action="store")
	parser.add_argument("--file", nargs="?", default=".", help="path to single csv file", action="store")
	parser.add_argument("--gui", default=False, help="Run gui", action="store_true", dest='gui')
	parser.add_argument("--init-db", default=False, help="init database", action="store_true", dest='init_db')
	parser.add_argument("--database_dropall", default=False, help="drop database", action="store_true", dest='database_dropall')
	parser.add_argument("--check-db", default=False, help="check database", action="store_true", dest='check_db')
	parser.add_argument("--fixcsv", default=False, help="repair csv", action="store_true", dest='fixcsv')
	parser.add_argument("--checkcsv", default=False, help="scan csv path", action="store_true", dest='checkcsv')
	parser.add_argument("--combinecsv", default=False, help="make big csv", action="store_true", dest='combinecsv')
	parser.add_argument("--dump-db", nargs="?", default=None, help="dump database to file", action="store")
	parser.add_argument("--check-file", default=False, help="check database", action="store_true", dest='check_file')
	parser.add_argument("--webstart", default=False, help="start web listener", action="store_true", dest='web')
	parser.add_argument("--sqlchunksize", nargs="?", default="1000", help="sql chunk", action="store")
	parser.add_argument("--max_workers", nargs="?", default="4", help="max_workers", action="store")
	parser.add_argument("--chunks", nargs="?", default="4", help="chunks", action="store")
	parser.add_argument("--dbmode", default="", help="sqlmode mysql/postgresql/sqlite", action="store")
	parser.add_argument("--dbname", default="", help="dbname", action="store")
	parser.add_argument("--dbhost", default="", help="dbname", action="store")
	parser.add_argument("--dbuser", default="", help="dbname", action="store")
	parser.add_argument("--dbpass", default="", help="dbname", action="store")
	parser.add_argument('--threadmode', default='ppe', help='threadmode ppe/tpe', action='store')
	args = parser.parse_args()
	main(args)


# gps_startingspoints = [s.query(Torqlogs.latitude).filter(Torqlogs.tripid == k.id).first() for k in trips]
# gps_endingpoints = [s.query(Torqlogs.latitude).filter(Torqlogs.tripid == k.id).order_by(Torqlogs.id.desc()).first() for k in trips]
