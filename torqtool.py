#!/usr/bin/python3
import argparse
from pickle import PicklingError
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
from dateutil import parser as dateparser
from timeit import default_timer as timer
from hashlib import md5
from multiprocessing import cpu_count

from loguru import logger
from pandas.errors import EmptyDataError
from pandas import (DataFrame, Index, Series, concat, to_datetime, read_sql)
from pandas import read_csv as read_csv_pandas
import polars as pl
from polars import read_csv as read_csv_polars
from polars import ComputeError
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

def replace(column: str, mapping: dict):
	if not mapping:
		raise Exception("Mapping can't be empty")
	elif not isinstance(mapping, dict):
		TypeError(f"mapping must be of type dict, but is type: {type(mapping)}")
	if not isinstance(column, str):
		raise TypeError(f"column must be of type str, but is type: {type(column)}")
	branch = pl.when(pl.col(column) == list(mapping.keys())[0]).then(list(mapping.values())[0])
	for from_value, to_value in mapping.items():
		try:
			branch = branch.when(pl.col(column) == from_value).then(to_value)
		except ComputeError as e:
			logger.error(e)
	return branch.otherwise(pl.col(column)).alias(column)


def data_replacer(column, from_, to_):
	branch =  pl.when(pl.col(column) == from_[0]).then(to_[0])
	# for every value add a `when.then`
	for (from_value, to_value) in zip(from_, to_):
		branch = branch.when(pl.col(column) == from_value).then(to_value)
	return branch.otherwise(pl.col(column)).alias(column)
	# finish with an `otherwise`
def read_buff(tf_csvfile, tf_fileid, tf_tripid):
	start = timer()
	datefields = ['gpstime', 'devicetime']
	if not 'fixed' in tf_csvfile:
		logger.warning(f'[read_buff] {tf_csvfile} is not fixed')
	try:
		#torqbuffer = read_csv_pandas(csvfilefixed, delimiter=',', na_values=BADVALS, low_memory=False, parse_dates=datefields, converters={'gpstime': convert_datetime}, dtype=entry_datamap, on_bad_lines='skip')
		torqbuffer = read_csv_polars(tf_csvfile, ignore_errors=True, try_parse_dates=True, use_pyarrow=True, null_values=['NaN','-','0\x88\x9e'])#, dtypes=entry_datamap)
	except ValueError as e:
		logger.error(f'[read_buff] {type(e)} {e} csvfile={tf_csvfile}')
		return None
	except ComputeError as e:
		logger.error(f'[read_buff] {type(e)} {e} csvfile={tf_csvfile}')
		return None
	# torqbuffer.fillna(0, inplace=True)
	# insert fileid and tripid
	#torqbuffer.insert(1, "fileid", [tf_fileid for k in range(len(torqbuffer))])
	#torqbuffer.insert(2, "tripid", [tf_tripid for k in range(len(torqbuffer))])
	# [datetime.strptime(k,'%d-%b-%Y %H:%M:%S') for k in torqbuffer['devicetime']]
	for k in torqbuffer.columns:
		mapping = {'-': 0}
		try:
			if '-' in torqbuffer[k]:
				torqbuffer = torqbuffer.with_columns(replace(k,mapping))
		except ComputeError as e:
			logger.error(f'[read_buff] {type(e)} {e} csvfile={tf_csvfile} column={k}')
	fileid_series = pl.Series("fileid", [tf_fileid for k in range(len(torqbuffer))])
	tripid_series = pl.Series("tripid", [tf_tripid for k in range(len(torqbuffer))])
	torqbuffer.insert_at_idx(1, fileid_series)
	torqbuffer.insert_at_idx(2, tripid_series)
	# fix datetime formatting for devicetime and gpstime
	try:
		if len(torqbuffer['devicetime'][0]) == 28:
			devicetime = pl.Series('devicetime', [datetime.strptime(k,'%a %b %d %H:%M:%S GMT %Y') for k in torqbuffer['devicetime']])
		elif len(torqbuffer['devicetime'][0]) == 24:
			devicetime = pl.Series('devicetime', [datetime.strptime(k,'%d-%b-%Y %H:%M:%S.%f') for k in torqbuffer['devicetime']])
		elif len(torqbuffer['devicetime'][0]) == 20:
			devicetime = pl.Series('devicetime', [datetime.strptime(k,'%d-%b-%Y %H:%M:%S') for k in torqbuffer['devicetime']])
		else:
			logger.error(f'[read_buff] devicetime format error {torqbuffer["devicetime"][0]}')
	except ValueError as e:
		logger.error(f'[read_buff] devicetime {type(e)} {e} csvfile: {tf_csvfile}')
		if 'unconverted data remains' in str(e):
			devicetime = pl.Series('devicetime', [datetime.strptime(k,'%d-%b-%Y %H:%M:%S.%f') for k in torqbuffer['devicetime']])
			#logger.warning(f'[read_buff] devicetime strptime {type(e)} {e} csvfile={tf_csvfile}')
	#gpstime = pl.Series('gpstime', [datetime.strptime(k,'%d-%b-%Y %H:%M:%S') for k in torqbuffer['gpstime']])
	#devtime = pl.Series('devicetime', [to_datetime(dateparser.parse(k)) for k in torqbuffer['devicetime']])
	# 'Sun Oct 10 11:58:45 GMT+02:00 2021' does not match format '%d-%b-%Y %H:%M:%S' in read_buff
	# 'Sun Oct 10 11:58:45 GMT+02:00 2021' does not match format '%a %b %d %H:%M:%S %Z+%z %Y' in read_buff
	try:
		#gpstime = pl.Series('gpstime', [dateparser.parse(k) for k in torqbuffer['gpstime']])
		if len(torqbuffer['gpstime'][0]) == 28:
			gpstime = pl.Series('gpstime', [datetime.strptime(k,'%a %b %d %H:%M:%S GMT %Y') for k in torqbuffer['gpstime']])
		elif len(torqbuffer['gpstime'][0]) == 34:
			gpstime = pl.Series('gpstime', [datetime.strptime(k,'%a %b %d %H:%M:%S %Z%z %Y') for k in torqbuffer['gpstime']])
		else:
			logger.error(f'[read_buff] gpstime format error ex: {torqbuffer["gpstime"][0]} len: {len(torqbuffer["gpstime"][0])}')
	except (ComputeError, ValueError) as e:
		logger.error(f'[read_buff] {type(e)} {e} csvfile: {tf_csvfile}')

	torqbuffer = torqbuffer.drop('devicetime')
	torqbuffer = torqbuffer.drop('gpstime')
	torqbuffer.insert_at_idx(3, gpstime)
	torqbuffer.insert_at_idx(4, devicetime)

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
	# if not buffer['torqbuffer']:
	# 	logger.warning(f'[sqlsender] buffer is missing!')
	# 	return None
	# if buffer['torqbuffer'].is_empty():
	# 	logger.warning(f'[sqlsender] buffer is empty {buffer["tf_csvfile"]}')
	# 	return None
	results = {
		'fileid': buffer['fileid'],
		'tripid': buffer['tripid'],
		'tf_csvfile': buffer['tf_csvfile'],
		'status': 'unknown'
	}
	try:
		tmpbuf = buffer['torqbuffer'].to_pandas()
	except ValueError as e:
		logger.error(f'[tosql] tmpbuf {type(e)} {e}')
		raise ValueError(f'[tosql] tmpbuf {type(e)} {e}')
	# logger.info(f'[tosql] tmpbuf.is_empty() {buffer["torqbuffer"].is_empty()} ')
	try:
		tmpbuf.to_sql('torqlogs', con=engine, if_exists='append', index=False)
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
		#logger.error(f'[!]{type(e)}\n{e}\n')
		tf_csvfile = buffer['tf_csvfile'] # session.query(TorqFile).filter(TorqFile.id == results['fileid']).first()
		errmsg = e.args[0]
		err_row = errmsg.split('row')[-1].strip()
		err_row = errmsg.split(',')[1].split('at row')[1].strip().strip('")')
		if 'Incorrect double value' in errmsg:
			err_col = errmsg.split()[8].split('.')[2].strip("`")
		else:
			err_col = errmsg.split(',')[1].split('at row')[0].split("'")[1]
		# logger.warning(f'\n[tosql] code={e.code}\nargs={e.args[0]}\nr={results}\nerr_row: {err_row}\nerr_col:{err_col}\ntorqfile={tf_err} tf_csvfile={buffer["tf_csvfile"]}\n')  # error:{e}
		logger.warning(f'\n[tosql] {type(e)} code={e.code} err_row: {err_row} err_col:{err_col} torqfile={tf_csvfile} fileid:{buffer["fileid"]}')  # error:{e}
		# tmpbuf = tmpbuf.drop(columns=err_col)
		err_row = int(err_row)
		try:
			tmpbuf = tmpbuf.drop(index=err_row)
		except Exception as exc:
			logger.error(f'[torql] {type(exc)} {exc} err_row: {err_row} err_col:{err_col} torqfile={tf_csvfile} fileid:{buffer["fileid"]}')
		try:
			#logger.warning(f'[tosql] dataerr code:{e.code} err:{errmsg} err_row: {err_row} err_col:{err_col} r={results}')  # row:{err_row} {buffer.iloc[err_row]}')
			# buffer = buffer.drop(columns=[err_col])
			#tmpbuf = tmpbuf.drop([err_col])
			tmpbuf.to_sql('torqlogs', con=engine, if_exists='append', index=False)
			# buffer['torqbuffer'] = buffer['torqbuffer'].drop(columns=[err_col])
			# buffer['torqbuffer'].to_pandas().to_pandas().to_sql('torqlogs', con=engine, if_exists='append', index=False)
			results['status'] = 'warning'
		except (IndexError, KeyError, DataError) as ex:
			errmsg = ex.args[0]
			logger.error(f'[!] {type(ex)}\nerrmsg: {errmsg}\n')
	except TypeError as e:
		logger.error(f'[!]{type(e)}\n{e}\n')
	except ValueError as e:
		logger.error(f'[!]{type(e)}\n{e}\n')
		#errmsg = e.args[0]
		#err_row = errmsg.split('row')[-1].strip()
		#logger.error(f'[tosql] code:{e.code} err:{errmsg} row:{err_row} {buffer.iloc[err_row]} r={results} tf_csvfile={buffer["tf_csvfile"]}')
		#results['status'] = 'error'
	return results



def torq_worker(tf, dburl):
	buffer = None
	results = None
	datares = None
	t0 = datetime.now()
	try:
		buffer = read_buff(tf.csvfilefixed, tf.id, tf.tripid)
	except (ValueError, TypeError, PicklingError, ComputeError) as e:
		logger.error(f'[!] {type(e)} {e} in read_buff')
		return None
	try:
		results = sqlsender(buffer, dburl)
	except (ValueError, TypeError, PicklingError) as e:
		logger.error(f'[!] {type(e)} {e} in sqlsender buffer.is_empty() {buffer["torqbuffer"].is_empty()}')
		return None
	try:
		datares = send_torqdata(tf.id, dburl)
	except (ValueError, TypeError, PicklingError) as e:
		logger.error(f'{type(e)} {e} in send_torqdata')
		return None

	tx = (datetime.now() - t0).total_seconds()
	res = {
		'tf': tf,
		'bufferlen': len(buffer),
		'results': results,
		'datares': datares if datares else None,
		'processing_time': tx,
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
		dburl = f'sqlite:///torqfiskur.db'
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
			logger.error(f'[database_dropall] {e}')

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
		tripstart = timer()
		dbtorqfiles = session.query(TorqFile).filter(TorqFile.read_flag == 0).all()
		for torqfile in dbtorqfiles:
			send_torqtrips(torqfile, session)
		tripend = timer()
		logger.debug(f'[main] sendtrips t0={datetime.now()-t0} time={timedelta(seconds=tripend - tripstart)} starting read_process for {len(dbtorqfiles)} files mode={args.threadmode}')
		tasks = []
		if args.threadmode == 'ppe':
			with ProcessPoolExecutor(max_workers=CPU_COUNT) as executor:
				for idx, tf in enumerate(dbtorqfiles):
					t = session.query(TorqFile).filter(TorqFile.id == tf.id).first()
					tasks.append(executor.submit(torq_worker,t, dburl))
		elif args.threadmode == 'tpe':
			with ThreadPoolExecutor(max_workers=CPU_COUNT) as executor:
				for idx, tf in enumerate(dbtorqfiles):
					t = session.query(TorqFile).filter(TorqFile.id == tf.id).first()
					tasks.append(executor.submit(torq_worker,t, dburl))
		main_results = []
		res_complete = 0
		for res in as_completed(tasks):
			res_complete += 1
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
		total_time = sum([k['processing_time'] for k in main_results])
		logger.debug(f'[*] done t={datetime.now() - t0} total_time={total_time} mr={len(main_results)} threadmode={args.threadmode}')


if __name__ == '__main__':
	parser = argparse.ArgumentParser(description="torqtool")
	parser.add_argument("--path", nargs="?", default=".", help="path to csv files", action="store")
	parser.add_argument("--file", nargs="?", default=".", help="path to single csv file", action="store")
	# parser.add_argument("--gui", default=False, help="Run gui", action="store_true", dest='gui')
	# parser.add_argument("--init-db", default=False, help="init database", action="store_true", dest='init_db')
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
