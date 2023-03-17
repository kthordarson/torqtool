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
from pandas import (DataFrame, Index, Series, concat, read_csv, to_datetime, read_sql)
from sqlalchemy import create_engine, text
from sqlalchemy.exc import (ArgumentError, CompileError, DataError, IntegrityError, OperationalError, ProgrammingError)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker)
from sqlalchemy.orm.exc import DetachedInstanceError
from datamodels import get_trip_profile, send_torqfiles, send_torq_trip, database_init, sqlite_db_init, send_trip_profile, send_torqtrips,database_dropall
from datamodels import Torqdata, TorqFile, Torqtrips
from updatetripdata import create_tripdata, send_torqdata
from utils import checkcsv, get_csv_files, convert_datetime
from datamap import entry_datamap
BADVALS = ['-', 'NaN', '0', 'â', r'0']

CPU_COUNT = cpu_count()

def read_buff(tf_csvfile, tf_fileid, tf_tripid):
	start = timer()
	csvfilefixed = tf_csvfile
	datefields = ['gpstime', 'devicetime']
	try:
		torqbuffer = read_csv(csvfilefixed, delimiter=',', na_values=BADVALS, low_memory=False, parse_dates=datefields, converters={'gpstime': convert_datetime}, dtype=entry_datamap, on_bad_lines='skip')
	except ValueError as e:
		logger.error(f'[read_buff] {e} tf_csvfile={tf_csvfile} csvfilefixed={csvfilefixed}')
		return None
	torqbuffer.fillna(0, inplace=True)
	# insert fileid and tripid
	torqbuffer.insert(1, "fileid", [tf_fileid for k in range(len(torqbuffer))])
	torqbuffer.insert(2, "tripid", [tf_tripid for k in range(len(torqbuffer))])
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
		buffer['torqbuffer'].to_sql('torqlogs', con=engine, if_exists='append', index=False)
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
		buffer['torqbuffer'].to_sql('torqlogs', con=engine, if_exists='append', index=False)
		results['status'] = 'warning'
	except TypeError as e:
		errmsg = e.args[0]
		err_row = errmsg.split('row')[-1].strip()
		logger.error(f'[tosql] code:{e.code} err:{errmsg} row:{err_row} {buffer.iloc[err_row]} r={results} tf_csvfile={buffer["tf_csvfile"]}')
		results['status'] = 'error'
	return results


def read_process_ppe(dbtorqfiles, session):

	read_res = []
	tasklist = []
	read_task_start = timer()
	t0=datetime.now()
	with ProcessPoolExecutor(max_workers=CPU_COUNT) as executor:
		for tf in dbtorqfiles:
			tasklist.append(executor.submit(read_buff, tf.csvfilefixed, tf.id, tf.tripid))
	for res in as_completed(tasklist):
		# set read_flag
		r = res.result()
		ntf = session.query(TorqFile).filter(TorqFile.id == r['fileid']).first()
		ntf.read_flag = 1
		#session.add(tf)
		session.commit()
		br = {
			'fileid': r['fileid'],
			'tripid': r['tripid'],
			'torqbuffer': r['torqbuffer'],
		}
		read_res.append(br)
	# buffer = [b for b in buffs]
	read_task_end = timer()
	logger.debug(f'read_res={len(read_res)} t={timedelta(seconds=read_task_end - read_task_start)} / {datetime.now()-t0}')
	return read_res

def read_process_tpe(dbtorqfiles, session):
	read_res = []
	tasklist = []
	read_task_start = timer()
	t0=datetime.now()
	with ThreadPoolExecutor(max_workers=CPU_COUNT) as executor:
		for tf in dbtorqfiles:
			tasklist.append(executor.submit(read_buff, tf.csvfilefixed, tf.id, tf.tripid))
	for res in as_completed(tasklist):
		# set read_flag
		r = res.result()
		ntf = session.query(TorqFile).filter(TorqFile.id == r['fileid']).first()
		ntf.read_flag = 1
		#session.add(tf)
		session.commit()
		br = {
			'fileid': r['fileid'],
			'tripid': r['tripid'],
			'torqbuffer': r['torqbuffer'],
		}
		read_res.append(br)
	# buffer = [b for b in buffs]
	read_task_end = timer()
	logger.debug(f'read_res={len(read_res)} t={timedelta(seconds=read_task_end - read_task_start)} / {datetime.now()-t0}')
	return read_res

def send_process_ppe(buffs, session):
	sendtasks = []
	sendres = []
	with ProcessPoolExecutor(max_workers=CPU_COUNT) as executor:
		for idx, tchunk in enumerate(buffs):
			sendtasks.append(executor.submit(sqlsender, tchunk, session))
	for res in as_completed(sendtasks):
		r = res.result()
		if r:
			sendres.append(r)
	return sendres

def send_process_tpe(buffs, session):
	sendtasks = []
	sendres = []
	with ThreadPoolExecutor(max_workers=CPU_COUNT) as executor:
		for idx, tchunk in enumerate(buffs):
			sendtasks.append(executor.submit(sqlsender, tchunk, session))
	for res in as_completed(sendtasks):
		r = res.result()
		if r:
			sendres.append(r)
	return sendres


def send_data_process_ppe(dbtorqfiles, dburl):
	sendtasks = []
	sendres = []
	with ProcessPoolExecutor(max_workers=CPU_COUNT) as executor:
		for idx, tf in enumerate(dbtorqfiles):
			sendtasks.append(executor.submit(send_torqdata,tf.id,dburl))
	for res in as_completed(sendtasks):
		try:
			r = res.result()
		except OperationalError as e:
			logger.error(f'[sdp] OperationalError {e}')
		if r:
			sendres.append(r)
	return sendres

def send_data_process_tpe(dbtorqfiles, dburl):
	sendtasks = []
	sendres = []
	with ThreadPoolExecutor(max_workers=CPU_COUNT) as executor:
		for idx, tf in enumerate(dbtorqfiles):
			sendtasks.append(executor.submit(send_torqdata,tf.id,dburl))
	for res in as_completed(sendtasks):
		try:
			r = res.result()
		except OperationalError as e:
			logger.error(f'[sdp] OperationalError {e}')
		if r:
			sendres.append(r)
	return sendres

class TorqWorker(Thread):
	def __init__(self, tf=None, dburl=None):
		Thread.__init__(self)
		self.tf = tf
		self.dburl = dburl
		#self.engine = create_engine(self.dburl, echo=False)
		#self.Session = sessionmaker(bind=self.engine)
		#self.Session = sessionmaker(bind=self.engine)
		#self.session = self.Session()

	def __repr__(self):
		return f'Worker: {self.tf}'

	def run(self):
		buffer = read_buff(self.tf.csvfilefixed, self.tf.id, self.tf.tripid)
		if buffer:
			try:
				results = sqlsender(buffer, self.dburl)
			except TypeError as e:
				errmsg = f'[TW] {self} typeerror {e}'
				logger.error(errmsg)
				return errmsg
			datares = send_torqdata(self.tf.id, self.dburl)
			return datares
		else:
			logger.error(f'[tw] no buffer from {self.tf.csvfilefixed}')
			return None

def get_torq_workers(torqfiles=None, dburl=None, engine=None, session=None):
	workers = []
	for tf in torqfiles:
		w = TorqWorker(tf=tf, dburl=dburl, engine=None, session=session)
		workers.append(w)
	return workers

def mainold(args):
	# 1. scan args.path for csv files
	# 2. check if csv files are in db
	# 3. if not in db, foreach run fixer, create TorqFile and send to db
	# 4.
	# 5. read profile.properties from csvfile folder, foreach, create Torqtrips and send to db
	# 6. foreach new TorqFile, read fixed csv, create TorqLogs and send to db
	# 7. read_process_ppe collects csvdata, insert columns fileid and tripid
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
	if args.init_db:
		try:
			database_init(session, engine)
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
		tripstart = timer()
		dbtorqfiles = session.query(TorqFile).filter(TorqFile.read_flag == 0).all()
		for torqfile in dbtorqfiles:
			send_torqtrips(torqfile, session)
		tripend = timer()
		logger.debug(f'[send_torqtrips] done t0={datetime.now()-t0} time={timedelta(seconds=tripend - tripstart)} starting read_process mode={args.threadmode}')
		readstart = timer()
		if args.threadmode == 'ppe':
			buffs = read_process_ppe(dbtorqfiles, session)
		elif args.threadmode == 'tpe':
			buffs = read_process_tpe(dbtorqfiles, session)
		readend = timer()
		logger.debug(f'[read_process] done t0={datetime.now()-t0} time={timedelta(seconds=readend - readstart)} buffs:{len(buffs)} starting send_process mode={args.threadmode}')
		send_start = timer()
		if args.threadmode == 'ppe':
			sendres = send_process_ppe(buffs, dburl)
		elif args.threadmode == 'tpe':
			sendres = send_process_tpe(buffs, dburl)
		for r in sendres:
			if r['status'] == 'success':
				# sent send_flag on successful send
				ntf = session.query(TorqFile).filter(TorqFile.id == r['fileid']).first()
				ntf.send_flag = 1
				session.commit()
		send_end = timer()

		datastart = timer()
		logger.debug(f'[send_process] done t0={datetime.now()-t0} time={timedelta(seconds=send_end - send_start)} starting send_data_process mode={args.threadmode}')
		#for tf in dbtorqfiles:
		#	send_torqdata(tf, session, engine)
		if args.threadmode == 'ppe':
			send_data_process_ppe(dbtorqfiles, dburl)
		elif args.threadmode == 'tpe':
			send_data_process_tpe(dbtorqfiles, dburl)
		# create_tripdata(engine, session, newfilelist)
		dataend = timer()

		logger.info(f'[*] timers t0={datetime.now()-t0} readtime={timedelta(seconds=readend - readstart)} sendtime={timedelta(seconds=send_end - send_start)} triptime={timedelta(seconds=tripend - tripstart)} datatime={timedelta(seconds=dataend - datastart)} buffs:{len(buffs)} dbtorqfiles={len(dbtorqfiles)} CPU_COUNT={CPU_COUNT}')
		# fixtime={timedelta(seconds=fix_end - fix_start)} uptime={timedelta(seconds=up_end - up_start)}

def torq_worker(tf, dburl):
	try:
		buffer = read_buff(tf.csvfilefixed, tf.id, tf.tripid)
	except EmptyDataError as e:
		logger.error(e)
		return None
	results = sqlsender(buffer, dburl)
	datares = send_torqdata(tf.id, dburl)
	res = {
		'tf': tf,
		'bufferlen': len(buffer),
		'results': results,
		'datares': datares
	}
	return res



def main(args):
	# 1. scan args.path for csv files
	# 2. check if csv files are in db
	# 3. if not in db, foreach run fixer, create TorqFile and send to db
	# 4.
	# 5. read profile.properties from csvfile folder, foreach, create Torqtrips and send to db
	# 6. foreach new TorqFile, read fixed csv, create TorqLogs and send to db
	# 7. read_process_ppe collects csvdata, insert columns fileid and tripid
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
		#workers = get_torq_workers(torqfiles=dbtorqfiles, dburl=dburl, engine=engine, session=session)
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
