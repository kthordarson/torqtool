import argparse
import asyncio
import functools
import sys
from pathlib import Path
from sqlalchemy.exc import InternalError
from psycopg2.errors import InvalidTextRepresentation
from concurrent.futures import (ProcessPoolExecutor, as_completed)
from datetime import datetime, timedelta
from timeit import default_timer as timer
from hashlib import md5
from multiprocessing import cpu_count

from loguru import logger
from pandas import (DataFrame, Index, Series, concat, read_csv, to_datetime, read_sql)
from sqlalchemy import create_engine, text
from sqlalchemy.exc import (ArgumentError, CompileError, DataError, IntegrityError, OperationalError, ProgrammingError)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker)
from sqlalchemy.orm.exc import DetachedInstanceError
from datamodels import get_trip_profile, send_torqfiles, send_torq_trip, database_init, sqlite_db_init, send_trip_profile, send_torqtrips
from datamodels import Torqdata, TorqFile, Torqtrips
from updatetripdata import create_tripdata
from utils import checkcsv, get_csv_files, convert_datetime
from datamap import entry_datamap
BADVALS = ['-', 'NaN', '0', 'â', r'0']



def read_buff(tf_csvfile, tf_fileid, tf_tripid):
	start = timer()
	csvfilefixed = tf_csvfile
	datefields = ['gpstime', 'devicetime']
	# torqbuffer = read_csv(csvfilefixed, delimiter=',', na_values=BADVALS, low_memory=False, dtype=entry_datamap)
	torqbuffer = read_csv(csvfilefixed, delimiter=',', na_values=BADVALS, low_memory=False, parse_dates=datefields, converters={'gpstime': convert_datetime}, dtype=entry_datamap)
	torqbuffer.fillna(0, inplace=True)
	# insert fileid and tripid
	torqbuffer.insert(1, "fileid", [tf_fileid for k in range(len(torqbuffer))])
	torqbuffer.insert(2, "tripid", [tf_tripid for k in range(len(torqbuffer))])
	end = timer()
	resultbuffer = {
		'torqbuffer' : torqbuffer,
		'fileid' : tf_fileid,
		'tripid' : tf_tripid,
	}
	return resultbuffer

#def data_sender(buffer, session,)

def sqlsender(buffer=None, session=None):
	results = {
		'fileid': buffer['fileid'],
		'tripid': buffer['tripid'],
		'status': 'unknown'
	}
	try:
		buffer['torqbuffer'].to_sql('torqlogs', con=session, if_exists='append', index=False)
		results['status'] = 'success'
	except (OperationalError, ProgrammingError, DataError) as e:
		logger.error(f'[tosql] code={e.code} args={e.args[0]}')  # error:{e}
		results['status'] = 'error'
	except InternalError as e:
		logger.error(e)
		results['status'] = 'error'
	except (InvalidTextRepresentation,IntegrityError) as e:
		logger.warning(f'[tosql] {type(e)} code={e.code} args={e.args[0]}')
		results['status'] = 'error'
		# logger.warning(f'[tosql] {e.statement} {e.params}')
		# logger.warning(f'[tosql] {e}')
	except DataError as e:
		errmsg = e.args[0]
		# err_row = errmsg.split('row')[-1].strip()
		err_row = errmsg.split(',')[1].split('at row')[1].strip().strip('")')
		err_col = errmsg.split(',')[1].split('at row')[0].split("'")[1]
		logger.warning(f'[tosql] dataerr code:{e.code} err:{errmsg} err_row: {err_row} err_col:{err_col}')  # row:{err_row} {buffer.iloc[err_row]}')
		buffer = buffer.drop(columns=[err_col])
		buffer.to_sql('torqlogs', con=session, if_exists='append', index=False)
		results['status'] = 'warning'
	except TypeError as e:
		errmsg = e.args[0]
		err_row = errmsg.split('row')[-1].strip()
		logger.error(f'[tosql] code:{e.code} err:{errmsg} row:{err_row} {buffer.iloc[err_row]}')
		results['status'] = 'error'
	return results


def read_process(dbtorqfiles, session):
	maxworkers = cpu_count()
	buffs = []
	read_res = []
	tasklist = []
	with ProcessPoolExecutor(max_workers=maxworkers) as executor:
		read_task_start = timer()
		for tf in dbtorqfiles:
			tasklist.append(executor.submit(read_buff, tf.csvfilefixed, tf.id, tf.tripid))
		read_task_end = timer()

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
	logger.debug(f'buffs:{len(buffs)} read_res={len(read_res)} t={timedelta(seconds=read_task_end - read_task_start)}')
	return read_res

def send_process(buffs, session):
	maxworkers = cpu_count()
	sendtasks = []
	sendres = []
	with ProcessPoolExecutor(max_workers=maxworkers) as executor:
		for idx, tchunk in enumerate(buffs):
			sendtasks.append(executor.submit(sqlsender, tchunk, session))
	for res in as_completed(sendtasks):
		r = res.result()
		if r:
			sendres.append(r)
	return sendres

def xsend_process(buffs, session):
	maxworkers = cpu_count()
	sendtasks = []
	sendres = []
	with ProcessPoolExecutor(max_workers=maxworkers) as executor:
		for idx, tchunk in enumerate(buffs):
			sendtasks.append(executor.submit(sqlsender, tchunk, session))
			#logger.debug(f'idx={idx} tc={type(tchunk)} sendtasks = {len(sendtasks)}')
	# for res in as_completed(sendtasks):
	# 	r = res.result()
	# 	if r:
	# 		sendres.append(r)
	# 		logger.debug(f'[send] r={len(r)} sr={len(sendres)}')
	return sendres


def main(args):
	# 1. scan args.path for csv files
	# 2. check if csv files are in db
	# 3. if not in db, foreach run fixer, create TorqFile and send to db
	# 4.
	# 5. read profile.properties from csvfile folder, foreach, create Torqtrips and send to db
	# 6. foreach new TorqFile, read fixed csv, create TorqLogs and send to db
	# 7. read_process collects csvdata, insert columns fileid and tripid
	# 8. send csvdata to db
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
		logger.debug(f'[send_torqtrips] done t0={datetime.now()-t0} time={timedelta(seconds=tripend - tripstart)} starting read_process')
		readstart = timer()
		buffs = read_process(dbtorqfiles, session)
		readend = timer()
		logger.debug(f'[read_process] done t0={datetime.now()-t0} time={timedelta(seconds=readend - readstart)} buffs:{len(buffs)} starting send_process')
		send_start = timer()
		sendres = send_process(buffs, dburl)
		for r in sendres:
			if r['status'] == 'success':
				# sent send_flag on successful send
				ntf = session.query(TorqFile).filter(TorqFile.id == r['fileid']).first()
				ntf.send_flag = 1
				session.commit()
		send_end = timer()
		logger.debug(f'[send_process] done t0={datetime.now()-t0} time={timedelta(seconds=send_end - send_start)} starting create_tripdata')
		datastart = timer()
		create_tripdata(engine, session, newfilelist)
		dataend = timer()

		logger.info(f'[*] timers t0={datetime.now()-t0} readtime={timedelta(seconds=readend - readstart)} sendtime={timedelta(seconds=send_end - send_start)} triptime={timedelta(seconds=tripend - tripstart)} datatime={timedelta(seconds=dataend - datastart)}')
		# fixtime={timedelta(seconds=fix_end - fix_start)} uptime={timedelta(seconds=up_end - up_start)}


if __name__ == '__main__':
	parser = argparse.ArgumentParser(description="torqtool")
	parser.add_argument("--path", nargs="?", default=".", help="path to csv files", action="store")
	parser.add_argument("--file", nargs="?", default=".", help="path to single csv file", action="store")
	parser.add_argument("--gui", default=False, help="Run gui", action="store_true", dest='gui')
	parser.add_argument("--init-db", default=False, help="init database", action="store_true", dest='init_db')
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
	args = parser.parse_args()
	main(args)
