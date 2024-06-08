#!/usr/bin/python3
import asyncio
import shutil
from collections.abc import AsyncIterable
import argparse
import sys
from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor, as_completed)
from datetime import datetime, timedelta
from multiprocessing import cpu_count
from pathlib import Path
from pickle import PicklingError
from timeit import default_timer as timer

import polars as pl
import pymysql
from loguru import logger
from pandas.errors import EmptyDataError
from polars import ComputeError
from polars import read_csv as read_csv_polars
from sqlalchemy import create_engine
from sqlalchemy.exc import (DataError, IntegrityError, InternalError, OperationalError, ProgrammingError)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session
from datamodels import (TorqFile, Torqtrips, Torqlogs, Torqdata, database_dropall, database_init, send_torqfiles)
from updatetripdata import send_torqdata, send_torqdata_ppe
from utils import fix_logfile, get_csv_files, get_engine_session, mapping_replace,  sqlsender, sqlsender_ppe, torq_worker_ppe

# june2024 rewrite: log files are stored diffrently from previous versions
# now the app stores the logs on the phone under /storage/emulated/0/Documents/torqueLogs
# one log file per trip, the log file is named with the start time of the trip
# newer versions do not create profile.properties files
# todo pull profile.properties info from log files
# todo handle reading from previous versions
# todo merge logs from previous versions
# check log files for errors and cleanup
# if a log files contains entries from more than 24h, check and split ???
# more ....

async def scanpath(engine, args):
	"""
	scan a path for log files
	param: engine sqlalchemy engine
	param: args argparse namespace
	return: dict with results
	{
	'results' : {'unfixed' : list_of_unfixed_files}}
	}
	"""
	results = {
		'results': {'unfixed': []}
	}
	t0 = datetime.now()
	Session = sessionmaker(bind=engine)
	session = Session()

	filelist = get_csv_files(searchpath=Path(args.path), dbmode=args.dbmode, debug=args.debug)
	filelist = sorted(filelist, key=lambda x: x['csvfile']) # sort by filename (date)

	if len(filelist) == 0:
		logger.error(f'no csv files found in {args.path}')
		sys.exit(1)
	try:
		newfilelist = send_torqfiles(filelist, session, debug=args.debug)
	except Exception as e:
		logger.error(f'[!] unhandled {type(e)} {e}')
		sys.exit(1)

	# get list of unfixed files from db
	unfixedfiles = session.query(TorqFile).filter(TorqFile.fixed_flag == 0).all()
	results['results']['unfixed'] = unfixedfiles
	if len(unfixedfiles)>0:
		if args.debug:
			logger.warning(f'found {len(unfixedfiles)} unfixed files')
			return results

	# get files from db that are fixed but not read or sent to db
	tripstart = timer()
	dbtorqfiles = session.query(TorqFile).filter(TorqFile.read_flag == 0).filter(TorqFile.fixed_flag == 1).all() # type: ignore
	if args.debug:
		logger.info(f'found {len(dbtorqfiles)} unread and unfixed files')
	dbcols = None # session.execute(text('show columns from torqdata')).fetchall() # get column names
	tripend = timer()
	if args.debug:
		logger.debug(f'[main] send_torqtrips done t0={datetime.now()-t0} time={timedelta(seconds=tripend - tripstart)} starting read_process for {len(newfilelist)} files mode={args.threadmode}')

	if len(dbtorqfiles) == 0:
		logger.info('no new files to process')
		sys.exit(0)

async def collect_info(session) -> AsyncIterable[str]:
	yield session.query(Torqtrips).count()
	yield session.query(TorqFile).count()
	yield session.query(Torqlogs).count()
	yield session.query(Torqdata).count()

async def collect(async_iterable):
    return [item async for item in async_iterable]

async def main(args):
	# 1. scan args.path for csv files
	# 2. check if csv files are in db
	# 3. if not in db, foreach run fixer, create TorqFile and send to db
	# 4.
	# 5. read profile.properties from csvfile folder, foreach, create Torqtrips and send to db
	# 6. foreach new TorqFile, read csv, create TorqLogs and send to db
	# 7.
	# 8. send csvdata to db
	# todo: create worker thread for each file, worker reads and processes file and sends to db.
	# todo: handle new columns from csv files, eg airfuelratiomeasured1
	# todo: set read_flag and send_flag for processed files
	t0 = datetime.now()
	engine, session = get_engine_session(args)
	if args.database_dropall:
		try:
			database_dropall(engine)
			sys.exit(0)
		except OperationalError as e:
			logger.error(f'[main] database_dropall {e}')
			sys.exit(2)
		except Exception as e:
			logger.error(f'[main] database_dropall {type(e)} {e}')
			sys.exit(2)
	if args.dbinfo:
		#info = collect_info()
		tasks = [
        asyncio.create_task(collect(collect_info(session))),
        #asyncio.create_task(collect(iterable())),
        #asyncio.create_task(collect(iterable()))
    	]
		results = await asyncio.gather(*tasks)
		print(f'dbinfo: {results}')
		#files = session.query(Torqtrips).count()
		#trips = session.query(Torqtrips).count()
		#logs = session.query(Torqlogs).count()
		#data = session.query(Torqdata).count()
		#logger.info(f'[main] {files=} {trips=} {logs=:,} {data=}')
		sys.exit(0)
	if args.scanpath:
		results = None
		res = None
		results = await scanpath(engine, args)
		if results:
			res = results.get('results', None)
		if res:
			unfixcount = len(res['unfixed'])
			fixcount = 0
			logger.debug(f"found {unfixcount} unfixed files")
			for idx,f in enumerate(res['unfixed']):
				try:
					if fix_logfile(Path(f.csvfile)): # attempt to fix file, returns True if fixed
						f.fixed_flag = 1 # fixed
						dbf = session.query(TorqFile).filter(TorqFile.id == f.id).first()
						dbf.fixed_flag = 1
						if args.debug:
							logger.debug(f'[{idx}/{unfixcount}/{fixcount}] fixed {f} {dbf}')
						fixcount += 1
					else:
						logger.warning(f'fixer failed of {f.csvfile}')
				except Exception as e:
					brokenfile = str(f.csvfile).replace('trackLog-', 'broken-')
					logger.error(f'[!] unhandled {type(e)} {e} {f} renaming')
					# shutil.move(f.csvfile, brokenfile)
				finally:
					logger.info(f'fixed: {fixcount} ')
					session.commit()
			if fixcount > 1:
				# send fixed files to db
				# read and process files
				tasks = []
				loop = asyncio.new_event_loop()
				dbtorqfiles = session.query(TorqFile).filter(TorqFile.read_flag == 0).filter(TorqFile.fixed_flag == 1).all() # type: ignore
				async with asyncio.TaskGroup() as tg:
					for idx, tf in enumerate(dbtorqfiles):
						#asyncio.set_event_loop(loop)
						t = session.query(TorqFile).filter(TorqFile.id == tf.id).first()
						tg.create_task(torq_worker_ppe(t, session, args.debug))
						#await asyncio.gather(*tasks)



if __name__ == '__main__':
	parser = argparse.ArgumentParser(description="torqtool")
	parser.add_argument('-d', '--debug', default=False, help="debugmode", action="store_true", dest='debug')

	parser.add_argument("--path", nargs="?", default=".", help="path to csv files", action="store")
	parser.add_argument("--file", nargs="?", default=".", help="path to single csv file", action="store")

	parser.add_argument('-dbdrop', '--database_dropall', default=False, help="drop database", action="store_true", dest='database_dropall')
	parser.add_argument("--check-db", default=False, help="check database", action="store_true", dest='check_db')
	parser.add_argument("--dump-db", nargs="?", default=None, help="dump database to file", action="store")
	parser.add_argument("-i", "--info", default=False, help="show dbinfo", action="store_true", dest='dbinfo')

	parser.add_argument("-s", "--scanpath", default=False, help="start scanpath", action="store_true", dest='scanpath')

	parser.add_argument("--fixcsv", default=False, help="repair csv", action="store_true", dest='fixcsv')
	parser.add_argument("--combinecsv", default=False, help="make big csv", action="store_true", dest='combinecsv')
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
	parser.add_argument('--threadmode', default='ppe', help='threadmode ppe/oldppe/tpe', action='store')

	# parser.add_argument("--gui", default=False, help="Run gui", action="store_true", dest='gui')
	# parser.add_argument("--init-db", default=False, help="init database", action="store_true", dest='init_db')


	args = parser.parse_args()
	asyncio.run(main(args))
