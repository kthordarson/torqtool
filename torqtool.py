#!/usr/bin/python3
import argparse
import asyncio
import sys
from collections.abc import AsyncIterable
from datetime import datetime
from pathlib import Path
from timeit import default_timer as timer
import pandas as pd
from loguru import logger
from sqlalchemy.exc import OperationalError
# sys.path.append('c:/apps/torqtool/torqtool')
from torqtool.datamodels import (
	Torqdata,
	TorqFile,
	Torqlogs,
	Torqtrips,
	database_dropall,
	send_torqfiles,
)
from torqtool.utils import (
	fix_logfile,
	generate_torqdata,
	get_csv_files,
	get_engine_session,
	send_torqtripdata,
	torq_dataworker_ppe,
	torq_worker_ppe,
)

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

async def create_torqdata(session, args):
	# dataworkders
	return
	t0 = datetime.now()
	dbtorqfiles = session.query(TorqFile).all() # filter(TorqFile.data_flag == 0).all() # type: ignore
	if args.debug:
		logger.debug(f'[create_torqdata] t: {(datetime.now()-t0).seconds} starting {len(dbtorqfiles)} dataworkers')
	async with asyncio.TaskGroup() as tg:
		for idx, tf in enumerate(dbtorqfiles):
			#asyncio.set_event_loop(loop)
			t = session.query(TorqFile).filter(TorqFile.fileid == tf.fileid).first()
			if args.debug:
				logger.debug(f'[{idx}/{len(dbtorqfiles)}] t: {(datetime.now()-t0).seconds} starting dataworker {t=} for {tf=} tfid:{tf.fileid}')
			tg.create_task(torq_dataworker_ppe(t, session, args.debug))
			#await asyncio.gather(*tasks)
	if args.debug:
		logger.debug(f't: {(datetime.now()-t0).seconds} finished {len(dbtorqfiles)} dataworkers')


async def scanpath(session, args):
	"""
	scan a path for log files
	param: engine sqlalchemy engine
	param: args argparse namespace
	return: dict with results
	{
	'results' : {'unfixed' : list_of_unfixed_files}}
	}
	"""
	results = { 'results': {'unfixed': []}}
	newfilelist = []
	t0 = datetime.now()
	#Session = sessionmaker(bind=engine)
	#session = Session()

	filelist = get_csv_files(searchpath=Path(args.logpath), dbmode=args.dbmode, debug=args.debug)
	filelist = sorted(filelist, key=lambda x: x['csvfile']) # sort by filename (date)

	if len(filelist) == 0:
		logger.error(f'no csv files found in {args.logpath}')
		sys.exit(1)
	try:
		newfilelist = send_torqfiles(filelist, session, debug=args.debug)
	except Exception as e:
		logger.error(f'[!] unhandled {type(e)} {e}')
		sys.exit(1)
	finally:
		return newfilelist

async def check_unfixedfiles(session, args):

	# get list of unfixed files from db
	t0 = datetime.now()
	unfixedfiles = session.query(TorqFile).filter(not TorqFile.fixed_flag).all()
	results = { 'results': {'unfixed': []}}

	if len(unfixedfiles)>0:
		if args.debug:
			logger.warning(f't: {(datetime.now()-t0).seconds} found {len(unfixedfiles)} unfixed files')
		for unfixed in unfixedfiles:
			punfix = Path(unfixed.csvfile)
			if args.debug:
				logger.debug(f'sending {punfix} to fixer {unfixed=} {type(unfixed)}')
			if fix_logfile(punfix):
				unfixed.fixed_flag = 1
				session.commit()
				if args.debug:
					logger.debug(f't: {(datetime.now()-t0).seconds} fixed {unfixed}')
			else:
				results['results']['unfixed'].append(unfixed)
				if args.debug:
					logger.warning(f't: {(datetime.now()-t0).seconds} fixer failed on {unfixed} unfixed: {len(results["results"]["unfixed"])}')

async def send_torq_logs(filelist, session, args):
	# get files from db that are fixed but not read or sent to db
	tripstart = timer()
	# dbtorqfiles = session.query(TorqFile).filter(TorqFile.read_flag == 1).filter(TorqFile.fixed_flag == 1).all() # type: ignore
	tripend = timer()
	t0 = datetime.now()
	if args.debug:
		logger.debug(f'sendtorqlogs  starting torq_worker_ppe for {len(filelist)} files mode={args.threadmode}')
	async with asyncio.TaskGroup() as tg:
		for idx, tf in enumerate(filelist):
			#asyncio.set_event_loop(loop)
			t = session.query(TorqFile).filter(TorqFile.fileid == tf.fileid).first()
			if args.debug:
				pass # logger.debug(f'[tw] t0={datetime.now()-t0} {tf=} {t}')
			if t:
				tg.create_task(torq_worker_ppe(t, session, args.debug))
			else:
				logger.warning(f'no t from {tf}')
			#await asyncio.gather(*tasks)

async def collect_info(session) -> AsyncIterable[str]:
	yield session.query(Torqtrips).count()
	yield session.query(TorqFile).count()
	yield session.query(Torqlogs).count()
	yield session.query(Torqdata).count()

async def collect(async_iterable):
    return [item async for item in async_iterable]

async def main(args):
	# 1. scan args.logpath for csv files
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
	if args.torqdata:
		await create_torqdata(session, args)
		sys.exit(0)
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
		print(f'[dbinfo]  trips: {results[0][0]} files: {results[0][1]} logs: {results[0][2]} data: {results[0][3]}')
		#files = session.query(Torqtrips).count()
		#trips = session.query(Torqtrips).count()
		#logs = session.query(Torqlogs).count()
		#data = session.query(Torqdata).count()
		#logger.info(f'[main] {files=} {trips=} {logs=:,} {data=}')
		sys.exit(0)
	if args.create_trips:
		# create trips data from database
		tf_ids = session.query(TorqFile.fileid).all()
		data = pd.DataFrame()
		for idx, tf in enumerate(tf_ids):
			# data = session.query(Torqlogs).filter(Torqlogs.fileid == tf.fileid).all()
			try:
				data = pd.read_sql(session.query(Torqlogs).filter(Torqlogs.fileid==tf.fileid).statement,con=engine)
			except OperationalError as e:
				logger.error(f'{idx} {e} {tf=}')
				continue
			if not data.empty:
				tripdata = None
				logger.info(f'[{idx}/{len(tf_ids)}] Generating tripdata for fileid {tf.fileid} ')
				try:
					tripdata = generate_torqdata(data, session, args)
				except Exception as e:
					logger.error(f'[!] unhandled {type(e)} {e} {tf=}')
					sys.exit(1)
				if tripdata:
					logger.debug(f'[{idx}/{len(tf_ids)}] Sending {len(tripdata)} tripdata for fileid {tf.fileid} ')
					send_torqtripdata(tripdata, session, args.debug)
		sys.exit(0)
	if args.scanpath:
		results = None
		res = None
		results = await scanpath(session, args)
		if args.debug:
			pass # logger.debug(f"t: {(datetime.now()-t0).seconds} scanpath returned {len(results)} files")
		for csvfile in results:
			if args.debug:
				pass # logger.debug(f"t: {(datetime.now()-t0).seconds} fixing {csvfile.csvfile} ")
			if fix_logfile(csvfile.csvfile): # attempt to fix file, returns True if fixed
				dbf = session.query(TorqFile).filter(TorqFile.fileid == csvfile.fileid).first()
				dbf.fixed_flag = 1
				if args.debug:
					logger.debug(f't: {(datetime.now()-t0).seconds} fixed {dbf}')
			else:
				logger.warning(f'fixer failed of {csvfile.csvfile}')

		await send_torq_logs(results, session, args)

	if args.foobar:
			unfixcount = 0
			fixcount = 0
			for idx,f in enumerate(res['unfixed']):
				pcsv = Path(f.csvfile)
				if args.debug:
					logger.debug(f'[{idx}/{unfixcount}/{fixcount}] t: {(datetime.now()-t0).seconds} fixing {pcsv}')
				try:
					if fix_logfile(pcsv): # attempt to fix file, returns True if fixed
						f.fixed_flag = 1 # fixed
						dbf = session.query(TorqFile).filter(TorqFile.fileid == f.fileid).first()
						dbf.fixed_flag = 1
						if args.debug:
							logger.debug(f'[{idx}/{unfixcount}/{fixcount}] t: {(datetime.now()-t0).seconds} fixed {dbf}')
						fixcount += 1
					else:
						logger.warning(f'fixer failed of {f.csvfile}')
				except Exception as e:
					# todo fix this
					# brokenfile = str(f.csvfile).replace('trackLog-', 'broken-')
					logger.error(f'[!] unhandled {type(e)} {e} {f} renaming')
					# shutil.move(f.csvfile, brokenfile)
				finally:
					# logger.info(f'fixed: {fixcount} ')
					session.commit()
			if fixcount > 1:
				# send fixed files to db
				# read and process files
				tasks = []
				# loop = asyncio.new_event_loop()
				dbtorqfiles = session.query(TorqFile).filter(TorqFile.read_flag == 0).filter(TorqFile.fixed_flag == 1).all() # type: ignore
				async with asyncio.TaskGroup() as tg:
					for idx, tf in enumerate(dbtorqfiles):
						#asyncio.set_event_loop(loop)
						t = session.query(TorqFile).filter(TorqFile.fileid == tf.fileid).first()
						tg.create_task(torq_worker_ppe(t, session, args.debug))
						#await asyncio.gather(*tasks)


def maincli():
	print('hello world')

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description="torqtool")
	parser.add_argument('-d', '--debug', default=False, help="debugmode", action="store_true", dest='debug')

	parser.add_argument("--logpath", nargs="?", default=".", help="path to csv files", action="store")
	parser.add_argument("--file", nargs="?", default=".", help="path to single csv file", action="store")

	parser.add_argument('--torqdata', default=False, help="create torqdata", action="store_true", dest='torqdata')
	parser.add_argument('-dbdrop', '--database_dropall', default=False, help="drop database", action="store_true", dest='database_dropall')
	parser.add_argument("--create-trips", default=False, help="create trip database", action="store_true", dest='create_trips')
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
	parser.add_argument("--dbmode", default="sqlite", help="sqlmode mysql/psql/sqlite/mariadb", action="store")
	parser.add_argument('--dbfile', default='torqfiskur.db', help='database file', action='store')
	parser.add_argument("--dbname", default="torq", help="dbname", action="store")
	parser.add_argument("--dbhost", default="localhost", help="dbname", action="store")
	parser.add_argument("--dbuser", default="torq", help="dbname", action="store")
	parser.add_argument("--dbpass", default="qrot", help="dbname", action="store")
	parser.add_argument('--threadmode', default='ppe', help='threadmode ppe/oldppe/tpe', action='store')
	parser.add_argument('--foobar', default=False, help='foobar', action='store_true')
	# parser.add_argument("--gui", default=False, help="Run gui", action="store_true", dest='gui')
	# parser.add_argument("--init-db", default=False, help="init database", action="store_true", dest='init_db')


	args = parser.parse_args()
	asyncio.run(main(args))
