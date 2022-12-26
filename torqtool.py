import argparse
import asyncio
import functools
import sys
from concurrent.futures import (ProcessPoolExecutor, as_completed)
from datetime import datetime, timedelta
from timeit import default_timer as timer
from hashlib import md5
from multiprocessing import cpu_count

from loguru import logger
from pandas import (DataFrame, Index, Series, concat, read_csv, to_datetime, read_sql)
from sqlalchemy import create_engine
from sqlalchemy.exc import (ArgumentError, CompileError, DataError, IntegrityError, OperationalError, ProgrammingError)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from datamap import entry_datamap
from datamodels import get_trip_profile, prepdb, send_torq_trip
from updatetripdata import create_tripdata
from utils import checkcsv, get_csv_files

BADVALS = ['-', 'NaN', '0', 'â', r'0']


def convert_datetime(val):
	newval = 0
	if val == '-':
		logger.warning(f'[cd] v:{val} ')
		return to_datetime('2000-01-01', errors='raise').to_numpy()
	try:
		newval = to_datetime(val, errors='raise', infer_datetime_format=False).to_numpy()
	except AttributeError as e:
		newval = val.strip()[2:]
		logger.warning(f'[cd] {e} v:{val} n:{newval}')
		newval = to_datetime(newval)
	return newval


async def read_buff(tf):
	start = timer()
	csvhash = tf['fixedhash']
	csvfilefixed = tf['csvfilefixed']
	tripid = tf['tripid']
	datefields = ['gpstime', 'devicetime']
	torqbuffer = read_csv(csvfilefixed, delimiter=',', na_values=BADVALS, low_memory=False, parse_dates=datefields, converters={'gpstime': convert_datetime}, dtype=entry_datamap)
	torqbuffer.fillna(0, inplace=True)
	torqbuffer.insert(1, "tripid", [tripid for k in range(len(torqbuffer))])
	end = timer()
	# logger.info(f'[read_buff] {csvfilefixed} tripid={tripid} t={timedelta(seconds=end-start)} csvs:{tf["csvsize"]} csvf:{tf["fixedsize"]} tb:{len(torqbuffer)}')
	return torqbuffer


def get_engine(args):
	if args == 'mysql':
		dburl = f"mysql+pymysql://{args.dbuser}:{args.dbpass}@{args.dbhost}/{args.dbname}?charset=utf8mb4"
	# return create_engine(dburl, pool_size=200, max_overflow=0)
	if args == 'postgresql':
		# dburl = f"postgresql://postgres:foobar9999@{args.dbhost}/{args.dbname}"
		dburl = f"postgresql://{args.dbuser}:{args.dbpass}@{args.dbhost}/{args.dbname}"
	if args == 'sqlite':
		dburl = f'sqlite:///torqfiskurdb'
	else:
		dburl = 'none'
	return create_engine(dburl)


def chunks(l, n):
	for i in range(0, len(l), n):
		yield l.iloc[i:i + n]


async def sqlsender(buffer=None, con=None, chsize=None, dburl=None):
	con = create_engine(dburl)
	try:
		buffer.to_sql('torqlogs', con=con, if_exists='append', index=False)
	except (OperationalError, ProgrammingError) as e:
		logger.warning(f'[tosql] code={e.code} args={e.args[0]}')  # error:{e}
	except IntegrityError as e:
		logger.warning(f'[tosql] code={e.code} args={e.args[0]}')
		# logger.warning(f'[tosql] {e.statement} {e.params}')
		logger.warning(f'[tosql] {e}')
	except DataError as e:
		errmsg = e.args[0]
		# err_row = errmsg.split('row')[-1].strip()
		err_row = errmsg.split(',')[1].split('at row')[1].strip().strip('")')
		err_col = errmsg.split(',')[1].split('at row')[0].split("'")[1]
		logger.warning(f'[tosql] dataerr code:{e.code} err:{errmsg} err_row: {err_row} err_col:{err_col}')  # row:{err_row} {buffer.iloc[err_row]}')
		buffer = buffer.drop(columns=[err_col])
		buffer.to_sql('torqlogs', con=con, if_exists='append', index=False)
	except TypeError as e:
		errmsg = e.args[0]
		err_row = errmsg.split('row')[-1].strip()
		logger.error(f'[tosql] code:{e.code} err:{errmsg} row:{err_row} {buffer.iloc[err_row]}')

	return f'[sqlsender] done'


# tt = torqsendtask(loop=loop_, buffer=tchunk, con=engine, chsize=chsize)
async def torqsendtask(loop=None, buffer=None, con=None, chsize=None):
	sendres = await loop.run_in_executor(None, functools.partial(sqlsender, buffer=buffer, con=con, chsize=chsize))
	logger.debug(f'[sendtask] done b:{len(buffer)} chsize:{chsize}')
	return sendres


async def torqreadtask(loop=None, tf=None):
	buff = await loop.run_in_executor(None, functools.partial(read_buff, tf=tf))
	logger.debug(f'[asyncread] done rb:{len(buff)} tf:{tf["csvfilefixed"]}')
	return buff


def fix_nulls(engine):
	for k in entry_datamap:
		df = read_sql(f'select {k} from torqlogs where {k} is null', engine)
		if len(df) >= 1:
			sqlcmd = f'update torqlogs set {k} = 0 where {k} IS NULL;'
			engine.execute(sqlcmd)
			# df0=read_sql(f'select {k} from torqlogs where {k} is null', engine)
			logger.debug(f'fixnulls k:{k} l:{len(k)}')


async def main(args):
	t0 = datetime.now()
	Base = declarative_base()
	dburl = None
	if args.dbmode == 'mysql':
		dburl = f"mysql+pymysql://{args.dbuser}:{args.dbpass}@{args.dbhost}/{args.dbname}?charset=utf8mb4"
	# engine = create_engine(dburl, pool_size=200, max_overflow=0)
	elif args.dbmode == 'postgresql':
		# dburl = f"postgresql://postgres:foobar9999@{args.dbhost}/{args.dbname}"
		dburl = f"postgresql://{args.dbuser}:{args.dbpass}@{args.dbhost}/{args.dbname}"
	elif args.dbmode == 'sqlite':
		dburl = f'sqlite:///torqfiskurdb'
	else:
		engine = None
	try:
		engine = create_engine(dburl)
		Session = sessionmaker(bind=engine)
		session = Session()
	except AttributeError as e:
		logger.error(f'engine err {e} d:{dburl} {args}')
		sys.exit(-1)

	filelist_ = get_csv_files(searchpath=args.path, dbmode=args.dbmode)
	# filelist = [k for k in reversed(sorted(filelist_, key=lambda d: d['fixedsize']) )]
	filelist = [k for k in sorted(filelist_, key=lambda d: d['csvtimestamp'])]
	newfilelist = prepdb(filelist, engine, args, dburl, Base)
	if len(newfilelist) == 0:
		sys.exit(1)
	for tidx, f in enumerate(newfilelist):
		csvfn = f['csvfilename']
		tprofile = get_trip_profile(csvfn)
		sendres = send_torq_trip(tf=f, tripdict=tprofile, session=session, engine=engine)
		newfilelist[tidx]['tripid'] = sendres
	# logger.debug(f"newfilelist[tidx]['tripid'] {newfilelist[tidx]['tripid']} = {sendres}")
	# newfilelist = [k for k in reversed(sorted(newfilelist, key=lambda d: d['tripdate']) )]
	totalbytes_fixed = sum([k['fixedsize'] for k in newfilelist])
	totalbytes_csv = sum([k['csvsize'] for k in newfilelist])
	totalbytes_lines = sum([k['buflen'] for k in newfilelist])
	logger.info(f'[sizes] totalbytes_fixed:{totalbytes_fixed} totalbytes_csv:{totalbytes_csv} totalbytes_lines:{totalbytes_lines} fl:{len(filelist)} nfl:{len(newfilelist)}')

	maxworkers = cpu_count()
	loop_ = asyncio.get_event_loop()
	read_tasks = []
	buffs = []
	readstart = timer()
	with ProcessPoolExecutor(max_workers=maxworkers) as executor:
		read_task_start = timer()
		for tf in newfilelist:
			rt = asyncio.create_task(read_buff(tf))
			read_tasks.append(rt)
		logger.debug(f'read_tasks:{len(read_tasks)}')
		buffs = await asyncio.gather(*read_tasks)
		read_task_end = timer()
		logger.debug(f'buffs:{len(buffs)} t={timedelta(seconds=read_task_end - read_task_start)}')
	# with ProcessPoolExecutor(max_workers=maxworkers) as executor:
	# 	future_to_stuff = [executor.submit(read_buff, tf) for tf in newfilelist]
	# 	for buffer in as_completed(future_to_stuff):
	# 		buffres = buffer.result()
	# 		buffs.append(buffres)
	# logger.debug(f'[tpe] br:{len(buffres)} b:{len(buffs)}')
	buffer = [b for b in buffs]
	mb = concat([b for b in buffer])
	dbmethod = None
	chsize = 10000  # int(len(mb) / len(buffer)) # 10000 # int(len(mb) / maxworkers)
	readend = timer()
	logger.debug(f'[read] done time={timedelta(seconds=readend - readstart)} b:{len(buffer)} bs:{len(buffs)} mb:{len(mb)} chsize:{chsize}')
	# chsize = (int(len(mb)/len(buffer))) #  int(totalbytes_lines/len(filelist)) #5000 # (int(len(mb)/len(buffer)))
	# todo send chunks with threads or processpool
	sendtasks = []
	results = None
	send_start = timer()
	if args.combinecsv:
		mb.to_csv('torqdump.csv')
	else:
		with ProcessPoolExecutor(max_workers=maxworkers) as executor:
			# future_to_stuff = [executor.submit(sqlsender, chunk, engine) for chunk in enumerate(chunks(mb, chsize))]
			for idx, tchunk in enumerate(chunks(mb, chsize)):
				# logger.debug(f'[{idx}] sending...')
				task = asyncio.create_task(sqlsender(buffer=tchunk, dburl=dburl))
				sendtasks.append(task)
			logger.debug(f'sendtasks = {len(sendtasks)}')
			await asyncio.gather(*sendtasks)
	send_end = timer()
	fix_start = timer()
	# fix_nulls(engine)
	fix_end = timer()
	up_start = timer()
	# todo fix [IntegrityError] trip:63 gkpj pymysql.err.IntegrityError 1452 Cannot add or update a child row: a foreign key constraint fails (`torq`.`torqdata`, CONSTRAINT `torqdata_ibfk_1` FOREIGN KEY (`id`) REFERENCES `torqlogs` (`tripid`))') tripdate=2022-12-20 18:03:04
	# todo fix only create tripdata for new trips
	create_tripdata(engine, newfilelist)
	up_end = timer()
	logger.info(f'timers readtime={timedelta(seconds=readend - readstart)} sendtime={timedelta(seconds=send_end - send_start)} fixtime={timedelta(seconds=fix_end - fix_start)} uptime={timedelta(seconds=up_end - up_start)}')


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
	t0 = datetime.now()
	main_start = timer()
	if args.checkcsv:
		errchk = checkcsv(args.path)
		print(f'err len:{len(errchk)}')
	else:
		asyncio.run(main(args))
	main_end = timer()
	timer1 = datetime.now() - t0
	timer2 = timedelta(seconds=main_end - main_start)
	td0 = timer1 - timer2
	logger.info(f'[main] done time: {timer1} / {timer2} / {td0} dbmode:{args.dbmode}')
