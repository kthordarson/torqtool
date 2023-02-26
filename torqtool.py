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
from datamap import entry_datamap
from datamodels import get_trip_profile, prepdb, send_torq_trip, database_init, sqlite_db_init, send_trip_profile
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


async def async_read_buff(tf):
	start = timer()
	csvhash = tf.fixedhash
	csvfilefixed = tf.csvfilefixed
	#tripid = tf.tripid
	datefields = ['gpstime', 'devicetime']
	torqbuffer = read_csv(csvfilefixed, delimiter=',', na_values=BADVALS, low_memory=False, parse_dates=datefields, converters={'gpstime': convert_datetime}, dtype=entry_datamap)
	torqbuffer.fillna(0, inplace=True)
	#torqbuffer.insert(1, "tripid", [tripid for k in range(len(torqbuffer))])
	end = timer()
	return torqbuffer

def read_buff(tf_csvfile, tf_fileid):
	start = timer()
	# csvhash = tf.fixedhash
	fileid = tf_fileid
	csvfilefixed = tf_csvfile
	datefields = ['gpstime', 'devicetime']
	torqbuffer = read_csv(csvfilefixed, delimiter=',', na_values=BADVALS, low_memory=False, dtype=entry_datamap)
	torqbuffer.fillna(0, inplace=True)
	torqbuffer.insert(1, "fileid", [fileid for k in range(len(torqbuffer))])
	end = timer()
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

async def async_sqlsender(buffer=None, con=None, chsize=None, dburl=None):
	con = create_engine(dburl)
	try:
		buffer.to_sql('torqlogs', con=con, if_exists='append', index=False)
	except (OperationalError, ProgrammingError) as e:
		logger.warning(f'[tosql] code={e.code} args={e.args[0]}')  # error:{e}
	except InternalError as e:
		logger.error(e)
	except (InvalidTextRepresentation,IntegrityError) as e:
		logger.warning(f'[tosql] {type(e)} code={e.code} args={e.args[0]}')
		# logger.warning(f'[tosql] {e.statement} {e.params}')
		# logger.warning(f'[tosql] {e}')
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

def sqlsender(buffer=None, session=None):
	try:
		buffer.to_sql('torqlogs', con=session, if_exists='append', index=False)
	except (OperationalError, ProgrammingError) as e:
		logger.error(f'[tosql] code={e.code} args={e.args[0]}')  # error:{e}
	except InternalError as e:
		logger.error(e)
	except (InvalidTextRepresentation,IntegrityError) as e:
		logger.warning(f'[tosql] {type(e)} code={e.code} args={e.args[0]}')
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
	except TypeError as e:
		errmsg = e.args[0]
		err_row = errmsg.split('row')[-1].strip()
		logger.error(f'[tosql] code:{e.code} err:{errmsg} row:{err_row} {buffer.iloc[err_row]}')


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


def read_process(newfilelist):
	maxworkers = cpu_count()
	buffs = []
	read_res = []
	with ProcessPoolExecutor(max_workers=maxworkers) as executor:
		read_task_start = timer()
		for tf in newfilelist:
			buffs.append(executor.submit(read_buff, tf.csvfilefixed, tf.id))
		read_task_end = timer()
		logger.debug(f'buffs:{len(buffs)} t={timedelta(seconds=read_task_end - read_task_start)}')
	for res in as_completed(buffs):
		try:
			read_res.append(res.result())
		except DetachedInstanceError as e:
			logger.error(e)
	# buffer = [b for b in buffs]
	return read_res

def send_process(buffs, session):
	maxworkers = cpu_count()
	sendtasks = []
	sendres = []
	with ProcessPoolExecutor(max_workers=maxworkers) as executor:
		for idx, tchunk in enumerate(buffs):
			sendtasks.append(executor.submit(sqlsender, tchunk, session))
			logger.debug(f'idx={idx} tc={type(tchunk)} sendtasks = {len(sendtasks)}')
	for res in as_completed(sendtasks):
		sendres.append(res.result())
	return sendres


def main(args):

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
		session.execute(text('PRAGMA foreign_keys=OFF'))
		sqlite_db_init(engine)
	if args.init_db:
		try:
			database_init(session, engine)
		except OperationalError as e:
			logger.error(f'[prepdb] {e}')

	filelist_ = get_csv_files(searchpath=Path(args.path), dbmode=args.dbmode)
	# filelist = [k for k in reversed(sorted(filelist_, key=lambda d: d['fixedsize']) )]
	filelist = [k for k in sorted(filelist_, key=lambda d: d['csvtimestamp'])]
	newfilelist = []
	newfilelist = prepdb(filelist, session)
	if newfilelist is None:
		logger.warning(f'[main]	prepdb returned None')
		sys.exit(1)
	elif len(newfilelist) == 0:
		logger.warning(f'[main]	0 files from prepdb....')
		sys.exit(1)
	else:
		readend = timer()
		readstart = timer()
		buffs = read_process(newfilelist)
		logger.debug(f'[read] done time={timedelta(seconds=readend - readstart)} buffs:{len(buffs)}')
		send_start = timer()
		send_results = send_process(buffs, dburl)
		send_end = timer()
		fix_start = timer()
		fix_end = timer()
		up_start = timer()
		create_tripdata(engine, session, newfilelist)
		up_end = timer()
		logger.info(f'timers readtime={timedelta(seconds=readend - readstart)} sendtime={timedelta(seconds=send_end - send_start)} fixtime={timedelta(seconds=fix_end - fix_start)} uptime={timedelta(seconds=up_end - up_start)} send_results={len(send_results)}')


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
		main(args)
		#asyncio.run(main(args))
	main_end = timer()
	timer1 = datetime.now() - t0
	timer2 = timedelta(seconds=main_end - main_start)
	td0 = timer1 - timer2
	logger.info(f'[main] done time: {timer1} / {timer2} / {td0} dbmode:{args.dbmode}')
