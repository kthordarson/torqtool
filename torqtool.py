import argparse
import os
import re
from attr import attr
from dateutil.parser._parser import ParserError
import numpy as np
import pymysql
import sqlalchemy
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from hashlib import md5
from multiprocessing import cpu_count
import psycopg2
from loguru import logger
from pandas import read_csv, Series, to_datetime, DataFrame, read_sql_query, Index, concat, io
from sqlalchemy import create_engine, MetaData, select, update, DDL, Column, String, Table, Integer, Float
from sqlalchemy.exc import IntegrityError, OperationalError, ProgrammingError, DataError, CompileError
from psycopg2.errors import DatatypeMismatch
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import close_all_sessions
from sqlalchemy.pool import NullPool
from sqlalchemy_utils import database_exists, create_database

from datamodels import TorqFile, TorqTrip, TorqLogEntry
from utils import get_csv_files, size_format, database_init
# from utils import get_col_list, get_col_names, fixbuffer, fixdates
from utils import read_torq_trip

Base = declarative_base()
TORQDBHOST = 'elitedesk'
TORQDBUSER = 'torq'
TORQDBPASS = 'dzt3f5jCvMlbUvRG'
TORQDATABASE = 'torqfiskur'
BADVALS = ['NaN', '-','∞','â', r'∞',340282346638528860000000000000000000000,'340282346638528860000000000000000000000',612508207723425200000000000000000000000,'612508207723425200000000000000000000000']

def prepdb(filelist=None, engine=None, dbinit=False):
	#for tf in filelist:
	#	torqentry_attr_dict[nc] = Column(name=nc)
	Session = sessionmaker(bind=engine)
	session = Session()
	
	Base = declarative_base()
	logger.info(f'[dbprep] f:{len(filelist)}')
	torqentry_attr_dict = {
			'__tablename__': 'entries',
			'__table_args__' 'extend_existing': True,
			'idx': Column(Integer, primary_key=True, autoincrement='true', unique=False)
		}
	col_list = []
	for f in filelist:
		fc = f['newcolumns'].split(',')
		[col_list.append(c) for c in fc if c not in col_list]
	for nc in col_list:	
		torqentry_attr_dict[nc] = Column(name=nc, type_=String(255), default=0)
	# torqentry_attr_dict['index'] = Column(Integer, primary_key=True, autoincrement='true', unique=True)
	torqentry_attr_dict['Averagetripspeedwhilstmovingonlykmh'] = Column('Averagetripspeedwhilstmovingonlykmh',Float, default=0)
	torqentry_attr_dict['EnginekWAtthewheelskW'] = Column('EnginekWAtthewheelskW', Float, default=0)
	torqentry_attr_dict['SpeedGPSkmh'] = Column('SpeedGPSkmh', Float, default=0)
	# SpeedGPSkmh
	# Averagetripspeedwhilstmovingonlykmh
	# EnginekWAtthewheelskW
	TorqEntry = type('TorqEntry', (Base,), torqentry_attr_dict)
	torqentry = TorqEntry()
	# Base.metadata.create_all(bind=engine,tables=[torqentry.__table__])
	if dbinit:
		logger.info(f'[dbprep] dropping')
		session.commit()
		close_all_sessions()
		#session.drop_all()
		#session.create_all()
		Base.metadata.drop_all(bind=engine, tables=[TorqLogEntry.__table__, TorqEntry.__table__, TorqFile.__table__, TorqTrip.__table__], checkfirst=True)
		logger.info(f'[dbprep] drop done')
		Base.metadata.create_all(bind=engine, tables=[TorqLogEntry.__table__, TorqEntry.__table__, TorqFile.__table__, TorqTrip.__table__], checkfirst=False)
		logger.info(f'[dbprep] create done')
	# logger.info(f'[rs] args:{args} {type(args)} h:{csvhash} e:{engine} s:{session} S:{Session}')

def read_buffers(filelist):
	bigbuffer = []
	for tf in filelist:
		csvhash = tf['hash']
		csvfile = tf['csvfilefixed']

		dbmode = tf['dbmode']
		engine = None
		try:
			engine = get_engine(dbmode)
		except AttributeError as e:
			logger.error(f'[rs] err {e} {tf}')
		torqbuffer = read_csv(csvfile, delimiter=',', low_memory=False, skipinitialspace=True, na_values=BADVALS, keep_default_na=False, index_col=0)
		bigbuffer.append(torqbuffer)
	return bigbuffer

def read_and_send(args):
	csvhash = args['hash']
	csvfile = args['csvfilefixed']

	dbmode = args['dbmode']
	engine = None
	try:
		engine = get_engine(dbmode)
	except AttributeError as e:
		logger.error(f'[rs] err {e} {args}')
	Session = sessionmaker(bind=engine)
	session = Session()
	session.autoflush = False
	session.autocommit = False
	# session.expires_on_commit = False
	t0 = datetime.now()
	# logger.info(f'[rs] args:{args} {type(args)} h:{csvhash} e:{engine} s:{session} S:{Session}')
	r0 = datetime.now()
	# torqbuffer = read_csv(str(csvfile), delimiter=',', low_memory=False, skipinitialspace=True, thousands=',', keep_default_na=False, on_bad_lines='skip', encoding='utf-8', na_values=BADVALS, index_col=False)
	torqbuffer = read_csv(csvfile, delimiter=',', low_memory=False, skipinitialspace=True, na_values=BADVALS, keep_default_na=False, index_col=False)
	kcols = [k for k in torqbuffer.columns]
	logger.info(f'[rs] csv:{csvfile} tb:{len(torqbuffer)} tbc:{len(torqbuffer.columns)} kc:{len(kcols)}')
	# torqbuffer.fillna(0, inplace=True)
	readtime = (datetime.now() - r0).microseconds
	r2 = datetime.now()

	fixtime = (datetime.now() - r2).microseconds

	trip = read_torq_trip(csvfile)
	trip.hash = csvhash
	session.add(trip)
	try:
		session.commit()
	except ProgrammingError as e:
		logger.error(f'[trip] err {e}')
		session.rollback()

	tfile = TorqFile()
	tfile.read_time = readtime
	tfile.fix_time = fixtime
	tfile.hash = csvhash
	tfile.torqfilename = str(csvfile)
	tfile.tripid = trip.tripid
	tfile.profile = trip.profile
	session.add(tfile)
	try:
		session.commit()
	except ProgrammingError as e:
		logger.error(f'[tfile] err {e}')
		session.rollback()

	t1 = datetime.now()
	# sqlbuffer = read_sql_query(con=engine, sql=buffer)
	# cols = buffer.keys()
	logger.info(f'[tosql] csv:{csvfile} b:{len(torqbuffer)} ')
	#buffer.index = [Index([k for k in range(len(buffer))])]
	# sqlbuffer['index'] = range(1, len(buffer) + 1)
	
	s0 = datetime.now()
	#newindex = DataFrame([k for k in range(len(torqbuffer))])
	#torqbuffer.insert(0, "id", newindex)
	#torqbuffer = torqbuffer.set_index('id')

	try:
		logger.debug(f'[rs] send start c:{csvfile} t:{(datetime.now() - t0).seconds} ts:{(datetime.now() - t1).seconds} ')
		#torqbuffer.to_sql('entries',con=engine, if_exists='append',method='multi', chunksize=10000)
		sendtime = (datetime.now() - s0).microseconds
		logger.debug(f'[rs] send done c:{csvfile} t:{(datetime.now() - t0).seconds} ts:{(datetime.now() - t1).seconds} st:{sendtime}  tripid:{trip.tripid} profid:{trip.tripid} tfileid:{tfile.torqfileid} ')
		session.flush()
	except (sqlalchemy.exc.DataError, pymysql.err.DataError, DataError, OperationalError, IntegrityError, DatatypeMismatch, ProgrammingError, psycopg2.errors.DatatypeMismatch, psycopg2.errors.InvalidTextRepresentation, psycopg2.errors.InvalidTextRepresentation) as e:
		logger.error(f'tosqlerr csv:{csvfile} {e.code} {e.args[0]} ')
	except ValueError as e:
		logger.error(f'csv:{csvfile} {e}')
	except Exception as e:
		logger.error(f'csv:{csvfile} {e}')
	return 1

def get_engine(args):
	if args == 'mysql':
		dburl = f"mysql+pymysql://{TORQDBUSER}:{TORQDBPASS}@{TORQDBHOST}/{TORQDATABASE}?charset=utf8mb4"
		return create_engine(dburl, pool_size=200, max_overflow=0)
	if args == 'postgres':
		return create_engine(f"postgresql://postgres:foobar9999@elitedesk/torqfiskur")

def chunks(l, n):
	for i in range(0, len(l), n):
         yield l.iloc[i:i+n]

def main(args):
	t0 = datetime.now()
	
	if args.dbmode == 'mysql':
		dburl = f"mysql+pymysql://{TORQDBUSER}:{TORQDBPASS}@{TORQDBHOST}/{TORQDATABASE}?charset=utf8mb4"
		engine = create_engine(dburl, pool_size=200, max_overflow=0)
	elif args.dbmode == 'postgres':
		engine = create_engine(f"postgresql://postgres:foobar9999@elitedesk/torqfiskur")
	else:
		engine = None
	session = sessionmaker(bind=engine)()
	#session = Session()
	#session.autoflush = True

	maxworkers = cpu_count()
	#if args.init_db:
	#	logger.debug(f'[mainpath] Calling init_db ... ')
	#	database_init(engine)

	filelist_ = get_csv_files(searchpath=args.path, dbmode=args.dbmode)
	# cleanfilelist = clean_files(filelist_)	#filelist = sorted(filelist, key=lambda d: d['size'])
	#filelist = [k for k in reversed(sorted(filelist_, key=lambda d: d['size']) ) if k['hash'] not in hashlist]
	filelist = [k for k in reversed(sorted(filelist_, key=lambda d: d['size']) )]
	try:
		hashres = session.execute(select(TorqFile)).fetchall()
		hashlist = [k[0].hash for k in hashres]
	except ProgrammingError as e:
		logger.error(f'[pe] hashlisterr {e}')
		hashlist = []
	
	# filelist = []
	
	prepdb(filelist, engine, args.init_db)
	totalbytes = sum([k['size'] for k in filelist])
	buffer = read_buffers(filelist)
	mb=concat([b for b in buffer])
	newindex = DataFrame([k for k in range(len(mb))])
	mb.insert(0, "idx", newindex)
	mb = mb.set_index('idx')
	dbmethod = None
	# dbmethod = 'multi'
	# chsize = int(totalbytes/len(mb))
	# chsize = int(totalbytes/len(buffer))
	# chsize = len(buffer)
	chsize = (int(len(mb)/len(buffer)))
	try:
		for idx, tchunk in enumerate(chunks(mb, chsize)):
			logger.info(f'[b] idx:{idx} tosql b:{len(buffer)} mb:{len(mb)} tb:{totalbytes} dbm:{dbmethod} chs:{chsize}')
		# mb.to_sql('entries',con=engine, if_exists='append',method='multi', chunksize=10000)
			tchunk.to_sql('entries',con=engine, if_exists='append', method=dbmethod, chunksize=chsize)
	except Exception as e:
		logger.error(f'[tosql] {e.code} {e.args[0]} ')
	# for idx, csv in enumerate(csv_file_list):
	# 	csvhash = md5(open(csv, 'rb').read()).hexdigest()
	# 	if csvhash in hashlist:
	# 		logger.warning(f'[{csv}] already in database')
	# 	else:
	# 		filelist.append({'filename': csv, 'hash': csvhash, 'dbmode':args.dbmode})
	# filelist=[({'filename':k, 'size':os.stat(k).st_size}) for k in csv_file_list]

	# logger.debug(f'read start time: {(datetime.now() - t0).seconds}  h:{len(hashlist)} fl:{len(filelist)} tb:{size_format(totalbytes)}')
	# pp_counter = 0
	# with ProcessPoolExecutor(max_workers=maxworkers) as ex:
	# 	for csv, res in zip(filelist, ex.map(read_and_send, filelist)):
	# 		pp_counter += 1
	# 		totalbytes -= csv['size']
	# 		logger.debug(f'[PP] c:{csv["csvfilename"]} h:{csv["hash"]} r:{res} ppc:{pp_counter} fl:{len(filelist)} remaining:{len(filelist)-pp_counter} tb:{size_format(totalbytes)} tb:{totalbytes}' )
	# logger.debug(f'torqtask done time: {(datetime.now() - t0).seconds} start:{t0} end:{datetime.now()} duration:{datetime.now() - t0} mw:{maxworkers} tb:{size_format(totalbytes)}')


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
	parser.add_argument("--dbmode", default="mysql", help="sqlmode mysql/postgres", action="store")
	args = parser.parse_args()
	t0 = datetime.now()
	main(args)
	logger.info(f'[main] done time: {datetime.now() - t0} dbmode:{args.dbmode}')
