import argparse
import os
import re
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
from sqlalchemy import create_engine, MetaData, select, update, DDL, Column, String, Table
from sqlalchemy.exc import IntegrityError, OperationalError, ProgrammingError, DataError
from psycopg2.errors import DatatypeMismatch
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import close_all_sessions
from sqlalchemy.pool import NullPool
from sqlalchemy_utils import database_exists, create_database

from datamodels import TorqEntry, TorqFile, TorqTrip, TorqLogEntry
from utils import get_csv_files, size_format, database_init, clean_files

Base = declarative_base()
TORQDBHOST = 'elitedesk'
TORQDBUSER = 'torq'
TORQDBPASS = 'dzt3f5jCvMlbUvRG'
TORQDATABASE = 'torqfiskur'
BADVALS = ['NaN', '-','∞','â', r'∞',340282346638528860000000000000000000000,'340282346638528860000000000000000000000',612508207723425200000000000000000000000,'612508207723425200000000000000000000000']

def read_torq_trip(filename):
	p_filename = os.path.join(filename.parent, 'profile.properties')
	with open(p_filename, 'r') as f:
		pdata_ = f.readlines()
	if len(pdata_) == 8:
		pdata = [l.strip('\n') for l in pdata_ if not l.startswith('#')]
		try:
			pdata_date = str(pdata_[1][1:]).strip('\n')
			tripdate = to_datetime(pdata_date).to_pydatetime()
		except (OperationalError, Exception) as e:
			logger.error(f'[readsend] {e}')
			tripdate = None
		trip_profile = dict([k.split('=') for k in pdata])
		torq_trip = TorqTrip()
		torq_trip.fuelCost = float(trip_profile['fuelCost'])
		torq_trip.fuelUsed = float(trip_profile['fuelUsed'])
		torq_trip.distanceWhilstConnectedToOBD = float(trip_profile['distanceWhilstConnectedToOBD'])
		torq_trip.distance = float(trip_profile['distance'])
		torq_trip.time = float(trip_profile['time'])
		torq_trip.filename = p_filename
		torq_trip.tripdate = tripdate
		torq_trip.profile = trip_profile['profile']
		return torq_trip
	else:
		logger.warning(f'[p] {filename} len={len(pdata_)}')


def fixbuffer(buffer=None):
	t0 = datetime.now()
	old_len = len(buffer)
	
	# newbuff = DataFrame()
	# newbuff = TorqEntry()
	# bcol_list = get_col_list(buffer)
	ser_list = []
	# s0 = Series
	# b1=DataFrame(buffer,columns=[b['name'] for b in bcol_list], index=[k for k in range(len(buffer))])
	# b1=DataFrame()
	# newbuff = DataFrame()
	frames = []
	for col in buffer: # :.columns:
		newname_ = col
		newname_ = re.sub(r'\W', '', col)
		newname = newname_.encode('ascii', 'ignore').decode()
		#s1=Series(buffer[col].values)
		s2=DataFrame(Series(buffer[col].values), index=buffer[col].index, columns=[newname])
		frames.append(s2)

		#newbuff.add(Series(buffer[col],name=newname))
	newbuff = concat(frames)
	return newbuff
		#buffer[col].rename(newname, inplace=True)
		#buffer[col].index.name = 'id'		
#		b1 = concat([b1, buffer[col]])

	# for b in col_list:
	# 	s1 = Series(b['newcol'])
	# 	# s1.set_axis(s1, inplace=True)
	# 	ser_list.append(s1)
	#ser_list = [Series(k['newcol']) for k in col_list]
	#s2 = DataFrame(ser_list)
	
	# newbuff = DataFrame(columns=[k['name'] for k in col_list], data=[k['newcol'] for k in col_list])
	# newbuff = {} # DataFrame()
	# for col in col_list:
	# 	newser = Series(col['newcol'])
	# 	newbuff[col['name']] = newser
	# 	#newcol = buffer[col['oldname']]		
	# 	#newbuff[newser] = buffer[col].index
	# return newbuff
	
	# for col in buffer.columns:
	# 	newname_ = col
	# 	newname_ = re.sub(r'\W', '', col)
	# 	newname = newname_.encode('ascii', 'ignore').decode()
#		for b in BADVALS:
#			buffer[col].replace(to_replace=b, value=0, regex=True, inplace=True)
#			newcol = buffer[col]
#			newcol.index.name = 'id'
			

def get_col_list(buffer):
	col_list = []
	for col in buffer: # :.columns:
		newname_ = col
		newname_ = re.sub(r'\W', '', col)
		newname = newname_.encode('ascii', 'ignore').decode()
		buffer[col].name = newname
		buffer[col].index.name = 'id'		
		#newcol.name = newname
		#col_list.append({'name':newname, 'newcol': newcol, 'oldname':col})
		#logger.info(f'[gcl] newcol:{newcol.name} col:{col} newname:{newname} c:{len(col_list)} b:{len(buffer)}')
	return buffer
	#newbuff = DataFrame(columns=[c for c in col_list])
	#newbuff.set_axis(col_list, axis=1, inplace=True)
# Incorrect datetime value: 'Fri Dec 10 18:31:58 GMT+01:00 2021' for column `torqfiskur`.`torqlogs`.`GPSTime` at row 1") tripid:2 profid:

def get_col_names(buffer):
	name_list = []
	for col in buffer: # :.columns:
		newname_ = col
		newname_ = re.sub(r'\W', '', col)
		newname = newname_.encode('ascii', 'ignore').decode()
		name_list.append({'name':newname, 'oldname':col})
	return name_list

def fixdates(buffer):
	try:
		GPSTime = to_datetime(buffer['GPSTime'], errors='ignore', infer_datetime_format=False)
		buffer['GPSTime'] = GPSTime
	except (ParserError, KeyError) as e:
		logger.warning(f'[fgpstime] err {e}')# {buffer.__dict__}')
		#newbuff.GPSTime = to_datetime(buffer['GPS Time'], format="%a %b %m %H:%M:%S %Z %Y", errors='ignore', infer_datetime_format=False)
		# raw_data['Mycol'] =  pd.to_datetime(raw_data['Mycol'], format='%d%b%Y:%H:%M:%S.%f')
		# buffer['GPSTime'] = to_datetime(datetime.now(), errors='raise', infer_datetime_format=True)
	try:
		DeviceTime = to_datetime(buffer['DeviceTime'], errors='ignore', infer_datetime_format=False)
		buffer['DeviceTime'] = DeviceTime
	except (ParserError, KeyError) as e:
		logger.warning(f'[fDeviceTime] err {e}')
		#newbuff.DeviceTime = to_datetime(buffer['Device Time'], format="%a %b %m %H:%M:%S %Z %Y", errors='ignore', infer_datetime_format=False)
		# newbuff['DeviceTime'] = to_datetime(datetime.now(), errors='ignore', infer_datetime_format=True)
	logger.info(f'[fixb] done time: {(datetime.now() - t0).seconds} b:{len(buffer)} ')
#	torqentry = TorqEntry()
#	for k in torqentry.__mapper__.tables[0].columns:
#		torqentry.__dict__[k] = newbuff[k]
	# [k for k in torqentry.__mapper__.tables[0].columns]
	return buffer



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
	logger.info(f'[rs] args:{args} {type(args)} h:{csvhash} e:{engine} s:{session} S:{Session}')

	r0 = datetime.now()
	# torqbuffer = read_csv(str(csvfile), delimiter=',', low_memory=False, skipinitialspace=True, thousands=',', keep_default_na=False, on_bad_lines='skip', encoding='utf-8', na_values=BADVALS, index_col=False)
	torqbuffer = read_csv(args['csvfilefixed'], delimiter=',', low_memory=False, skipinitialspace=True, na_values=BADVALS, keep_default_na=False)
	# torqbuffer.fillna(0, inplace=True)

	readtime = (datetime.now() - r0).microseconds

	r2 = datetime.now()
	torqentry = TorqEntry(buffer=torqbuffer)
	
	Base.metadata.create_all(bind=engine,tables=[torqentry.__table__])
	session.add(torqentry)
	try:
		session.commit()
	except OperationalError as e:
		logger.error(f'torqentry err csv:{csvfile} {e.code} {e.args[0]} tetable: {TorqEntry.__table__} temap: {TorqEntry.__mapper__}')
		session.rollback()
	# torqentry.setdata(buffer)

	fixtime = (datetime.now() - r2).microseconds

	trip = read_torq_trip(csvfile)
	trip.hash = csvhash
	session.add(trip)
	session.commit()
	tfile = TorqFile()
	tfile.read_time = readtime
	tfile.fix_time = fixtime
	tfile.hash = csvhash
	tfile.torqfilename = str(csvfile)
	tfile.tripid = trip.tripid
	tfile.profile = trip.profile
	session.add(tfile)
	session.commit()
	torqlogentry = TorqLogEntry()
	torqlogentry.tripid = tfile.tripid
	torqlogentry.torqfileid = tfile.torqfileid
	session.add(torqlogentry)
	session.commit()

	#sbuffer = Series(data=[k for k in range(len(buffer))], dtype='int')
	# buffer['id'] = DataFrame(data=[k for k in range(len(buffer))], dtype='int')

	#sbuffer = Series(data=[tfile.tripid for k in range(len(buffer))], dtype='int')
	#buffer['tripid'] = DataFrame(data=[sbuffer], dtype='int')

	#sbuffer = Series(data=[tfile.torqfileid for k in range(len(buffer))], dtype='int')
	#buffer['torqfileid'] = DataFrame(data=[sbuffer], dtype='int')
	# logger.info(f'[tt] {csvfile} b:{len(buffer)} tb:{len(sbuffer)}')

	logger.debug(f'[rs] sending c:{csvfile} tlid:{torqlogentry.torqlog_entry} tfid:{tfile.tripid}')
	t1 = datetime.now()
	# sqlbuffer = read_sql_query(con=engine, sql=buffer)
	# cols = buffer.keys()
	logger.info(f'[tosql] csv:{csvfile} b:{len(torqbuffer)} ')
	#buffer.index = [Index([k for k in range(len(buffer))])]
	# sqlbuffer['index'] = range(1, len(buffer) + 1)
	
	s0 = datetime.now()
	try:
		sendtime = (datetime.now() - s0).microseconds
	except (sqlalchemy.exc.DataError, pymysql.err.DataError, DataError, OperationalError, IntegrityError, DatatypeMismatch, ProgrammingError, psycopg2.errors.DatatypeMismatch, psycopg2.errors.InvalidTextRepresentation, psycopg2.errors.InvalidTextRepresentation) as e:
		logger.error(f'tosqlerr csv:{csvfile} {e.code} {e.args[0]} ')
	except ValueError as e:
		logger.error(f'csv:{csvfile} {e}')
	except Exception as e:
		logger.error(f'csv:{csvfile} {e}')
	logger.debug(f'[rs] send done c:{csvfile} time: {(datetime.now() - t0).seconds} time: {(datetime.now() - t1).seconds} tripid:{trip.tripid} profid:{trip.tripid} tfileid:{tfile.torqfileid} ')  #
	session.flush()
	return 1

def get_engine(args):
	if args == 'mysql':
		dburl = f"mysql+pymysql://{TORQDBUSER}:{TORQDBPASS}@{TORQDBHOST}/{TORQDATABASE}?charset=utf8mb4"
		return create_engine(dburl, pool_size=200, max_overflow=0)
	if args == 'postgres':
		return create_engine(f"postgresql://postgres:foobar9999@elitedesk/torqfiskur")

def main(args):
	t0 = datetime.now()
	
	if args.dbmode == 'mysql':
		dburl = f"mysql+pymysql://{TORQDBUSER}:{TORQDBPASS}@{TORQDBHOST}/{TORQDATABASE}?charset=utf8mb4"
		engine = create_engine(dburl, pool_size=200, max_overflow=0)
	elif args.dbmode == 'postgres':
		engine = create_engine(f"postgresql://postgres:foobar9999@elitedesk/torqfiskur")
	else:
		engine = None
	Session = sessionmaker(bind=engine)
	session = Session()
	session.autoflush = True

	maxworkers = cpu_count()
	if args.init_db:
		logger.debug(f'[mainpath] Calling init_db ... ')
		database_init(engine)

	hashres = session.execute(select(TorqFile)).fetchall()
	hashlist = [k[0].hash for k in hashres]
	# filelist = []
	filelist_ = get_csv_files(searchpath=args.path, dbmode=args.dbmode)
	# cleanfilelist = clean_files(filelist_)	#filelist = sorted(filelist, key=lambda d: d['size'])
	filelist = [k for k in reversed(sorted(filelist_, key=lambda d: d['size']) ) if k['hash'] not in hashlist]
	totalbytes = sum([k['size'] for k in filelist])
	# for idx, csv in enumerate(csv_file_list):
	# 	csvhash = md5(open(csv, 'rb').read()).hexdigest()
	# 	if csvhash in hashlist:
	# 		logger.warning(f'[{csv}] already in database')
	# 	else:
	# 		filelist.append({'filename': csv, 'hash': csvhash, 'dbmode':args.dbmode})
	# filelist=[({'filename':k, 'size':os.stat(k).st_size}) for k in csv_file_list]

	logger.debug(f'read start time: {(datetime.now() - t0).seconds}  h:{len(hashlist)} fl:{len(filelist)} tb:{size_format(totalbytes)}')
	pp_counter = 0
	with ProcessPoolExecutor(max_workers=maxworkers) as ex:
		for csv, res in zip(filelist, ex.map(read_and_send, filelist)):
			pp_counter += 1
			totalbytes -= csv['size']
			logger.debug(f'[PP] c:{csv["csvfilename"]} h:{csv["hash"]} r:{res} ppc:{pp_counter} fl:{len(filelist)} remaining:{len(filelist)-pp_counter} tb:{size_format(totalbytes)} tb:{totalbytes}' )
	logger.debug(f'torqtask done time: {(datetime.now() - t0).seconds} start:{t0} end:{datetime.now()} duration:{datetime.now() - t0} mw:{maxworkers} tb:{size_format(totalbytes)}')


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
