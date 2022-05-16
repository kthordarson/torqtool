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
from pandas import read_csv, Series, to_datetime, DataFrame
from sqlalchemy import create_engine, MetaData, select
from sqlalchemy.exc import IntegrityError, OperationalError, ProgrammingError, DataError
from psycopg2.errors import DatatypeMismatch
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import close_all_sessions
from sqlalchemy.pool import NullPool
from sqlalchemy_utils import database_exists, create_database

from datamodels import TorqEntry, TorqFile, TorqTrip, TorqLogEntry
from utils import get_csv_files

Base = declarative_base()
TORQDBHOST = 'elitedesk'  # os.getenv('TORQDBHOST')
TORQDBUSER = 'torq'  # os.getenv('TORQDBUSER')
TORQDBPASS = 'dzt3f5jCvMlbUvRG'
TORQDATABASE = 'torq9'
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
		# torq_trip.tripid = tripid
		# trip_profile = DataFrame([trip_profile])
		return torq_trip
	else:
		logger.warning(f'[p] {filename} len={len(pdata_)}')


def fixbuffer(buffer=None):  # , torqentryid=None):
	t0 = datetime.now()
	old_len = len(buffer)
	
	newbuff = DataFrame()
	try:
		buffer['GPSTime'] = to_datetime(buffer['GPS Time'], errors='raise', infer_datetime_format=True)
	except ParserError as e:
		logger.error(f'[fgpstime] err {e}')
		buffer['GPSTime'] = to_datetime(datetime.now(), errors='raise', infer_datetime_format=True)
	try:
		buffer['DeviceTime'] = to_datetime(buffer['Device Time'], errors='raise', infer_datetime_format=True)
	except ParserError as e:
		logger.error(f'[fDeviceTime] err {e}')
		buffer['DeviceTime'] = to_datetime(datetime.now(), errors='raise', infer_datetime_format=True)
	col_list = []
	for col in buffer.columns:
		for b in BADVALS:
			buffer[col].replace(to_replace=b, value=0, regex=True, inplace=True)
			#buffer = buffer.iloc[np.where(buffer[col].values!=b)]
			#indexNames = buffer[buffer[col] == b ].index
			#buffer.drop(indexNames , inplace=True)
			# logger.warning(f'[drop] {b} {col} {indexNames}')
			# buffer = buffer[col].drop(buffer[col] == b)
			# buffer[col].replace(to_replace=b, value=np.nan, regex=True, inplace=True)
	if len(buffer) < old_len:
		logger.warning(f'[fbd] bwdrop old:{old_len} new:{len(buffer)} ')
	new_len = len(buffer)
	if len(buffer) < new_len:
		logger.warning(f'[fbd] bwstrdrop {old_len} drops: {len(buffer)} ')
	for col in buffer.columns:
		newname_ = re.sub(r'\W', '', col)
		newname = newname_.encode('ascii', 'ignore').decode()
		col_list.append(newname)
	newbuff = buffer.set_axis(col_list, axis=1, inplace=False)
	logger.info(f'[fixb] done time: {(datetime.now() - t0).seconds} b:{len(buffer)} nb:{len(newbuff)}')
	return newbuff


def database_init(engine):
	close_all_sessions()
#	session_factory = sessionmaker(engine)
#	session_factory.close_all()
	meta = MetaData(engine)
	t1 = datetime.now()
	# tables = (TorqLogEntry.__table__, TorqEntry.__table__, TorqFile.__table__, TorqTrip.__table__)
	
	# for t in tables:
	# 	logger.info(f'dropping t: {t}')
	# 	try:
	# 		connection = engine.connect()
	# 		logger.info(f'dropping t: {t} conn: {connection}')
	# 		r = connection.execute(t.select())
	# 		logger.info(f'dropping t: {t} conn: {connection} r:{r}')
	# 		r.fetchone()
	# 		connection.close()
	# 		t.drop(engine)
	# 	except Exception as e:
	# 		logger.error(f'ERR {e}')
	try:
		logger.debug(f'[dbinit] {(datetime.now() - t1).total_seconds()} dropping from {meta}')
		Base.metadata.drop_all(bind=engine, tables=[TorqLogEntry.__table__, TorqEntry.__table__, TorqFile.__table__, TorqTrip.__table__], checkfirst=True)
	except (OperationalError, psycopg2.errors.UndefinedTable, sqlalchemy.exc.ProgrammingError) as e:
		logger.error(f'dropall {e}')
	try:
		Base.metadata.create_all(bind=engine, tables=[TorqLogEntry.__table__, TorqEntry.__table__, TorqFile.__table__, TorqTrip.__table__], checkfirst=False)
	except OperationalError as e:
		logger.error(f'metacreateall {e}')
	logger.debug(f'[dbinit] {(datetime.now() - t1).total_seconds()} done')


def chunks(l, n):
	"""Yield n number of sequential chunks from l."""
	d, r = divmod(len(l), n)
	for i in range(n):
		si = (d + 1) * (i if i < r else r) + d * (0 if i < r else i - r)
		yield l[si:si + (d + 1 if i < r else d)]


def read_and_send(args):
	csvhash = args['hash']
	csvfile = args['filename']
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

#	TORQDBHOST = 'elitedesk'
#	TORQDBUSER = 'torq'
#	TORQDBPASS = 'dzt3f5jCvMlbUvRG'
#	TORQDATABASE = 'torq9'
#	dburl = f"mysql+pymysql://{TORQDBUSER}:{TORQDBPASS}@{TORQDBHOST}/{TORQDATABASE}?charset=utf8mb4"  # &sessionVariables=sql_mode='NO_ENGINE_SUBSTITUTION'"
	#engine = create_engine(dburl, poolclass=NullPool)  # , isolation_level='AUTOCOMMIT')
	#engine = create_engine(f"postgresql://postgres:foobar9999@elitedesk/torq", max_overflow=0, pool_size=10)
	# tripid = str(csvfile.parts[-2])

	trip = read_torq_trip(csvfile)
	trip.hash = csvhash
	session.add(trip)
	session.commit()
	tfile = TorqFile()
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
	torqbuffer = read_csv(str(csvfile), delimiter=',', low_memory=False, skipinitialspace=True, thousands=',', keep_default_na=False, on_bad_lines='skip', encoding='utf-8', na_values=BADVALS)
	torqbuffer.fillna(0, inplace=True)
	buffer = fixbuffer(buffer=torqbuffer)
#	try:
	logger.info(f'[tb] {csvfile} b:{len(buffer)} tb:{len(torqbuffer)}')
	sbuffer = Series(data=[tfile.tripid for k in range(len(buffer))], dtype='object')
#	except ValueError as e:
#		logger.error(f'{csvfile} {e} {tfile.tripid} {type({tfile.tripid})}')
	# logger.info(f'[tbi] {csvfile} b:{len(buffer)} tb:{len(sbuffer)}')
	buffer['tripid'] = DataFrame(data=sbuffer, columns=['tripid'])

	sbuffer = Series(data=[tfile.torqfileid for k in range(len(buffer))], dtype='object')
	buffer['torqfileid'] = DataFrame(data=sbuffer, columns=['torqfileid'])
	# logger.info(f'[tt] {csvfile} b:{len(buffer)} tb:{len(sbuffer)}')

	logger.debug(f'[rs] sending c:{csvfile} size:{len(buffer)} tlid:{torqlogentry.torqentryid} tfid:{tfile.tripid}')
	t1 = datetime.now()
	try:
		# logger.info(f'[tosql] csv:{csvfile} b:{len(buffer)} ')
		buffer.to_sql(con=engine, name='torqlogs', if_exists='append', method='multi', chunksize=10000, index=False)
	except (DataError, OperationalError, IntegrityError, DatatypeMismatch, ProgrammingError, psycopg2.errors.DatatypeMismatch, psycopg2.errors.InvalidTextRepresentation, psycopg2.errors.InvalidTextRepresentation) as e:
		logger.error(f'csv:{csvfile} {e.code} {e.args[0]} tripid:{trip.tripid} profid:{trip.tripid} tfileid:{tfile.torqfileid}')
	except Exception as e:
		logger.error(f'csv:{csvfile} {e.code} {e.args[0]} {e} tripid:{trip.tripid} profid:{trip.tripid} tfileid:{tfile.torqfileid}')

	logger.debug(f'[rs] send done c:{csvfile} time: {(datetime.now() - t0).seconds} time: {(datetime.now() - t1).seconds} tripid:{trip.tripid} profid:{trip.tripid} tfileid:{tfile.torqfileid} ')  #
	return 1

def get_engine(args):
	if args == 'mysql':
		dburl = f"mysql+pymysql://{TORQDBUSER}:{TORQDBPASS}@{TORQDBHOST}/{TORQDATABASE}?charset=utf8mb4"
		return create_engine(dburl, pool_size=200, max_overflow=0)
	if args == 'postgres':
		return create_engine(f"postgresql://postgres:foobar9999@elitedesk/torq")


def main(args):
	t0 = datetime.now()
	
	if args.dbmode == 'mysql':
		dburl = f"mysql+pymysql://{TORQDBUSER}:{TORQDBPASS}@{TORQDBHOST}/{TORQDATABASE}?charset=utf8mb4"
		engine = create_engine(dburl, pool_size=200, max_overflow=0)
	elif args.dbmode == 'postgres':
		engine = create_engine(f"postgresql://postgres:foobar9999@elitedesk/torq")
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
	filelist = []
	csv_file_list = get_csv_files(searchpath=args.path)
	for idx, csv in enumerate(csv_file_list):
		csvhash = md5(open(csv, 'rb').read()).hexdigest()
		if csvhash in hashlist:
			logger.warning(f'[{csv}] already in database')
		else:
			filelist.append({'filename': csv, 'hash': csvhash, 'dbmode':args.dbmode})
	logger.debug(f'read start time: {(datetime.now() - t0).seconds} csv:{len(csv_file_list)} h:{len(hashlist)} fl:{len(filelist)}')
	with ProcessPoolExecutor(max_workers=maxworkers) as ex:
		#result = ex.map(read_and_send, filelist, [args])
		#result = ex.map(read_and_send, filelist)
		#for output in result:
		#	logger.debug(f'[z] c:{result} r:{output} f:{filelist}')
		for csv, res in zip(filelist, ex.map(read_and_send, filelist)):
			logger.debug(f'[z] c:{csv} r:{res}')
	logger.debug(f'torqtask done time: {(datetime.now() - t0).seconds} start:{t0} end:{datetime.now()} duration:{datetime.now() - t0}')


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
