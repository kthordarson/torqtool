from multiprocessing import cpu_count

import os
import re
from concurrent.futures import ProcessPoolExecutor
from functools import wraps
import argparse
import functools
import asyncio
from pathlib import Path
from pandas import read_csv, DataFrame, to_datetime, Series
from numpy import nan
from datetime import datetime
from dateutil.parser import ParserError
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.exc import OperationalError, ProgrammingError, InternalError, NoReferencedTableError, ResourceClosedError, PendingRollbackError, IntegrityError
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, inspect, select, Numeric, DateTime, text, BIGINT, BigInteger, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import OperationalError, DataError
import sqlalchemy.pool as pool
from hashlib import md5
# from utils import read_csv_columns_raw, column_fixer, FIELDMAPS, badvals, badvals_str, get_csv_files, Torqlog, Torqfile, TripProfile
from utils import get_csv_files, torqbuffer_fixer, read_torq_profile
from datamodels import TorqEntry, TorqFile, TorqProfile
Base = declarative_base()

from loguru import logger

def fixbuffer(buffer=None, metadata=None, profile=None, csvfile=None, csvhash=None, tripid_=None, fileid=None):
	t0 = datetime.now()
	logger.info(f'[fixb] b:{len(buffer)} c:{csvfile} ')# h:{csvhash} t:{tripid_} {type(tripid_)} p:{profile}')
	buffer['filename'] = Series(data=[f'{csvfile}' for k in range(len(buffer))])
	buffer['hash'] = Series(data=[f'{csvhash}' for k in range(len(buffer))])
	buffer['file_id'] = Series(data=[f'{fileid}' for k in range(len(buffer))])
	buffer['tripid'] = Series(data=[f'{tripid_}' for k in range(len(buffer))])
	buffer['profile'] = Series(data=[f'{profile}' for k in range(len(buffer))])
	badvals_str = ['9999999999', '-9999999999', '3.402823466385289e+38', '-3402823534620772000000000000000000000','-3.402823534620772e+36','-3.4028236187100775e+36','-3.402823618710077e+36','-3402823618710077500000000000000000000','612508207723425200000000000000000000000','612508207723425231880386882817669201920','340282346638528860000000000000000000000']
	badvals = [9999999999, -9999999999,3.402823466385289e+38, -3402823534620772000000000000000000000,-3.402823534620772e+36,-3.4028236187100775e+36,-3.402823618710077e+36,-3402823618710077500000000000000000000,612508207723425200000000000000000000000,612508207723425231880386882817669201920,340282346638528860000000000000000000000]

	for col in buffer.columns:
		for b in badvals:
			buffer[col].replace(to_replace=b, value=0, inplace=True, regex=True)	
		for b in badvals_str:
			buffer[col].replace(to_replace=b, value=0, inplace=True, regex=True)	
		newname_ = re.sub('\W', '', col) #.encode('ascii', 'ignore')
		newname = newname_.encode('ascii','ignore').decode()
		buffer.rename(columns={col:newname}, inplace=True)
		tempbuffer = buffer[newname].replace(nan, 0) # .transpose()
		buffer[newname] = tempbuffer.transpose()
		# logger.info(f'[fixb] c:{col} n:{newname} time: {datetime.now() - t0}')
		# buffer[newname].replace(to_replace=340282346638528860000000000000000000000, value=0, inplace=True, regex=True)
		# buffer[newname].replace(to_replace='340282346638528860000000000000000000000', value=0, inplace=True)
		# buffer[newname].replace('340282346638528860000000000000000000000',0,inplace=True)
		# buffer[newname].replace(340282346638528860000000000000000000000,0,inplace=True)
		# buffer[newname].replace('-9999999999',0,inplace=True)
		# buffer[newname].replace(9999999999,0,inplace=True)
		# tempbuffer = tempbuffer.transpose()
	logger.info(f'[fixb] done time: {(datetime.now() - t0).seconds} b:{len(buffer)} c:{csvfile} ')# 
	return buffer


def check_threads(threads):
	return True in [t.is_alive() for t in threads]

def database_init(engine):
	meta = MetaData(engine)
	t1 = datetime.now()
	logger.debug(f'[dbinit] {(datetime.now() - t1).total_seconds()} dropping from {meta}')
	tables=(TorqEntry.__table__, TorqFile.__table__, TorqProfile.__table__)
	try:
		for t in tables:
			logger.debug(f'[d] dropping {t}')
			t.drop(bind=engine, checkfirst=False)
			#meta.drop_all(t, bind=engine, checkfirst=False)
	except (OperationalError, NoReferencedTableError) as e:
		pass
		# logger.error(f'[tdrop] {e}')
	for t in tables:
		try:	
			meta.drop_all(bind=engine, tables=[t], checkfirst=False)
		except OperationalError as e:
			pass
			# logger.error(f'metadroperr {e}')
	try:
		Base.metadata.create_all(bind=engine, tables=[TorqEntry.__table__, TorqFile.__table__, TorqProfile.__table__], checkfirst=False)
	except OperationalError as e:
		logger.error(f'metacreateall {e}')
	logger.debug(f'[dbinit] {(datetime.now() - t1).total_seconds()} done')

def chunks(l, n):
	"""Yield n number of sequential chunks from l."""
	d, r = divmod(len(l), n)
	for i in range(n):
		si = (d + 1) * (i if i < r else r) + d * (0 if i < r else i - r)
		yield l[si:si + (d + 1 if i < r else d)]

def read_and_send(csvfile=None, engine=None, session=None, Session=None, csvhash=None, tablename=None, conn=None):
	
	meta = MetaData(engine)
	conn = engine.connect()
	session = Session()
	t0 = datetime.now()
	tripid = str(csvfile.parts[-2])

	tprof = read_torq_profile(csvfile, tripid)
	session.add(tprof)
	tfile = TorqFile(csvfile, csvhash, tripid, tprof.profile)
	session.add(tfile)
	session.commit()
	torqbuffer = read_csv(csvfile, delimiter=',', low_memory=False, skipinitialspace=True, thousands=',' ,na_values=0, keep_default_na=False, on_bad_lines='skip')
	buffer = fixbuffer(buffer=torqbuffer, metadata=meta, profile=tprof.profile, csvfile=csvfile, tripid_=tripid, csvhash=csvhash, fileid=tfile.id)
	logger.debug(f'[rs] sending c:{csvfile} size:{len(buffer)}')
	t1 = datetime.now()
	try:
		buffer.to_sql(con=engine, name='torqlogs', if_exists='append', index=False, method='multi', chunksize=10000)
	except OperationalError as e:
		logger.error(f'csv:{csvfile} {e.code} {e.args[0]}')
	logger.debug(f'[rs] send done c:{csvfile} time: {(datetime.now() - t0).seconds} time: {(datetime.now() - t1).seconds} ')# 
	return


def main(args):
	t0 = datetime.now()
	TORQDBHOST = 'elitedesk' # os.getenv('TORQDBHOST')
	TORQDBUSER = 'torq' # os.getenv('TORQDBUSER')
	TORQDBPASS = 'dzt3f5jCvMlbUvRG'
	TORQDATABASE = 'torq7'
	dburl = f"mysql+pymysql://{TORQDBUSER}:{TORQDBPASS}@{TORQDBHOST}/{TORQDATABASE}?charset=utf8mb4" # &sessionVariables=sql_mode='NO_ENGINE_SUBSTITUTION'"
	engine = create_engine(dburl, pool_size=200, max_overflow=0)# , isolation_level='AUTOCOMMIT')
	maxworkers = cpu_count()
	tasks = []
	Session = sessionmaker(bind=engine)
	session = Session()
	session.autoflush = True
	# sql = "SET sql_mode = 'NO_ENGINE_SUBSTITUTION'"
	# session.execute(sql)
	if not database_exists(dburl):
		create_database(dburl)
	if args.init_db:
		logger.debug(f'[mainpath] Calling init_db ... ')
		try:
			# sql = 'DROP TABLE IF EXISTS torqfiles;'
			sql = 'DROP TABLE IF EXISTS  torq7.torqfiles;'
			session.execute(sql)
		except IntegrityError as e:
			pass
			# logger.warning(f'[e] {e}')
		try:
			sql = 'DROP TABLE IF EXISTS  torq7.torqlogs;'
			session.execute(sql)
		except IntegrityError as e:
			pass
			# logger.warning(f'[e] {e}')
		try:
			sql = 'DROP TABLE IF EXISTS  torq7.torqtrips;'
			session.execute(sql)
		except IntegrityError as e:
			pass
			# logger.warning(f'[e] {e}')
		database_init(engine)

	hashres = session.execute(select(TorqFile)).fetchall()
	hashlist = [k[0].hash for k in hashres]
	filelist = []
	csv_file_list = get_csv_files(searchpath=args.path)
	logger.debug(f'read start time: {(datetime.now() -t0).seconds} csv:{len(csv_file_list)} h:{len(hashlist)}')
	for idx,csv in enumerate(csv_file_list):
		csvhash = md5(open(csv,'rb').read()).hexdigest()
		if csvhash in hashlist:
			logger.warning(f'[{csv}] already in database')
		else:
			filelist.append(csv)
			read_and_send(csvfile=csv, Session=Session, csvhash=csvhash, engine=engine)
			logger.info(f'[Sent] {idx} of {len(csv_file_list)} r: {len(csv_file_list)-idx}')
	logger.debug(f'torqtask done time: {(datetime.now() -t0).seconds} ')

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
	args = parser.parse_args()
	t0 = datetime.now()
	main(args)
	logger.info(f'[main] done time: {datetime.now() - t0}')
