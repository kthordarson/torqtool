# utils and db things here
from lib2to3.pgen2.token import OP
from multiprocessing.sharedctypes import Value
from multiprocessing import Process
import os
import sys
import re
from pathlib import Path
from loguru import logger

from datamodels import TorqProfile
logger.add('tool.log')
import inspect
from re import T, search, sub
from pandas import read_csv, DataFrame, to_datetime, to_numeric
from datetime import datetime
from dateutil.parser import ParserError
from sqlalchemy.exc import ProgrammingError
from hashlib import md5
from sqlalchemy import ForeignKey, create_engine, Table, MetaData, Column, Integer, String, inspect, select, BigInteger, Numeric, DateTime, text, BIGINT,  Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship
from sqlalchemy.exc import OperationalError, DataError
import pymysql
from fieldmaps import FIELDMAPS
import json
from threading import Thread, active_count
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

MIN_FILESIZE = 4096

def get_csv_files(searchpath:Path, recursive=True):
	# todo fix globbing....
	# csvlist = searchpath.glob('tracklog*.csv')
	if not isinstance(searchpath, Path):
		searchpath = Path(searchpath)
	if not isinstance(searchpath, Path):
		logger.debug(f'[getcsv] err: searchpath {searchpath} is {type(searchpath)} need Path object')
		return []
	else:
		torqcsvfiles = [k for k in searchpath.glob("**/trackLog.csv") if k.stat().st_size >= MIN_FILESIZE]
		return torqcsvfiles

def column_fixer(inputline):
	columns = inputline.split(',') # split
	columns = [sub('\n', '', col) for col in columns] # remove \n 's
	columns = [sub(' ', '', col) for col in columns] # degree symbol
	# columns = [sub('-', '', col) for col in columns] # degree symbol
	columns = [sub(',', '', col) for col in columns] # degree symbol
	columns = [sub('â', '', col) for col in columns] # degree symbol
	columns = [sub('Â', '', col) for col in columns] # symbol cleanup
	columns = [sub('Ã¢', '', col) for col in columns] # symbol cleanup
	columns = [sub('Ã‚', '', col) for col in columns] # symbol cleanup
	columns = [sub('CO,‚', 'CO', col) for col in columns] # symbol cleanup
	columns = [sub(r'^\s', '', k) for k in columns] # remove extra spaces from start of col name
	columns = ''.join([str(k)+',' for k in columns])
	columns = columns.rstrip(',')
	# columns = columns.lrstrip(',')
	logger.debug(f'[i] {inputline} [o] {columns}')
	return columns

def read_csv_columns_raw(csv_filename):
	with open(csv_filename) as f:
		lineone = f.readline()
	fixed_cols = column_fixer(lineone)
	return fixed_cols


def get_torqlog_table(engine):
	# meta = MetaData(engine)
	# Session = sessionmaker(bind=engine)
	# session = Session()
	conn = engine.connect()
	torqfiles = conn.execute('SELECT * FROM torqlogs').fetchall()
	engine.dispose()
	return len(torqfiles)


def make_column_list(columnlist):
	templist = []
	for list  in columnlist:
		for col in list:
			if col in templist:
				pass
			else:
				templist.append(col)
				# logger.debug(f'[templist] {len(templist)} added {col}')
	templist = sorted(set(templist))
	with open('tempfields.txt', 'a') as f:
		f.writelines(templist)
	return templist


def parse_csvfile(csv_filename):
	if len(csv_filename.parent.name) == 13: # torq creates folders based on unix time with milliseconds
		dateguess = datetime.utcfromtimestamp(int(csv_filename.parent.name)/1000)
	else: # normal....
		dateguess = datetime.utcfromtimestamp(int(csv_filename.parent.name))
	# logger.info(f'[dateguess] fn:{csv_filename} lfn:{len(csv_filename.parent.name)} dg:{dateguess} ')
	return f'{dateguess}'


def read_torq_profile(filename, tripid):
	p_filename = os.path.join(filename.parent, 'profile.properties')
	with open(p_filename, 'r') as f:
		pdata_ = f.readlines()
	if len(pdata_) == 8:
		pdata = [l.strip('\n') for l in pdata_ if not l.startswith('#')]
		try:
			pdata_date = str(pdata_[1][1:]).strip('\n')
			tripdate = to_datetime(pdata_date).to_pydatetime()
			# logger.info(f'[rs] pd:{pdata_date} {type(pdata_date)} td:{tripdate} {type(tripdate)}')
		except (OperationalError, Exception) as e:
			logger.error(f'[readsend] {e}')
			tripdate = None
		trip_profile = dict([k.split('=') for k in pdata])
		torqprofile = TorqProfile()
		torqprofile.fuelCost = float(trip_profile['fuelCost'])
		torqprofile.fuelUsed = float(trip_profile['fuelUsed'])
		torqprofile.distanceWhilstConnectedToOBD = float(trip_profile['distanceWhilstConnectedToOBD'])
		torqprofile.distance = float(trip_profile['distance'])
		torqprofile.time = float(trip_profile['time'])
		torqprofile.filename = p_filename
		torqprofile.tripdate = tripdate
		torqprofile.profile = trip_profile['profile']
		torqprofile.tripid = tripid
		# trip_profile = DataFrame([trip_profile])
		return torqprofile
	else:
		logger.warning(f'[p] {filename} len={len(pdata_)}')
