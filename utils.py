# utils and db things here

import os
import re
from datetime import datetime
from hashlib import md5
from pathlib import Path
import shutil
from loguru import logger
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
from polars.exceptions import InvalidOperationError, ColumnNotFoundError
from sqlalchemy import create_engine
from sqlalchemy.exc import (DataError, IntegrityError, InternalError, OperationalError, ProgrammingError)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session
from datamodels import (TorqFile, Torqlogs,  database_init )
from updatetripdata import send_torqdata, send_torqdata_ppe

MIN_FILESIZE = 2500



def replace_all(text, dic):
	for i, j in dic.items():
		textout = text.replace(i, j)
	if text != textout:
		logger.warning(f'{text} -> {textout}')
	return textout

def get_fixed_lines(logfile, debug=True):
	# read csv file, replace badvals and fix column names
	# returns a buff with the fixed csv file
	badvals = {
			#'-': '0',
			"'-'": '0',
			'"-"': '0',
			'-,': ',0,',
			'∞': '0',
			#',-' : ',0',
			'â': '0',
			'₂': '',
			'°': '',
			'Â°': '0',
			'Â': '0',
			#'612508207723425200000000000000000000000': '0',
			# '340282346638528860000000000000000000000': '0',
			# '-3402823618710077500000000000000000000': '0'
			}
	with open(logfile, 'r') as reader:
		data0 = reader.readlines()
		orgcol = data0[0].split(',')
		data = data0[1:] # skip first line, fix column names later....
		#lines0 = [k for k in data if not k.startswith('-')]
		lines = [replace_all(b, badvals) for b in data]
		newcolname = ','.join([re.sub(r'\W', '', col) for col in orgcol]).encode('ascii', 'ignore').decode()
		newcolname += '\n'
		newcolname = newcolname.lower()
		# column_count = newcolname.count(',')
		lines[0] = newcolname
	return lines

def check_split(logfile: Path, debug=False):
	"""
	check if file is damanaged, if so split it and save new log files
	if the file contains multiple column headers, split into multiple files for each column header line
	todo, check if time difference is small between headers, then ignore and assume its part of the same trip
	"""
	with open(logfile, 'r') as f:
		data = f.readlines()
		splits = sum([k[0:4].lower().count('gps') for k in data])
	return splits

def fix_logfile(logfile: Path, debug=False):
	"""
	fix_logfile - fix bad values in csv files
	returns True if ok, False if not ok
	"""

	# get sanatized data from csv
	try:
		splits = check_split(logfile, debug=debug)
		if splits > 1:
			logger.warning(f'[gcv] {splits=} in {logfile}')
			# todo make splitter ....
			return False
		else:
			fixedlines = get_fixed_lines(logfile, debug=debug)
			logger.info(f'fixer read {len(fixedlines)} lines from {logfile}')
			# make backup of original file before overwriting
			backupfile = f'{logfile}.bak'
			if Path(backupfile).exists():
				logger.warning(f'backupfile {backupfile} exists, skipping ')
				# todo verify backupfile before returing ok.....
				# assume file is fixed return ok
				return True
			else:
				shutil.move(logfile, backupfile)
				# write to fixed csv file
				with open(file=logfile, mode='w', encoding='utf-8', newline='') as writer:
					writer.writelines(fixedlines)
				if debug:
					logger.debug(f'[gcv] saved fixed {logfile}')
				return True
	except Exception as e:
		logger.error(f'[gcv] unhandled {type(e)} {e} in {logfile}')
		return False

def get_csv_files(searchpath: str,  dbmode=None, debug=False):
	# scan searchpath for csv files
	torqcsvfiles = [({
		'csvfile': k, # original csv file
		'csvhash': md5(open(k, 'rb').read()).hexdigest(),
		'size': os.stat(k).st_size,
		'dbmode': dbmode}) for k in Path(searchpath).glob("**/trackLog-*.csv") if k.stat().st_size >= MIN_FILESIZE] # and not os.path.exists(f'{k}.fixed.csv')]
	return torqcsvfiles


def get_bad_vals(csvfile: str):
	with open(csvfile, 'r') as reader:
		data = reader.readlines()
	for line in data:
		l0 = line.split(',')
		for lx in l0:
			try:
				l1 = lx.encode('ascii')
			except (UnicodeEncodeError, UnicodeDecodeError) as e:
				logger.error(f'unicodeerr: {e} in {csvfile} lt={type(line)} l={line}')
			except AttributeError as e:
				logger.error(f'AttributeError: {e} in {csvfile} lt={type(line)} l={line}')

def get_engine_session(args):
	dburl = None
	engine = None
	if args.dbmode == 'mysql':
		dburl = f"mysql+pymysql://{args.dbuser}:{args.dbpass}@{args.dbhost}/{args.dbname}?charset=utf8mb4"
		engine = create_engine(dburl)
		# Session = sessionmaker(bind=engine)
		# session = Session()
	elif args.dbmode == 'postgresql':
		dburl = f"postgresql://{args.dbuser}:{args.dbpass}@{args.dbhost}/{args.dbname}"
		engine = create_engine(dburl)
		# Session = sessionmaker(bind=engine)
		# session = Session()
	elif args.dbmode == 'sqlite':
		dburl = f'sqlite:///torqfiskur.db'
		engine = create_engine(dburl, echo=False, connect_args={'check_same_thread': False})
		# Session = sessionmaker(bind=engine)
		# session = Session()
	if not engine:
		logger.error(f'no engine')
		sys.exit(-1)
	Session = sessionmaker(bind=engine)
	session = Session()
	try:
		database_init(engine)
	except AssertionError as e:
		logger.error(f'[maindbinit] {e}')
		sys.exit(-1)
	return engine, session


def mapping_replace(column: str, mapping: dict):
	if not mapping:
		raise Exception("Mapping can't be empty")
	elif not isinstance(mapping, dict):
		TypeError(f"mapping must be of type dict, but is type: {type(mapping)}")
	if not isinstance(column, str):
		raise TypeError(f"column must be of type str, but is type: {type(column)}")
	branch = pl.when(pl.col(column) == list(mapping.keys())[0]).then(list(mapping.values())[0])
	for from_value, to_value in mapping.items():
		try:
			branch = branch.when(pl.col(column) == from_value).then(to_value)
		except ComputeError as e:
			logger.error(e)
	return branch.otherwise(pl.col(column)).alias(column)





def sqlsender(buffer, dburl, debug=False):
	engine = create_engine(url=dburl, echo=False)
	Session = sessionmaker(bind=engine)
	session = Session()
	results = {
		'fileid': buffer['fileid'],
		'tripid': buffer['tripid'],
		'csvfile': buffer['csvfile'],
		'status': 'unknown'
	}
	try:
		tmpbuf = buffer['torqbuffer'].to_pandas()
	except ValueError as e:
		logger.error(f'[tosql] tmpbuf {type(e)} {e}')
		raise ValueError(f'[tosql] tmpbuf {type(e)} {e}')
	# logger.info(f'[tosql] tmpbuf.is_empty() {buffer["torqbuffer"].is_empty()} ')
	torqfile = session.query(TorqFile).filter(TorqFile.id == results['fileid']).first()
	torqfile.read_flag = 1
	try:
		session.commit() # set send_flag=1
	except Exception as e:
		logger.error(f'{type(e)} {e}')
		return results
	try:
		tmpbuf.to_sql('torqlogs', con=engine, if_exists='append', index=False)
		results['status'] = 'success'
		torqfile = session.query(TorqFile).filter(TorqFile.id == results['fileid']).first()
		torqfile.send_flag = 1
		session.commit() # set send_flag=1
	except (OperationalError, ProgrammingError) as e:
		# todo handle db locks
		# todo handle unknown / new columns from csv files
		# [tosql] code=e3q8 args=(sqlite3.OperationalError) database is locked r={'fileid': 156, 'tripid': 156, 'status': 'unknown'}
		newcol = 'unknown'
		if e.code == 'e3q8' and 'Unknown column' in e.args[0]:
			try:
				newcol = e.args[0].split()[4].replace("'",'')
			except IndexError as iexpt:
				logger.error(f'[tosql] {iexpt} while handling {e}')
			logger.warning(f'[tosql] {newcol=} code={e} args={e.args} r={results} csvfile={buffer["csvfile"]}')  # error:{e}
		elif e.code == 'e3q8' and 'database is locked' in e.args[0]:
			logger.warning(f'[tosql] {newcol=} code={e} args={e.args} r={results} csvfile={buffer["csvfile"]}')  # error:{e}
		else:
			logger.error(f'[tosql] code={e} r={results} csvfile={buffer["csvfile"]}')  # error:{e}
			results['status'] = 'error'
	except InternalError as e:
		logger.error(f'[tosql] InternalError {e} r={results} csvfile={buffer["csvfile"]}')
		results['status'] = 'error'
	except IntegrityError as e:
		logger.warning(f'[tosql] {type(e)} code={e} args={e.args[0]} r={results} csvfile={buffer["csvfile"]}')
		results['status'] = 'error'
		# logger.warning(f'[tosql] {e.statement} {e.params}')
		# logger.warning(f'[tosql] {e}')
	except (pymysql.err.DataError, DataError) as e:
		# r={'fileid': 156, 'tripid': 156, 'status': 'unknown'}
		#logger.error(f'[!]{type(e)}\n{e}\n')
		csvfile = buffer['csvfile'] # session.query(TorqFile).filter(TorqFile.id == results['fileid']).first()
		errmsg = e.args[0]
		err_row = errmsg.split('row')[-1].strip()
		err_row = errmsg.split(',')[1].split('at row')[1].strip().strip('")')
		if 'Incorrect double value' in errmsg:
			err_col = errmsg.split()[8].split('.')[2].strip("`")
		else:
			err_col = errmsg.split(',')[1].split('at row')[0].split("'")[1]
		# logger.warning(f'\n[tosql] code={e}\nargs={e.args[0]}\nr={results}\nerr_row: {err_row}\nerr_col:{err_col}\ntorqfile={tf_err} csvfile={buffer["csvfile"]}\n')  # error:{e}
		logger.warning(f'\n[tosql] {type(e)} code={e} err_row: {err_row} err_col:{err_col} torqfile={csvfile} fileid:{buffer["fileid"]}')  # error:{e}
		# tmpbuf = tmpbuf.drop(columns=err_col)
		err_row = int(err_row)
		try:
			tmpbuf = tmpbuf.drop(index=err_row)
		except Exception as exc:
			logger.error(f'[torql] {type(exc)} {exc} err_row: {err_row} err_col:{err_col} torqfile={csvfile} fileid:{buffer["fileid"]}')
		try:
			tmpbuf.to_sql('torqlogs', con=engine, if_exists='append', index=False)
			results['status'] = 'warning'
		except (IndexError, KeyError, DataError) as ex:
			errmsg = ex.args[0]
			logger.error(f'[!] {type(ex)}\nerrmsg: {errmsg}\n')
	except (TypeError, ValueError) as e:
		logger.error(f'[!]{type(e)}\n{e}\n')
	return results


def sqlsender_ppe(buffer, session, debug=False):
	#engine = create_engine(url=dburl, echo=False)
	#Session = sessionmaker(bind=engine)
	#session = Session()
	results = {
		'fileid': buffer['fileid'],
		'tripid': buffer['tripid'],
		'csvfile': buffer['csvfile'],
		'status': 'unknown'
	}
	try:
		tmpbuf = buffer['torqbuffer'].to_pandas()
	except ValueError as e:
		logger.error(f'[tosql] tmpbuf {type(e)} {e}')
		raise ValueError(f'[tosql] tmpbuf {type(e)} {e}')
	# logger.info(f'[tosql] tmpbuf.is_empty() {buffer["torqbuffer"].is_empty()} ')
	torqfile = session.query(TorqFile).filter(TorqFile.id == results['fileid']).first()
	torqfile.read_flag = 1
	try:
		session.commit() # set send_flag=1
	except Exception as e:
		logger.error(f'{type(e)} {e}')
		return results
	try:
		tmpbuf.to_sql('torqlogs', con=session.get_bind(), if_exists='append', index=False)
		results['status'] = 'success'
		torqfile = session.query(TorqFile).filter(TorqFile.id == results['fileid']).first()
		torqfile.send_flag = 1
		session.commit() # set send_flag=1
	except (OperationalError, ProgrammingError) as e:
		# todo handle db locks
		# todo handle unknown / new columns from csv files
		# [tosql] code=e3q8 args=(sqlite3.OperationalError) database is locked r={'fileid': 156, 'tripid': 156, 'status': 'unknown'}
		newcol = 'unknown'
		if e.code == 'e3q8' and 'Unknown column' in e.args[0]:
			try:
				newcol = e.args[0].split()[4].replace("'",'')
			except IndexError as iexpt:
				logger.error(f'[tosql] {iexpt} while handling {e}')
			logger.warning(f'[tosql] {newcol=} code={e} args={e.args} r={results} csvfile={buffer["csvfile"]}')  # error:{e}
		elif e.code == 'e3q8' and 'database is locked' in e.args[0]:
			logger.warning(f'[tosql] {newcol=} code={e} args={e.args} r={results} csvfile={buffer["csvfile"]}')  # error:{e}
		else:
			logger.error(f'[tosql] code={e} r={results} csvfile={buffer["csvfile"]}')  # error:{e}
			results['status'] = 'error'
	except InternalError as e:
		logger.error(f'[tosql] InternalError {e} r={results} csvfile={buffer["csvfile"]}')
		results['status'] = 'error'
	except IntegrityError as e:
		logger.warning(f'[tosql] {type(e)} code={e} args={e.args[0]} r={results} csvfile={buffer["csvfile"]}')
		results['status'] = 'error'
		# logger.warning(f'[tosql] {e.statement} {e.params}')
		# logger.warning(f'[tosql] {e}')
	except (pymysql.err.DataError, DataError) as e:
		# r={'fileid': 156, 'tripid': 156, 'status': 'unknown'}
		#logger.error(f'[!]{type(e)}\n{e}\n')
		csvfile = buffer['csvfile'] # session.query(TorqFile).filter(TorqFile.id == results['fileid']).first()
		errmsg = e.args[0]
		err_row = errmsg.split('row')[-1].strip()
		err_row = errmsg.split(',')[1].split('at row')[1].strip().strip('")')
		if 'Incorrect double value' in errmsg:
			err_col = errmsg.split()[8].split('.')[2].strip("`")
		else:
			err_col = errmsg.split(',')[1].split('at row')[0].split("'")[1]
		# logger.warning(f'\n[tosql] code={e}\nargs={e.args[0]}\nr={results}\nerr_row: {err_row}\nerr_col:{err_col}\ntorqfile={tf_err} csvfile={buffer["csvfile"]}\n')  # error:{e}
		logger.warning(f'\n[tosql] {type(e)} code={e} err_row: {err_row} err_col:{err_col} torqfile={csvfile} fileid:{buffer["fileid"]}')  # error:{e}
		# tmpbuf = tmpbuf.drop(columns=err_col)
		err_row = int(err_row)
		try:
			tmpbuf = tmpbuf.drop(index=err_row)
		except Exception as exc:
			logger.error(f'[torql] {type(exc)} {exc} err_row: {err_row} err_col:{err_col} torqfile={csvfile} fileid:{buffer["fileid"]}')
		try:
			#tmpbuf.to_sql('torqlogs', con=engine, if_exists='append', index=False)
			results['status'] = 'warning'
		except (IndexError, KeyError, DataError) as ex:
			errmsg = ex.args[0]
			logger.error(f'[!] {type(ex)}\nerrmsg: {errmsg}\n')
	except (TypeError, ValueError) as e:
		logger.error(f'[!]{type(e)}\n{e}\n')
	return results


def read_buff(csvfile, tf_fileid, tf_tripid, debug=False):
	try:
		torqbuffer = read_csv_polars(csvfile, ignore_errors=True, try_parse_dates=True, use_pyarrow=True, null_values=['NaN','-','0\x88\x9e'])
	except (InvalidOperationError,ValueError) as e:
		logger.error(f'[rb] {type(e)} {e} csvfile={csvfile}')
		return None
	except ComputeError as e:
		logger.error(f'[rb] {type(e)} {e} csvfile={csvfile}')
		return None
	# for column in torqbuffer.columns: # replace - with 0
	# 	mapping = {'-': 0}
	# 	try:
	# 		if '-' in str(torqbuffer[column]):
	# 			torqbuffer = torqbuffer.with_columns(mapping_replace(column,mapping))
	# 	except ComputeError as e:
	# 		logger.error(f'[rb] {type(e)} {e} csvfile={csvfile}')
	# 		logger.warning(f'{column=} rbcol: {torqbuffer[column]} {torqbuffer.columns=}')
	if torqbuffer.is_empty():
		logger.error(f'[rb] torqbuffer is empty {csvfile}')
		return None
	fileid_series = pl.Series("fileid", [tf_fileid for k in range(len(torqbuffer))])
	tripid_series = pl.Series("tripid", [tf_tripid for k in range(len(torqbuffer))])
	torqbuffer.insert_at_idx(1, fileid_series)
	torqbuffer.insert_at_idx(2, tripid_series)
	# fix datetime formatting for devicetime and gpstime
	try:
		if len(torqbuffer['devicetime'][0]) == 28:
			devicetime = pl.Series('devicetime', [datetime.strptime(k,'%a %b %d %H:%M:%S GMT %Y') for k in torqbuffer['devicetime']])
		elif len(torqbuffer['devicetime'][0]) == 24:
			devicetime = pl.Series('devicetime', [datetime.strptime(k,'%d-%b-%Y %H:%M:%S.%f') for k in torqbuffer['devicetime']])
		elif len(torqbuffer['devicetime'][0]) == 20:
			devicetime = pl.Series('devicetime', [datetime.strptime(k,'%d-%b-%Y %H:%M:%S') for k in torqbuffer['devicetime']])
		else:
			logger.error(f'[rb] devicetime format error {torqbuffer["devicetime"][0]}')
	except (ColumnNotFoundError, ValueError) as e:
		logger.error(f'[rb] devicetime {type(e)} {e} csvfile: {csvfile}')
		if 'unconverted data remains' in str(e):
			devicetime = pl.Series('devicetime', [datetime.strptime(k,'%d-%b-%Y %H:%M:%S.%f') for k in torqbuffer['devicetime']])
	try:
		if len(torqbuffer['gpstime'][0]) == 28:
			gpstime = pl.Series('gpstime', [datetime.strptime(k,'%a %b %d %H:%M:%S GMT %Y') for k in torqbuffer['gpstime']])
		elif len(torqbuffer['gpstime'][0]) == 34:
			gpstime = pl.Series('gpstime', [datetime.strptime(k,'%a %b %d %H:%M:%S %Z%z %Y') for k in torqbuffer['gpstime']])
		else:
			logger.error(f'[rb] gpstime format error ex: {torqbuffer["gpstime"][0]} len: {len(torqbuffer["gpstime"][0])}')
	except (ComputeError, ValueError) as e:
		logger.error(f'[rb] {type(e)} {e} csvfile: {csvfile}')

	torqbuffer = torqbuffer.drop('devicetime')
	torqbuffer = torqbuffer.drop('gpstime')
	torqbuffer.insert_at_idx(3, gpstime)
	torqbuffer.insert_at_idx(4, devicetime)

	resultbuffer = {
		'torqbuffer' : torqbuffer,
		'fileid' : tf_fileid,
		'tripid' : tf_tripid,
		'csvfile' : csvfile,
	}
	return resultbuffer



async def torq_worker_ppe(tf, session, debug=False):
	buffer = None
	results = None
	datares = None
	t0 = datetime.now()
	try:
		buffer = read_buff(tf.csvfile, tf.id, tf.tripid, debug=debug)
		if not buffer:
			logger.warning(f'[!] buffer is None tf={tf}')
		elif debug:
			pass # logger.debug(f'file {tf.csvfile} buffer: {len(buffer["torqbuffer"])}')
	except (InvalidOperationError, ValueError, TypeError, PicklingError, ComputeError) as e:
		logger.error(f'[!] {type(e)} {e} in read_buff {tf.csvfile}')
		return None
	try:
		results = sqlsender_ppe(buffer,session, debug=debug) # send triplog data
		if debug:
			logger.debug(f'file {tf.csvfile} {results=} buffer: {len(buffer["torqbuffer"])}')
	except (ValueError, TypeError, PicklingError) as e:
		logger.error(f'[!] {type(e)} {e} in sqlsender buffer.is_empty() {buffer["torqbuffer"].is_empty()}')
		return None

async def torq_dataworker_ppe(tf, session, debug=False):
	buffer = None
	results = None
	datares = None
	t0 = datetime.now()
	try:
		datares = send_torqdata_ppe(tf.id, session, debug=debug) # send trip data
		if debug:
			logger.debug(f'file {tf.csvfile} {datares=}')
	except (ValueError, TypeError, PicklingError, OperationalError) as e:
		logger.error(f'{type(e)} {e} in send_torqdata')
		return None

	tx = (datetime.now() - t0).total_seconds()
	res = {
		'tf': tf,
		'bufferlen': len(buffer),
		'results': results,
		'datares': datares if datares else None,
		'processing_time': tx,
	}
	return res


