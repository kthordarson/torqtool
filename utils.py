# utils and db things here

import os
import re
import shutil
import sys
from datetime import datetime
from hashlib import md5
from pathlib import Path
from pickle import PicklingError
import argparse
import pandas as pd
import polars as pl
import pymysql
import pytz
from loguru import logger
from polars import ComputeError
from polars import read_csv as read_csv_polars
from polars.exceptions import ColumnNotFoundError, InvalidOperationError
from sqlalchemy import create_engine
from sqlalchemy.exc import (
	ArgumentError,
	DataError,
	IntegrityError,
	InternalError,
	OperationalError,
	ProgrammingError,
)
from sqlalchemy.orm import sessionmaker

from commonformats import fmt_20, fmt_24, fmt_26, fmt_28, fmt_30, fmt_34, fmt_36
from datamodels import TorqFile, database_init
from updatetripdata import send_torqdata_ppe
from schemas import ncc

MIN_FILESIZE = 2500

class TimeZoneAwareConstructorWarning():
	pass

def replace_all(text, dic):
	for i, j in dic.items():
		textout = text.replace(i, j)
	if text != textout:
		logger.warning(f'{text} -> {textout}')
	return textout

def get_sanatized_column_names(orgcol):
	"""
	clean up column names, remove special characters and make lowercase
	orgcol DataFrame.columns (list of column names) or str of column names
	"""
	if isinstance(orgcol, list):
		newcolname = ','.join([re.sub(r'\W', '', col).lower() for col in orgcol]).encode('ascii', 'ignore').decode()
		newcolname += '\n'
		newcolname = newcolname.lower()
		return newcolname
	elif isinstance(orgcol, str):
		newcolname = ','.join([re.sub(r'\W', '', col).lower() for col in orgcol.split(',')]).encode('ascii', 'ignore').decode()
		newcolname += '\n'
		return newcolname
	else:
		logger.warning(f'unknown type {type(orgcol)} {orgcol}')
		return orgcol

def get_fixed_lines(logfile, debug=True):
	# read csv file, replace badvals and fix column names
	# returns a buff with the fixed csv file
	badvals = {
			#'-': '0',
			#"'-'": '0',
			#'"-"': '0',
			#'-,': ',0,',
			'∞': '0',
			# '-,' : ',0',
			#'â': '0',
			#₂': '',
			#'°': '',
			#'Â°': '0',
			#'Â': '0',
			#'612508207723425200000000000000000000000': '0',
			# '340282346638528860000000000000000000000': '0',
			# '-3402823618710077500000000000000000000': '0'
			}
	with open(logfile, 'r') as reader:
		data0 = reader.readlines()
	orgcol = data0[0].split(',')
	data = data0[1:] # skip first line, fix column names later....
	#lines0 = [k for k in data if not k.startswith('-')]
	# lines = [replace_all(b, badvals) for b in data]
	lines = [re.sub(',-,',',0,',k) for k in data]
	lines = [re.sub('∞','0',k) for k in lines]
	lines = [re.sub('â','0',k) for k in lines]
	lines = [re.sub('Â','0',k) for k in lines]
	#for bv in badvals:
	#	lines = [re.sub(bv,'0,',k) for k in data]
	# lines = [re.sub('∞','0',k) for k in data]
	# lines = [re.sub(b,'0',k) for k in data for b in badvals]
	#newcolname = ','.join([re.sub(r'\W', '', col) for col in orgcol]).encode('ascii', 'ignore').decode()
	#newcolname += '\n'
	#newcolname = newcolname.lower()
	newcolname = get_sanatized_column_names(orgcol)
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
			# logger.info(f'fixer read {len(fixedlines)} lines from {logfile}')
			# make backup of original file before overwriting
			backupfile = f'{logfile}.bak'
			if Path(backupfile).exists():
				pass # logger.warning(f'backupfile {backupfile} exists, skipping ')
				# todo verify backupfile before returing ok.....
				# assume file is fixed return ok
				return True
			else:
				shutil.move(logfile, backupfile)
				# write to fixed csv file
				with open(file=logfile, mode='w', encoding='utf-8', newline='') as writer:
					writer.writelines(fixedlines)
				if debug:
					logger.debug(f'[gcv] saved {len(fixedlines)} fixed lines to {logfile}')
				return True
	except FileNotFoundError as e:
		logger.error(f'[gcv] {type(e)} {e} in {logfile=} ')
		return False
	except Exception as e:
		logger.error(f'[gcv] unhandled {type(e)} {e} in {logfile}')
		return False

def get_csv_files(searchpath: str,  dbmode=None, debug=False):
	# scan searchpath for csv files
	torqcsvfiles = [({
		'csvfile': k, # original csv file
		'csvhash': md5(open(k, 'rb').read()).hexdigest(),
		'size': os.stat(k).st_size,
		'dbmode': dbmode}) for k in Path(searchpath).glob("**/*.csv") if k.stat().st_size >= MIN_FILESIZE] # and not os.path.exists(f'{k}.fixed.csv')]
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
	elif args.dbmode == 'mariadb':
		dburl = f"mysql+pymysql://{args.dbuser}:{args.dbpass}@{args.dbhost}/{args.dbname}?charset=utf8mb4"
		engine = create_engine(dburl)
		# Session = sessionmaker(bind=engine)
		# session = Session()
	elif args.dbmode == 'psql':
		dburl = f"postgresql://{args.dbuser}:{args.dbpass}@{args.dbhost}/{args.dbname}"
		engine = create_engine(dburl)
		# Session = sessionmaker(bind=engine)
		# session = Session()
	elif args.dbmode == 'sqlite':
		dburl = f'sqlite:///{args.dbfile}'
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
		'csvfile': buffer['csvfile'],
		'status': 'unknown'
	}
	try:
		tmpbuf = buffer['torqbuffer'].to_pandas()
	except ValueError as e:
		logger.error(f'[tosql] tmpbuf {type(e)} {e}')
		raise ValueError(f'[tosql] tmpbuf {type(e)} {e}')
	# logger.info(f'[tosql] tmpbuf.is_empty() {buffer["torqbuffer"].is_empty()} ')
	torqfile = session.query(TorqFile).filter(TorqFile.fileid == results['fileid']).first()
	torqfile.read_flag = 1
	if debug:
		logger.debug(f'set read_flag on {torqfile=}')

	try:
		session.commit() # set send_flag=1
	except Exception as e:
		logger.error(f'{type(e)} {e}')
		return results
	try:
		tmpbuf.to_sql('torqlogs', con=engine, if_exists='append', index=False)
		results['status'] = 'success'
		torqfile = session.query(TorqFile).filter(TorqFile.fileid == results['fileid']).first()
		torqfile.send_flag = 1
		if debug:
			logger.debug(f'set send_flag on {torqfile=}')

		session.commit() # set send_flag=1
	except (OperationalError, ProgrammingError) as e:
		# todo handle db locks
		# todo handle unknown / new columns from csv files
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
		#logger.error(f'[!]{type(e)}\n{e}\n')
		csvfile = buffer['csvfile'] # session.query(TorqFile).filter(TorqFile.fileid == results['fileid']).first()
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
		'csvfile': buffer['csvfile'],
		'status': 'unknown'
	}
	try:
		if not isinstance(buffer['torqbuffer'], pd.DataFrame):
			tmpbuf = buffer['torqbuffer'].to_pandas()
		else:
			tmpbuf = buffer['torqbuffer']
	except ValueError as e:
		logger.error(f'[tosql] tmpbuf {type(e)} {e}')
		raise ValueError(f'[tosql] tmpbuf {type(e)} {e}')
	# logger.info(f'[tosql] tmpbuf.is_empty() {buffer["torqbuffer"].is_empty()} ')
	torqfile = session.query(TorqFile).filter(TorqFile.fileid == results['fileid']).first()
	torqfile.read_flag = 1
	if debug:
		pass # logger.debug(f'set read_flag on {torqfile=}')
	try:
		session.commit() # set send_flag=1
	except Exception as e:
		logger.error(f'{type(e)} {e}')
		return results
	try:
		tmpbuf.to_sql('torqlogs', con=session.get_bind(), if_exists='append', index=False)
		results['status'] = 'success'
		torqfile = session.query(TorqFile).filter(TorqFile.fileid == results['fileid']).first()
		torqfile.send_flag = 1
		if debug:
			pass # logger.debug(f'set send_flag on {torqfile=}')

		session.commit() # set send_flag=1
	except (OperationalError, ProgrammingError, ArgumentError) as e:
		# todo handle db locks
		# todo handle unknown / new columns from csv files
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
		#logger.error(f'[!]{type(e)}\n{e}\n')
		csvfile = buffer['csvfile'] # session.query(TorqFile).filter(TorqFile.fileid == results['fileid']).first()
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

def read_buf_2(logdir,maxfiles=100):
	dfx = pd.DataFrame()
	errors=0
	readfiles=0
	files_with_errors = []
	for k in Path(logdir).glob('*.csv'):
		if readfiles>=maxfiles:
			logger.warning(f'MAX: {maxfiles} {readfiles=}')
			break
		try:
			d=pl.read_csv(k, ignore_errors=True, try_parse_dates=True,truncate_ragged_lines=True)
			# dfx=pd.concat([d.to_pandas(),dfx])
			print(f'{errors} {len(dfx)} {readfiles}')
			readfiles+=1
		except Exception as e:
			print(f'{errors} {type(e)} {e}')
			errors+=1
			files_with_errors.append(k)
	if errors>0:
		print(f'Errors: {files_with_errors}')
	return dfx

def read_buff(csvfile, tf_fileid,  debug=False):
	error_files = []
	devicetime = []
	gpstime = []
	rb = {
		'torqbuffer' : pd.DataFrame(),
		'fileid' : tf_fileid,
		'csvfile' : csvfile,
	}
	try:
		torqbuffer = read_csv_polars(csvfile, ignore_errors=True, try_parse_dates=True,truncate_ragged_lines=True) #, use_pyarrow=True ,  ) #, null_values=['NaN','-','0\x88\x9e'])
		torqbuffer = torqbuffer.fill_null(0).fill_nan(0)

	except (InvalidOperationError,ValueError) as e:
		logger.error(f'[rb] {type(e)} {e} csvfile={csvfile}')
		return rb, error_files
	except ComputeError as e:
		logger.error(f'[rb] {type(e)} {e} csvfile={csvfile}')
		return rb, error_files
	# for column in torqbuffer.columns: # replace - with 0
	# 	mapping = {'-': 0}
	# 	try:
	# 		if '-' in str(torqbuffer[column]):
	# 			torqbuffer = torqbuffer.with_columns(mapping_replace(column,mapping))
	# 	except ComputeError as e:
	# 		logger.error(f'[rb] {type(e)} {e} csvfile={csvfile}')
	# 		logger.warning(f'{column=} rbcol: {torqbuffer[column]} {torqbuffer.columns=}')

	# devtime = torqbuffer.devicetime
	# if not devtime:
	# 	logger.error(f'[rb] missing devicetime {csvfile}')
	# 	return None
	if torqbuffer.is_empty():
		logger.error(f'[rb] torqbuffer is empty {csvfile}')
		return rb, error_files
	fileid_series = pl.Series("fileid", [tf_fileid for k in range(len(torqbuffer))])
	torqbuffer.insert_at_idx(1, fileid_series)
	rbx = None
	errf = None
	try:
		rbx, errf = fix_timestamps(torqbuffer, csvfile,tf_fileid)
	except Exception as e:
		logger.error(f'[rb] {type(e)} {e} in fix_timestamps {csvfile}\nrbx: {rbx}\n')
	if rbx:
		rb['torqbuffer'] = rbx['torqbuffer']
		if debug:
			pass # logger.info(f'[rb] {csvfile} rbx={rbx} \nrb={rb}\n{error_files}\n')
	if errf:
		error_files.extend(errf)
	return rb, error_files
	# fix datetime formatting for devicetime and gpstime
	#if not isinstance(torqbuffer['devicetime'], pl.Series):
	#	print(f"{csvfile} {torqbuffer['devicetime']}")
	#if not torqbuffer['devicetime'].is_empty():
	#	print(f"{csvfile} {torqbuffer}")

def fix_timestamps(torqbuffer, csvfile, tf_fileid):
	# todo fix gpstime and devicetime
	# drop rows where either values are null or missing
	error_files = []
	resultbuffer = {
		'torqbuffer' : torqbuffer,
		'fileid' : tf_fileid,
		'csvfile' : csvfile,
	}
	try:
		idx = len(torqbuffer['devicetime']) // 2 # get middle index to guess dateformat
	except (ColumnNotFoundError, ComputeError, ValueError) as e:
		logger.error(f'[rb] devicetime {type(e)} {e} csvfile: {csvfile}')
		idx = 10
	try:
		idx = len(torqbuffer['gpstime']) // 2 # get middle index to guess dateformat
	except (ColumnNotFoundError, ComputeError, ValueError) as e:
		logger.error(f'[rb] gpstime {type(e)} {e} csvfile: {csvfile}')
		idx = 10
	gpstime = torqbuffer['gpstime']
	devicetime = torqbuffer['devicetime']
	try:
		if len(torqbuffer['devicetime'][idx]) == 28:
			devicetime = pl.Series('devicetime', [datetime.strptime(k,fmt_28).astimezone(pytz.timezone('UTC')) for k in torqbuffer['devicetime'] if k])
		elif len(torqbuffer['devicetime'][idx]) == 24:
			devicetime = pl.Series('devicetime', [datetime.strptime(k,fmt_24).astimezone(pytz.timezone('UTC')) for k in torqbuffer['devicetime'] if k])
		elif len(torqbuffer['devicetime'][idx]) == 26:
			devicetime = pl.Series('devicetime', [datetime.strptime(k,fmt_26).astimezone(pytz.timezone('UTC')) for k in torqbuffer['devicetime'] if k] )
		elif len(torqbuffer['devicetime'][idx]) == 20:
			devicetime = pl.Series('devicetime', [datetime.strptime(k,fmt_20).astimezone(pytz.timezone('UTC')) for k in torqbuffer['devicetime']if k])
		else:
			logger.error(f'[rb] devicetime format error! len = {len(torqbuffer["devicetime"][idx])} {idx=} buffer: {torqbuffer["devicetime"]}')
	except (ColumnNotFoundError, ComputeError, ValueError,TypeError) as e:
		logger.error(f'[rb] devicetime {type(e)} {e} csvfile: {csvfile} len = {len(torqbuffer["devicetime"][idx])} {idx=} ' )
		error_files.append(csvfile)
	try:
		if len(torqbuffer['gpstime'][idx]) == 28:
			gpstime = pl.Series('gpstime', [datetime.strptime(k,fmt_28).astimezone(pytz.timezone('UTC')) for k in torqbuffer['gpstime'] if k])
		elif len(torqbuffer['gpstime'][idx]) == 26:
			# gpstime = pl.Series('gpstime', [datetime.strptime(k,fmt_26).astimezone(pytz.timezone('UTC')) for k in torqbuffer['gpstime'] if k])
			gpstime = pl.Series('gpstime', [datetime.strptime(k,fmt_26).astimezone(pytz.timezone('UTC')) for k in torqbuffer['gpstime'] if k ]  )
		elif len(torqbuffer['gpstime'][idx]) == 34:
			# to fix TimeZoneAwareConstructorWarning
			gpstime = pl.Series('gpstime', [datetime.strptime(k,fmt_34).astimezone(pytz.timezone('UTC')) for k in torqbuffer['gpstime'] if k])
			# gpstime = pl.Series('gpstime', [datetime.strptime(k,fmt_34) for k in torqbuffer['gpstime'] if k], strict=True, dtype_if_empty=str, nan_to_null=True)
			# gpstime = pl.Series("dt", [ts.astimezone(pytz.timezone('UTC'))])
			# print(f'\n timestamp \n {torqbuffer["gpstime"][idx]} \n \n')
		else:
			logger.error(f'[rb] gpstime format error ex: {torqbuffer["gpstime"]} len: {len(torqbuffer["gpstime"])}')
	except (ComputeError, ValueError,TypeError) as e:
		logger.error(f'[rb] {type(e)} {e} csvfile: {csvfile} len = {len(torqbuffer["devicetime"][idx])} {idx=} buf: {torqbuffer["gpstime"]}')
		error_files.append(csvfile)
		# raise e

	gpstime_err = [idx for idx, k in enumerate(torqbuffer['gpstime']) if not k ]
	devicetime_err = [idx for idx, k in enumerate(torqbuffer['devicetime']) if not k ]
	try:
		torqbuffer = torqbuffer.drop('devicetime')
		if len(torqbuffer) != len(devicetime):
			torqbuffer = torqbuffer[0:len(devicetime)]
		torqbuffer.insert_at_idx(4, devicetime)
	except (AttributeError, UnboundLocalError, pl.exceptions.ShapeError) as e:
		logger.error(f'[rb] {type(e)} {e} csvfile: {csvfile} tblen={len(torqbuffer)} glen={len(gpstime)} dlen={len(devicetime)} {gpstime_err=} {devicetime_err=}')
		error_files.append(csvfile)
	try:
		torqbuffer = torqbuffer.drop('gpstime')
		if len(torqbuffer) != len(gpstime):
			torqbuffer = torqbuffer[0:len(gpstime)]
		torqbuffer.insert_at_idx(3, gpstime)
	except (AttributeError, UnboundLocalError, pl.exceptions.ShapeError) as e:
		logger.error(f'[rb] {type(e)} {e} csvfile: {csvfile} tblen={len(torqbuffer)} glen={len(gpstime)} dlen={len(devicetime)} {gpstime_err=} {devicetime_err=}')
		error_files.append(csvfile)
	resultbuffer['torqbuffer'] = torqbuffer
	# resultbuffer = {
	# 	'torqbuffer' : torqbuffer,
	# 	'fileid' : tf_fileid,
	# 	'csvfile' : csvfile,
	# }
	return resultbuffer, error_files



async def torq_worker_ppe(tf, session, debug=False):
	buffer = None
	results = None
	datares = None
	t0 = datetime.now()
	timetotal = 0
	try:
		buffer, error_files = read_buff(tf.csvfile, tf.fileid,  debug=debug)
		if not buffer:
			logger.warning(f'[!] buffer is None tf={tf}')
		if debug:
			if len(error_files) > 0:
				logger.warning(f'error_files: {len(error_files)} ') #pass # logger.debug(f'file {tf.csvfile} buffer: {len(buffer["torqbuffer"])}')
				_ = [print(f'error in file: {k}') for k in error_files]
	except ( TypeError,) as e:
		logger.error(f'[!] {type(e)} {e} in read_buff {tf.csvfile}')
		raise e
	except (InvalidOperationError, ValueError, PicklingError, ComputeError) as e:
		logger.error(f'[!] {type(e)} {e} in read_buff {tf.csvfile}')
		return None
	try:
		results = sqlsender_ppe(buffer,session, debug=debug) # send triplog data
		timetotal += (datetime.now()-t0).seconds
		if debug:
			pass # logger.debug(f't: {(datetime.now()-t0).seconds}/{timetotal} fileid {results.get("fileid")} {results.get("status")} buffer: {len(buffer["torqbuffer"])}')
	except (ValueError, TypeError, PicklingError) as e:
		logger.error(f'[!] {type(e)} {e} in sqlsender buffer.is_empty() {buffer["torqbuffer"].is_empty()}')
		return None

async def torq_dataworker_ppe(tf, session, debug=False):
	if not tf:
		logger.warning('missing tf!')
		return None
	buffer = None
	results = None
	datares = None
	t0 = datetime.now()
	# logger.warning(f'torq_dataworker_ppe not implemented {tf=}')
	# return None
	try:
		if debug:
			logger.debug(f'file {tf}  ')
		datares = send_torqdata_ppe(tf, session, debug=debug) # send trip data
		if not datares:
			logger.warning(f'torq_dataworker_ppe got no data file {tf.fileid}  {tf.csvfile} {datares=}')
	except (ValueError, TypeError, PicklingError, OperationalError) as e:
		logger.error(f'torq_dataworker_ppe {type(e)} {e} in send_torqdata_ppe {tf=}')
		raise e
	return datares

def send_torqtripdata(stats_data:dict, session:sessionmaker, args:argparse.Namespace):
	"""
	generate some stats from torqlogs and send to database
	param stats_data dict of stats, session sqlalchemy session, args
	"""
	# todo
	# send the data generated by generate_torqdata to database
	print(stats_data)

def get_time_stats(time_cols):
	stats = {}
	for c in time_cols:
		stats[c.name] = {
			'name':c.name,
			f'{c.name}.min': c.min(),
			f'{c.name}.mean': c.mean(),
			f'{c.name}.max': c.max(),
			f'{c.name}.tdelta':c.max() - c.min(),
		   }
	return stats

def get_speed_stats(speed_cols):
	stats = {}
	for c in speed_cols:
		stats[c.name] = {
			'name':c.name,
			f'{c.name}.mean':c.mean(),
			f'{c.name}.max':c.max(),
		   }
	return stats

def get_gps_stats(gpscols):
	stats = {}
	for c in gpscols:
		stats[c.name] = {
			'name':c.name,
			f'{c.name}.min':c.min(),
			f'{c.name}.mean':c.mean(),
			f'{c.name}.max':c.max(),
		   }
	return stats

def get_cost_stats(cost_cols):
	stats = {}
	for c in cost_cols:
		stats[c.name] = {
			'name':c.name,
			f'{c.name}.min':c.min(),
			f'{c.name}.mean':c.mean(),
			f'{c.name}.max':c.max(),
		   }
	return stats

def get_temp_stats(temp_cols):
	stats = {}
	for c in temp_cols:
		stats[c.name] = {
			'name':c.name,
			f'{c.name}.min':c.min(),
			f'{c.name}.mean':c.mean(),
			f'{c.name}.max':c.max(),
		   }
	return stats

def generate_torqdata(df:pd.DataFrame, session:sessionmaker=None, args:argparse.Namespace=None):
	# generate torqdata from torqlogs
	#df = pd.DataFrame([k.__dict__ for k in data])
	time_cols = [df[k] for k in df.columns if 'gpstime' in k or 'devicetime' in k]
	stats = {}
	stats['timestats'] = get_time_stats(time_cols)

	speed_cols = [df[k] for k in df.columns if 'speed' in k]
	stats['speedstats'] = get_speed_stats(speed_cols)

	gps_cols = [df[k] for k in df.columns if 'gps' in k]
	stats['gpsstats'] = get_gps_stats(gps_cols)

	cost_cols = [df[k] for k in df.columns if 'cost' in k]
	stats['coststats'] = get_cost_stats(cost_cols)

	temp_cols = [df[k] for k in df.columns if 'temp' in k]
	stats['tempstats'] = get_temp_stats(temp_cols)

	# gps_cols = [df[k] for k in df.columns if 'gps' in k]
	# for c in gps_cols:
	# 	stats[c.name] = {
	# 		'name':c.name,
	# 		f'{c.name}.mean':c.mean(),
	# 		f'{c.name}.max':c.max(),
	# 		f'{c.name}.mix':c.min(),
	# 	   }
	# 	# print(stats)
	# cost_cols = [df[k] for k in df.columns if 'cost' in k]
	# for c in cost_cols:
	# 	stats[c.name] = {
	# 		'name':c.name,
	# 		f'{c.name}.mean':c.mean(),
	# 		f'{c.name}.max':c.max(),
	# 		f'{c.name}.mix':c.min(),
	# 	   }
	# 	# print(stats)
	# temp_cols = [df[k] for k in df.columns if 'temp' in k]
	# for c in temp_cols:
	# 	stats[c.name] = {
	# 		'name':c.name,
	# 		f'{c.name}.mean':c.mean(),
	# 		f'{c.name}.max':c.max(),
	# 		f'{c.name}.mix':c.min(),
	# 	   }
	# 	#print(stats)
	return stats




def xdate_fixer_devicetime(data, f):
	"""
	fix devicetime columns
	"""
	# cfix = ','.join([re.sub(r'\W', '', col) for col in data.columns]).encode('ascii', 'ignore').decode().lower().split(',')
	# drop old date columns from data and store new df0
	df0 = data.rename(columns=ncc).fillna(0)
	# df0 = data.drop(columns=date_columns)
	fixed_datecol = pd.DataFrame()
	datecol = 'devicetime'
	# what date format to use
	# fmt_selector = sum([len(k) for k in data[datecol]])//len(data)

	try:
		testdate = df0[datecol][len(df0)//2]
		if isinstance(testdate, pd.Timestamp):
			logger.info(f'{datecol} is ok....')
			return df0
		fmt_selector = len(testdate) # use value in middle to check.....
	except (ValueError, TypeError, KeyError) as e:
		logger.warning(f'fmtselector {e} {f=} {testdate=} {datecol=}')
		fmt_selector = len(df0[datecol][1])
		logger.warning(f'fmtselector {e} {f=} {testdate=} {datecol=} newfmt_selector:{fmt_selector} d: {len(data)} d0: {len(df0)} \n sample0 {df0[datecol][0]} samplefmt {df0[datecol][fmt_selector]} \n datacols={data.columns} \n df0cols={df0.columns}\n')
		# continue #
	try:
		match fmt_selector:
			case 20:
				fixed_datecol = pd.DataFrame({datecol:[datetime.strptime(k,fmt_20).astimezone(pytz.timezone('UTC')) for k in df0[datecol][1:] ]})
			case 24:
				fixed_datecol = pd.DataFrame({datecol:[datetime.strptime(k,fmt_24).astimezone(pytz.timezone('UTC')) for k in df0[datecol][1:] ]})
			case 26:
				fixed_datecol = pd.DataFrame({datecol:[datetime.strptime(k,fmt_26).astimezone(pytz.timezone('UTC')) for k in df0[datecol][1:] ]})
			case 28:
				fixed_datecol = pd.DataFrame({datecol:[datetime.strptime(k,fmt_28).astimezone(pytz.timezone('UTC')) for k in df0[datecol][1:] ]})
			case 30:
				fixed_datecol = pd.DataFrame({datecol:[datetime.strptime(k,fmt_30).astimezone(pytz.timezone('UTC')) for k in df0[datecol][1:] ]})
			case 34:
				fixed_datecol = pd.DataFrame({datecol:[datetime.strptime(k,fmt_34).astimezone(pytz.timezone('UTC')) for k in df0[datecol][1:] ]})
			case 36:
				fixed_datecol = pd.DataFrame({datecol:[datetime.strptime(k,fmt_36).astimezone(pytz.timezone('UTC')) for k in df0[datecol][1:] ]})
			case _:
				pass # logger.warning(f'could not match format for fmt_selector {fmt_selector} for {datecol} {f=}.\n sample:first= {df0[datecol][0]} middle= {df0[datecol][len(data)//2]} last= {df0[datecol][len(df0)-1]}\n')
	except (ValueError,TypeError,KeyError) as e:
		logger.error(f'datefix {type(e)} {e} {testdate=} {f=} datecol: {datecol} fmt: {fmt_selector} \n sample:first= {df0[datecol][0]} middle= {df0[datecol][len(data)//2]} \n') # last= {df0[datecol][len(df0)-1]}
	if not fixed_datecol.empty:
		df0 = data.drop(columns=datecol)
		df0 = pd.concat([df0,fixed_datecol])
		logger.info(f'replaced {datecol}')
	else:
		logger.warning(f'could not match format for fmt_selector {fmt_selector} for {datecol} {f=} {testdate=} .\n sample:first= {df0[datecol][0]} middle= {df0[datecol][len(data)//2]} ') # last= {df0[datecol][len(df0)-1]}\n')
	return df0


def xdate_fixer_gpstime(data, f):
	"""
	fix gpstime columns
	"""
	# cfix = ','.join([re.sub(r'\W', '', col) for col in data.columns]).encode('ascii', 'ignore').decode().lower().split(',')
	# drop old date columns from data and store new df0
	df0 = data.rename(columns=ncc).fillna(0)
	# df0 = data.drop(columns=date_columns)
	fixed_datecol = pd.DataFrame()
	# gpstime = [datetime.strptime(k,fmt_28).astimezone(pytz.timezone('UTC')) for k in data['gpstime']]
	# devicetime = [datetime.strptime(k,fmt_28).astimezone(pytz.timezone('UTC')) for k in data['devicetime']]
	# fc = pd.DataFrame({dc:data})
	# fc = pd.DataFrame(data=[datetime.strptime(k,fmt_24).astimezone(pytz.timezone('UTC')) for k in data[dc]])
	datecol = 'gpstime'
	# what date format to use
	# fmt_selector = sum([len(k) for k in data[datecol]])//len(data)

	try:
		# testdate = df0[datecol][len(df0)//2]
		testdate = max([k for k in df0[datecol].values if k !=0])
		if isinstance(testdate, pd.Timestamp):
			logger.info(f'{datecol} is ok....')
			return df0
		fmt_selector = len(testdate) # use value in middle to check.....
	except (ValueError, TypeError, KeyError) as e:
		logger.warning(f'fmtselector {e} {f=} {testdate=} {datecol=}')
		fmt_selector = len(df0[datecol][1])
		logger.warning(f'fmtselector {e} {f=} {testdate=} {datecol=} newfmt_selector:{fmt_selector} d: {len(data)} d0: {len(df0)} \n sample0 {df0[datecol][0]} samplefmt {df0[datecol][fmt_selector]} \n datacols={data.columns} \n df0cols={df0.columns}\n')
		# continue #
	try:
		match fmt_selector:
			case 20:
				fixed_datecol = pd.DataFrame({datecol:[datetime.strptime(k,fmt_20).astimezone(pytz.timezone('UTC')) for k in df0[datecol][1:] ]})
			case 24:
				fixed_datecol = pd.DataFrame({datecol:[datetime.strptime(k,fmt_24).astimezone(pytz.timezone('UTC')) for k in df0[datecol][1:] ]})
			case 26:
				fixed_datecol = pd.DataFrame({datecol:[datetime.strptime(k,fmt_26).astimezone(pytz.timezone('UTC')) for k in df0[datecol][1:] ]})
			case 28:
				fixed_datecol = pd.DataFrame({datecol:[datetime.strptime(k,fmt_28).astimezone(pytz.timezone('UTC')) for k in df0[datecol][1:] ]})
			case 30:
				fixed_datecol = pd.DataFrame({datecol:[datetime.strptime(k,fmt_30).astimezone(pytz.timezone('UTC')) for k in df0[datecol][1:] ]})
			case 34:
				fixed_datecol = pd.DataFrame({datecol:[datetime.strptime(k,fmt_34).astimezone(pytz.timezone('UTC')) for k in df0[datecol][1:] ]})
			case 36:
				fixed_datecol = pd.DataFrame({datecol:[datetime.strptime(k,fmt_36).astimezone(pytz.timezone('UTC')) for k in df0[datecol][1:] ]})
			case _:
				pass # logger.warning(f'could not match format for fmt_selector {fmt_selector} for {datecol} {f=}.\n sample:first= {df0[datecol][0]} middle= {df0[datecol][len(data)//2]} last= {df0[datecol][len(df0)-1]}\n')
	except (ValueError,TypeError,KeyError) as e:
		logger.error(f'datefix {type(e)} {e} {testdate=} {f=} datecol: {datecol} fmt: {fmt_selector} \n sample:first= {df0[datecol][0]} middle= {df0[datecol][len(data)//2]} \n') # last= {df0[datecol][len(df0)-1]}
	if not fixed_datecol.empty:
		df0 = data.drop(columns=datecol)
		df0 = pd.concat([df0,fixed_datecol])
		logger.info(f'replaced {datecol}')
	else:
		logger.warning(f'could not match format for fmt_selector {fmt_selector} for {datecol} {f=} {testdate=} .\n sample:first= {df0[datecol][0]} middle= {df0[datecol][len(data)//2]} ') # last= {df0[datecol][len(df0)-1]}\n')
	return df0


if __name__ == '__main__':
	pass
