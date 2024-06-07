# utils and db things here

import os
import re
from datetime import datetime
from hashlib import md5
from pathlib import Path

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
from sqlalchemy import create_engine
from sqlalchemy.exc import (DataError, IntegrityError, InternalError, OperationalError, ProgrammingError)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session
from datamodels import (TorqFile, Torqtrips, Torqlogs, Torqdata, database_dropall, database_init, send_torqfiles, send_torqtrips)
from updatetripdata import send_torqdata, send_torqdata_ppe

MIN_FILESIZE = 2500



def replace_all(text, dic):
	for i, j in dic.items():
		textout = text.replace(i, j)
	if text != textout:
		logger.warning(f'{text} -> {textout}')
	return textout

def fix_csv_file(tf, debug=True):
	# read csv file, replace badvals and fix column names
	# returns a buff with the fixed csv file
	badvals = {
			#'-': '0',
			"'-'": '0',
			'"-"': '0',
			',-,': ',0,',
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
	with open(tf, 'r') as reader:
		data0 = reader.readlines()
		data = data0[1:] # skip first line, fix column names later....
		#lines0 = [k for k in data if not k.startswith('-')]
		lines = [replace_all(b, badvals) for b in data]
		orgcol = data[0].split(',')
		newcolname = ','.join([re.sub(r'\W', '', col) for col in orgcol]).encode('ascii', 'ignore').decode()
		newcolname += '\n'
		newcolname = newcolname.lower()
		# column_count = newcolname.count(',')
		lines[0] = newcolname
	return lines

def get_csv_files(searchpath: Path,  dbmode=None, debug=False):
	# scan searchpath for csv files
	torqcsvfiles = [({
		'csvfilename': k, # original csv file
		'csvfilefixed': f'{k}.fixed.csv', # fixed csv file
		'size': os.stat(k).st_size,
		'dbmode': dbmode}) for k in searchpath.glob("**/trackLog.csv") if k.stat().st_size >= MIN_FILESIZE] # and not os.path.exists(f'{k}.fixed.csv')]
	return torqcsvfiles

def fix_csv(tf):
	torqcsvfiles = {}
	fixedlines = fix_csv_file(tf)
	if debug:
		logger.debug(f'fixed {tf} {len(fixedlines)}')
	with open(file=tf['csvfilefixed'], mode='w', encoding='utf-8', newline='') as writer:
		writer.writelines(fixedlines)
	if debug:
		logger.debug(f'[gcv] {idx}/{len(torqcsvfiles)} {tf["csvfilefixed"]} saved')

	csvhash = md5(open(torqcsvfiles[idx]['csvfilename'], 'rb').read()).hexdigest()
	fixedhash = md5(open(torqcsvfiles[idx]['csvfilefixed'], 'rb').read()).hexdigest()
	torqcsvfiles[idx] = {
		'csvfilename': tf['csvfilename'],
		'csvfilefixed' : tf['csvfilefixed'],
		'csvhash': csvhash,
		'fixedhash': fixedhash,
		'dbmode': dbmode,
	}

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




def read_buff(tf_csvfile, tf_fileid, tf_tripid, debug=False):
	if not 'fixed' in tf_csvfile:
		logger.warning(f'[rb] {tf_csvfile} is not fixed')
	try:
		torqbuffer = read_csv_polars(tf_csvfile, ignore_errors=True, try_parse_dates=True, use_pyarrow=True, null_values=['NaN','-','0\x88\x9e'])
	except ValueError as e:
		logger.error(f'[rb] {type(e)} {e} csvfile={tf_csvfile}')
		return None
	except ComputeError as e:
		logger.error(f'[rb] {type(e)} {e} csvfile={tf_csvfile}')
		return None
	for column in torqbuffer.columns: # replace - with 0
		mapping = {'-': 0}
		try:
			if '-' in torqbuffer[column]:
				torqbuffer = torqbuffer.with_columns(mapping_replace(column,mapping))
		except ComputeError as e:
			logger.error(f'[rb] {type(e)} {e} csvfile={tf_csvfile} column={k}')
	if torqbuffer.is_empty():
		logger.error(f'[rb] torqbuffer is empty {tf_csvfile}')
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
	except ValueError as e:
		logger.error(f'[rb] devicetime {type(e)} {e} csvfile: {tf_csvfile}')
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
		logger.error(f'[rb] {type(e)} {e} csvfile: {tf_csvfile}')

	torqbuffer = torqbuffer.drop('devicetime')
	torqbuffer = torqbuffer.drop('gpstime')
	torqbuffer.insert_at_idx(3, gpstime)
	torqbuffer.insert_at_idx(4, devicetime)

	resultbuffer = {
		'torqbuffer' : torqbuffer,
		'fileid' : tf_fileid,
		'tripid' : tf_tripid,
		'tf_csvfile' : tf_csvfile,
	}
	return resultbuffer


def sqlsender(buffer, dburl, debug=False):
	engine = create_engine(url=dburl, echo=False)
	Session = sessionmaker(bind=engine)
	session = Session()
	results = {
		'fileid': buffer['fileid'],
		'tripid': buffer['tripid'],
		'tf_csvfile': buffer['tf_csvfile'],
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
			logger.warning(f'[tosql] {newcol=} code={e} args={e.args} r={results} tf_csvfile={buffer["tf_csvfile"]}')  # error:{e}
		elif e.code == 'e3q8' and 'database is locked' in e.args[0]:
			logger.warning(f'[tosql] {newcol=} code={e} args={e.args} r={results} tf_csvfile={buffer["tf_csvfile"]}')  # error:{e}
		else:
			logger.error(f'[tosql] code={e} r={results} tf_csvfile={buffer["tf_csvfile"]}')  # error:{e}
			results['status'] = 'error'
	except InternalError as e:
		logger.error(f'[tosql] InternalError {e} r={results} tf_csvfile={buffer["tf_csvfile"]}')
		results['status'] = 'error'
	except IntegrityError as e:
		logger.warning(f'[tosql] {type(e)} code={e} args={e.args[0]} r={results} tf_csvfile={buffer["tf_csvfile"]}')
		results['status'] = 'error'
		# logger.warning(f'[tosql] {e.statement} {e.params}')
		# logger.warning(f'[tosql] {e}')
	except (pymysql.err.DataError, DataError) as e:
		# r={'fileid': 156, 'tripid': 156, 'status': 'unknown'}
		#logger.error(f'[!]{type(e)}\n{e}\n')
		tf_csvfile = buffer['tf_csvfile'] # session.query(TorqFile).filter(TorqFile.id == results['fileid']).first()
		errmsg = e.args[0]
		err_row = errmsg.split('row')[-1].strip()
		err_row = errmsg.split(',')[1].split('at row')[1].strip().strip('")')
		if 'Incorrect double value' in errmsg:
			err_col = errmsg.split()[8].split('.')[2].strip("`")
		else:
			err_col = errmsg.split(',')[1].split('at row')[0].split("'")[1]
		# logger.warning(f'\n[tosql] code={e}\nargs={e.args[0]}\nr={results}\nerr_row: {err_row}\nerr_col:{err_col}\ntorqfile={tf_err} tf_csvfile={buffer["tf_csvfile"]}\n')  # error:{e}
		logger.warning(f'\n[tosql] {type(e)} code={e} err_row: {err_row} err_col:{err_col} torqfile={tf_csvfile} fileid:{buffer["fileid"]}')  # error:{e}
		# tmpbuf = tmpbuf.drop(columns=err_col)
		err_row = int(err_row)
		try:
			tmpbuf = tmpbuf.drop(index=err_row)
		except Exception as exc:
			logger.error(f'[torql] {type(exc)} {exc} err_row: {err_row} err_col:{err_col} torqfile={tf_csvfile} fileid:{buffer["fileid"]}')
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
		'tf_csvfile': buffer['tf_csvfile'],
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
			logger.warning(f'[tosql] {newcol=} code={e} args={e.args} r={results} tf_csvfile={buffer["tf_csvfile"]}')  # error:{e}
		elif e.code == 'e3q8' and 'database is locked' in e.args[0]:
			logger.warning(f'[tosql] {newcol=} code={e} args={e.args} r={results} tf_csvfile={buffer["tf_csvfile"]}')  # error:{e}
		else:
			logger.error(f'[tosql] code={e} r={results} tf_csvfile={buffer["tf_csvfile"]}')  # error:{e}
			results['status'] = 'error'
	except InternalError as e:
		logger.error(f'[tosql] InternalError {e} r={results} tf_csvfile={buffer["tf_csvfile"]}')
		results['status'] = 'error'
	except IntegrityError as e:
		logger.warning(f'[tosql] {type(e)} code={e} args={e.args[0]} r={results} tf_csvfile={buffer["tf_csvfile"]}')
		results['status'] = 'error'
		# logger.warning(f'[tosql] {e.statement} {e.params}')
		# logger.warning(f'[tosql] {e}')
	except (pymysql.err.DataError, DataError) as e:
		# r={'fileid': 156, 'tripid': 156, 'status': 'unknown'}
		#logger.error(f'[!]{type(e)}\n{e}\n')
		tf_csvfile = buffer['tf_csvfile'] # session.query(TorqFile).filter(TorqFile.id == results['fileid']).first()
		errmsg = e.args[0]
		err_row = errmsg.split('row')[-1].strip()
		err_row = errmsg.split(',')[1].split('at row')[1].strip().strip('")')
		if 'Incorrect double value' in errmsg:
			err_col = errmsg.split()[8].split('.')[2].strip("`")
		else:
			err_col = errmsg.split(',')[1].split('at row')[0].split("'")[1]
		# logger.warning(f'\n[tosql] code={e}\nargs={e.args[0]}\nr={results}\nerr_row: {err_row}\nerr_col:{err_col}\ntorqfile={tf_err} tf_csvfile={buffer["tf_csvfile"]}\n')  # error:{e}
		logger.warning(f'\n[tosql] {type(e)} code={e} err_row: {err_row} err_col:{err_col} torqfile={tf_csvfile} fileid:{buffer["fileid"]}')  # error:{e}
		# tmpbuf = tmpbuf.drop(columns=err_col)
		err_row = int(err_row)
		try:
			tmpbuf = tmpbuf.drop(index=err_row)
		except Exception as exc:
			logger.error(f'[torql] {type(exc)} {exc} err_row: {err_row} err_col:{err_col} torqfile={tf_csvfile} fileid:{buffer["fileid"]}')
		try:
			#tmpbuf.to_sql('torqlogs', con=engine, if_exists='append', index=False)
			results['status'] = 'warning'
		except (IndexError, KeyError, DataError) as ex:
			errmsg = ex.args[0]
			logger.error(f'[!] {type(ex)}\nerrmsg: {errmsg}\n')
	except (TypeError, ValueError) as e:
		logger.error(f'[!]{type(e)}\n{e}\n')
	return results


def torq_worker(tf, dburl, dbcols, debug=False):
	buffer = None
	results = None
	datares = None
	t0 = datetime.now()
	try:
		buffer = read_buff(tf.csvfilefixed, tf.id, tf.tripid, debug=debug)
		if debug:
			logger.debug(f'file {tf.csvfilefixed} buffer: {len(buffer["torqbuffer"])}')
	except (ValueError, TypeError, PicklingError, ComputeError) as e:
		logger.error(f'[!] {type(e)} {e} in read_buff {tf.csvfilefixed}')
		return None
	try:
		results = sqlsender(buffer, dburl, debug=debug) # send triplog data
		if debug:
			logger.debug(f'file {tf.csvfilefixed} {results=}')
	except (ValueError, TypeError, PicklingError) as e:
		logger.error(f'[!] {type(e)} {e} in sqlsender buffer.is_empty() {buffer["torqbuffer"].is_empty()}')
		return None
	try:
		datares = send_torqdata(tf.id, dburl, debug=debug) # send trip data
		if debug:
			logger.debug(f'file {tf.csvfilefixed} {datares=}')
	except (ValueError, TypeError, PicklingError) as e:
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

async def torq_worker_ppe(tf, session, debug=False):
	buffer = None
	results = None
	datares = None
	t0 = datetime.now()
	try:
		buffer = read_buff(tf.csvfilefixed, tf.id, tf.tripid, debug=debug)
		if debug:
			logger.debug(f'file {tf.csvfilefixed} buffer: {len(buffer["torqbuffer"])}')
	except (ValueError, TypeError, PicklingError, ComputeError) as e:
		logger.error(f'[!] {type(e)} {e} in read_buff {tf.csvfilefixed}')
		return None
	try:
		results = sqlsender_ppe(buffer,session, debug=debug) # send triplog data
		if debug:
			logger.debug(f'file {tf.csvfilefixed} {results=}')
	except (ValueError, TypeError, PicklingError) as e:
		logger.error(f'[!] {type(e)} {e} in sqlsender buffer.is_empty() {buffer["torqbuffer"].is_empty()}')
		return None
	try:
		datares = send_torqdata_ppe(tf.id, session, debug=debug) # send trip data
		if debug:
			logger.debug(f'file {tf.csvfilefixed} {datares=}')
	except (ValueError, TypeError, PicklingError) as e:
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



def oldscanpath(engine, args):
	if args.threadmode == 'ppe': # ProcessPoolExecutor
		#Session = sessionmaker(bind=engine)
		#with Session() as session:
		with ProcessPoolExecutor(max_workers=cpu_count()) as executor:
			for idx, tf in enumerate(dbtorqfiles):
				t = session.query(TorqFile).filter(TorqFile.id == tf.id).first()
				tasks.append(executor.submit(torq_worker_ppe,t, session, args.debug))
				if args.debug:
					logger.debug(f'{idx}/{len(dbtorqfiles)} tasks={len(tasks)}')
	elif args.threadmode == 'oldppe': # ProcessPoolExecutor
		with ProcessPoolExecutor(max_workers=cpu_count()) as executor:
			for idx, tf in enumerate(dbtorqfiles):
				t = session.query(TorqFile).filter(TorqFile.id == tf.id).first()
				tasks.append(executor.submit(torq_worker,t, dburl, dbcols, args.debug))
				if args.debug:
					logger.debug(f'{idx}/{len(dbtorqfiles)} tasks={len(tasks)}')
	elif args.threadmode == 'tpe': # ThreadPoolExecutor
		with ThreadPoolExecutor(max_workers=cpu_count()) as executor:
			for idx, tf in enumerate(dbtorqfiles):
				t = session.query(TorqFile).filter(TorqFile.id == tf.id).first()
				tasks.append(executor.submit(torq_worker,t, dburl, dbcols, args.debug))
				if args.debug:
					logger.debug(f'{idx}/{len(dbtorqfiles)} tasks={len(tasks)}')
	total_time = 0
	for res in as_completed(tasks):
		try:
			r = res.result()
		except ProgrammingError as e:
			logger.error(f'[!] ProgrammingError {e} res:{res}')
		except EmptyDataError as e:
			logger.error(f'[!] EmptyDataError {e} res:{res}')
		except (ValueError, TypeError) as e:
			logger.error(f'[!] {type(e)} {e} res:{res}')
		except Exception as e:
			logger.error(f'[!] unhandledException {type(e)} {e} res:{res} {type(res)}')
		else:
			total_time += 1 #r['processing_time']
			if args.debug:
				logger.debug(f't={datetime.now() - t0} totaltime: {total_time:.2f}')
	logger.debug(f'[*] done t={datetime.now() - t0} total_time={total_time} threadmode={args.threadmode}')

