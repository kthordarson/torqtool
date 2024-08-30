#!/usr/bin/python3
import argparse
import os
import re
import shutil
import sys
from datetime import datetime
from hashlib import md5
from pathlib import Path
import random
import pandas as pd
import polars as pl
import pytz
from loguru import logger
import sqlalchemy
from sqlalchemy import text
from sqlalchemy.exc import ArgumentError, DataError, IntegrityError, InternalError, OperationalError, ProgrammingError
from sqlalchemy.exc import DataError, OperationalError, NoResultFound
from sqlalchemy.orm.exc import DetachedInstanceError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import MultipleResultsFound
from psycopg2.errors import UndefinedColumn
import sqlite3
from commonformats import fmt_19, fmt_20, fmt_24, fmt_26, fmt_28, fmt_30, fmt_34, fmt_36
from datamodels import TorqFile, database_init
from schemas import schema_datatypes,s_columns, dataschema
from utils import get_parser, get_engine_session, MIN_FILESIZE, transfer_older_logs,get_sanatized_column_names, convert_string_to_datetime
from fixers import split_file, run_fixer, get_cols, check_and_fix_logs, replace_headers, fix_column_names
from updatetripdata import update_torqfile

pd.set_option("future.no_silent_downcasting", True)
# x = latitude y = longitude !

class Polarsreaderror(Exception):
	pass

# tool to rename and import tripLogs from older versions of the app
# get tripdate from profile.properties file and rename the log file to the new format
# tripdate should match with foldername of each trip, named as unix timestamp (13 digits)
# timestamp is the start time of the trip, first line of the log file
# example :
# original path /torq/tripLogs/1708245165793
# new filenames are in the format: trackLog-2021-Dec-01_23-40-45.csv
# datetime.fromtimestamp(1708245165793/1000).strftime("%Y-%b-%d_%H-%M-%S")

# move small logs
# for f in $(find /home/kth/development/torq/torqueLogs/ -type f ); do linecount=$(cat $f | wc -l); if [ $linecount -lt 10 ]; then echo "file $f lc=$linecount";fi;done;


def read_csv_file(logfile:str, args:argparse.Namespace):
	"""
	read csv file
	param: logfile = full path and name of file
	param: schema to use
	param: newcolumns = dict with sanatized column names, generated with new_columns_collector
	returns pandas dataframe, with sanatized column names
	raises Polarsreaderror if something goes wrong
	"""
	# todo handle missing gpstime, if not present, copy from devicetime
	t0 = datetime.now()
	nullvals = ['-','∞']
	try:
		data0 = pl.read_csv(logfile, ignore_errors=True, try_parse_dates=True, truncate_ragged_lines=False, n_threads=4, use_pyarrow=True, null_values=nullvals, schema=dataschema)  # , infer_schema=True
	except pl.exceptions.ShapeError as e:
		logger.error(f"{type(e)} {e} {logfile}")
		raise e
	except pl.exceptions.ComputeError as e:
		logger.error(f"{type(e)} {e} {logfile}")
		raise e
	except pl.exceptions.DuplicateError as e:
		msg = f"{type(e)} {e} {logfile}"
		logger.error(msg)
		raise e
	except pl.exceptions.NoDataError as e:
		msg = f"NoDataError {type(e)} {e} {logfile}"
		logger.error(msg)
		raise Polarsreaderror(msg)

	data = data0.filter(pl.col('gpstime') != '-')
	if len(data) != len(data0):
		logger.warning(f'filtered {len(data0)-len(data)} rows with - in gpstime {logfile}')
	dupe_rows = [(idx,k) for idx,k in enumerate(data['gpstime']) if 'GPS Time' in k]
	if len(dupe_rows) > 0:
		logger.warning(f"found {len(dupe_rows)} dupe GPS Time in {logfile} ")
	for row in dupe_rows:
		datebefore = convert_string_to_datetime(data['gpstime'][row[0]-1])
		dateafter = convert_string_to_datetime(data['gpstime'][row[0]+1])
		date_diff = (dateafter - datebefore).total_seconds()
		if date_diff > 60:  # todo check drop or fix file here ?
			logger.warning(f"skipping {logfile} {row} date_diff:{date_diff} dupe GPS Time ")
			return pd.DataFrame()
	data_filter = data.filter(pl.col('gpstime') != 'GPS Time')
	if len(data) != len(data_filter):
		logger.warning(f'filtered {len(data)-len(data_filter)} rows with - in GPS Time {logfile}')

	# check trip duration....
	# starttime = convert_string_to_datetime(data_filter['gpstime'][0])
	tripdur = (convert_string_to_datetime(data_filter['gpstime'][-1]) - convert_string_to_datetime(data_filter['gpstime'][0])).total_seconds()
	if tripdur > 86400:  # todo maybe split file here ?
		logger.warning(f'skipping {logfile} {tripdur=}')
		return pd.DataFrame()
	df = data_filter.to_pandas()
	return df

def send_data_to_db(args: argparse.Namespace,
	data: pd.DataFrame,
	csvfilename: str,
	insertid: bool = True,
):
	"""
	send this csvdata to database, catch all exceptions in here
	return dict {'fileid': fileid, 'rows': len(data)}
	"""
	engine, session = get_engine_session(args)
	csvhash = md5(open(csvfilename, "rb").read()).hexdigest()

	# user only stem part of filename in db
	try:
		t = TorqFile(csvfile=Path(csvfilename).parts[-1], csvhash=csvhash)
		session.add(t)
		session.commit()
	except IntegrityError as e:
		# session.close()
		logger.error(f"{type(e)} {e} from {csvfilename}")
		return None
	todropcols = []
	send_results = {'fileid': t.fileid, 'sent_rows': 0}
	fileidcol = pd.DataFrame([t.fileid for k in range(len(data))], columns=["fileid",],)
	data = pd.concat((data, fileidcol), axis=1)

	try:
		sr = data.to_sql("torqlogs", con=engine, if_exists="append", index=False)
		send_results["sent_rows"] = session.execute(text(f"select count(*) from torqlogs where fileid={t.fileid} ; ")).one()[0]
		logger.debug(f'{csvfilename=} {t.fileid=} sent {len(data)} rows to db  sentrows: {send_results["sent_rows"]}')
	except DataError as e:
		logger.warning(f"{type(e)} {e.args[0]} {csvfilename=}")
	except (sqlalchemy.exc.OperationalError, OperationalError, sqlite3.OperationalError,) as e:
		logger.error(f"{type(e)} {e} {csvfilename=}")
	except Exception as e:
		logger.error(f"unhandled {type(e)} {e} ")
	finally:
		session.close()
		return send_results

def get_files_to_send(session: sessionmaker, args):
	"""
	scan logpath for csv files, check if they have already been sent to data base
	returns list of files not in the database, filenames as str, NOT Path!
	# todo determine if file is split or not, if split, split it and send both parts
	# todo check if file has been imported, if not, import it
	# e.g. check filename and/or hash
	"""
	files_to_send = []
	unread_dbfiles = []
	csvfiles = [str(k) for k in Path(args.logpath).glob("**/trackLog-*.csv") if k.stat().st_size > MIN_FILESIZE]
	csvfiles.extend([str(k) for k in Path(args.logpath).glob("**/trackLog*.csv") if k.stat().st_size > MIN_FILESIZE])
	csvlist = [Path(k).parts[-1] for k in csvfiles]
	smallcsvfiles = [str(k) for k in Path(args.logpath).glob("trackLog-*.csv") if k.stat().st_size < MIN_FILESIZE]
	try:
		dbfilecount = session.query(TorqFile).count()
		alldbfiles = session.query(TorqFile).all()
		dbfnlist = []  # [k.csvfile for k in alldbfiles]
		# files_to_send = [k for k in csvlist if not k in dbfnlist]
		# files_to_send = [f'{args.logpath}/{k}' for k in csvlist if k not in dbfnlist]
		files_to_send = [k for k in csvfiles if Path(k).parts[-1] not in dbfnlist]
		skipped_count = len([k for k in csvlist if k in dbfnlist])
		unread_dbfiles = (session.query(TorqFile).filter(TorqFile.read_flag == 0).filter(TorqFile.error_flag == 0).all())
		read_dbfiles = session.query(TorqFile).filter(TorqFile.read_flag == 1).all()
		error_dbfiles = session.query(TorqFile).filter(TorqFile.error_flag != 0).all()
		# files_to_send = set(csvfiles) - set([k.csvfile for k in read_dbfiles])
	except UndefinedColumn as e:
		logger.warning(f"UndefinedColumn {type(e)} {e} from {args.logpath} args {args}")
	except Exception as e:
		logger.error(f"unhandled {type(e)} {e} from {args.logpath} args {args}")
	finally:
		session.close()
		if len(files_to_send) > 0:
			logger.info(f"found {len(files_to_send)} files_to_send, dupeskips: {skipped_count} skipping {len(smallcsvfiles)} files under MIN_FILESIZE - unread_dbfiles:{len(unread_dbfiles)} error_dbfiles: {len(error_dbfiles)} all_db_files:{dbfilecount}")
		else:
			logger.warning(f"no valid files found in {args.logpath} ! dupeskips: {skipped_count} dbfiles: all= {dbfilecount} / unread= {len(unread_dbfiles)} csvfiles:{len(csvfiles)} ... Exit!")
			sys.exit(-1)

		files_to_send = sorted(files_to_send)  # sort by filename (date)
		if args.samplemode:
			# select small number of random logs
			files_to_send = [random.choice(files_to_send) for k in range(random.randint(10, 30))]
		return files_to_send

def date_column_fixer(data: pd.DataFrame = None, datecol: str = None, f: str = None):
	"""
	fixes timedatestamp format in dataframe
	param: data Dataframe, datecol name of date column, f filename (for ref)
	"""
	testdate = data[datecol][len(data) // 2]
	if testdate == 0:
		errmsg = f"invalid testdate {testdate} {datecol=} datalen: {len(data)} f: {f}"
		logger.error(errmsg)
		raise TypeError(errmsg)
	try:
		fmt_selector = len(testdate)  # use value in middle to check.....
	except TypeError as e:
		logger.error(f"datefix {type(e)} {e}  {type(datecol)} datecol: {datecol} {testdate=}")
		raise e
	fixed_datecol = data[datecol]  # copy pd.DataFrame()
	# chk = [k for k in fixed_datecol if isinstance(k,str) and '-' in k]
	# chk_g = [k for k in fixed_datecol if isinstance(k,str) and 'G' in k]
	# if len(chk)>0:
	# logger.warning(f'CHECK {datecol=} {len(chk)}/{len(fixed_datecol)} chk_g:{len(chk_g)} things in {f} ')
	# fixed_datecol = fixed_datecol.replace('-',0) # copy pd.DataFrame()
	try:
		match fmt_selector:
			case 19:
				fixed_datecol = pd.DataFrame({datecol: [datetime.strptime(k, fmt_19).astimezone(pytz.timezone("UTC")) for k in fixed_datecol if isinstance(k, str)]})
			case 20:
				fixed_datecol = pd.DataFrame({datecol: [datetime.strptime(k, fmt_20).astimezone(pytz.timezone("UTC")) for k in fixed_datecol if isinstance(k, str)]})
			case 24:
				fixed_datecol = pd.DataFrame({datecol: [datetime.strptime(k, fmt_24).astimezone(pytz.timezone("UTC")) for k in fixed_datecol if isinstance(k, str) and 'time' not in k.lower()]})
			case 26:
				fixed_datecol = pd.DataFrame({datecol: [datetime.strptime(k, fmt_26).astimezone(pytz.timezone("UTC")) for k in fixed_datecol if isinstance(k, str)]})
			case 28:
				fixed_datecol = pd.DataFrame({datecol: [datetime.strptime(k, fmt_28).astimezone(pytz.timezone("UTC")) for k in fixed_datecol if isinstance(k, str) and 'time' not in k.lower()]})
			case 30:
				fixed_datecol = pd.DataFrame({datecol: [datetime.strptime(k, fmt_30).astimezone(pytz.timezone("UTC")) for k in fixed_datecol if isinstance(k, str)]})
			case 34:
				fixed_datecol = pd.DataFrame({datecol: [datetime.strptime(k, fmt_34).astimezone(pytz.timezone("UTC")) for k in fixed_datecol if isinstance(k, str)]})
			case 36:
				fixed_datecol = pd.DataFrame({datecol: [datetime.strptime(k, fmt_36).astimezone(pytz.timezone("UTC")) for k in fixed_datecol if isinstance(k, str)]})
			case _:
				pass  # logger.warning(f'could not match format for fmt_selector {fmt_selector} for {datecol} {f=}.\n sample:first= {df0[datecol][0]} middle= {df0[datecol][len(data)//2]} last= {df0[datecol][len(df0)-1]}\n')
	except (ValueError, TypeError, KeyError) as e:
		logger.error(f"datefix {type(e)} {e} data: {type(data)} {data} dc: {type(datecol)} datecol: {datecol} fmt: {fmt_selector} \n sample:first= {data[datecol][0]} middle= {data[datecol][len(data)//2]} \n")  # last= {df0[datecol][len(df0)-1]}
		return data[datecol]
	# chk = [k for k in fixed_datecol[datecol] if '-' in k]
	# fixed_datecol[datecol] = fixed_datecol[datecol].replace('-',0)
	# chk = [k for k in fixed_datecol[datecol] if isinstance(k,str)]
	# chk3 = [k for k in fixed_datecol[datecol] if isinstance(k,str)]
	# if len(chk)>0:
	# logger.warning(f'CHECK2 {len(chk)} things in {f} {datecol=}')
	return fixed_datecol


def split_check(csvfile: str):
	"""
	check if file is damaged and needs splitting...
	todo write splitter and refresh file list .....
	"""
	try:
		with open(csvfile, "r") as f:
			data = f.readlines()
	except FileNotFoundError as e:
		logger.error(f"{e} {csvfile} ...")
		raise e
	gpscount = 0
	for line in data[1:]:
		if "gps" in line.lower():
			gpscount += 1
	if gpscount > 0:
		# logger.warning(f'splits: {gpscount=} in {csvfile} ')
		return True
	return False


def data_fixer(data: pd.DataFrame, f):
	# check if file needs splitting
	# fix dates
	newdatecol = pd.DataFrame()
	# fixed_data = pd.DataFrame()
	date_columns = ["gpstime", "devicetime"]
	droprows = [idx for idx, k in enumerate(data.gpstime) if k == "-"]
	if len(droprows) > 0:
		data = data.drop(droprows)
		logger.warning(f"dropping invalid gpstimerows {droprows} from {f}")
	for col in date_columns:
		try:
			newdatecol = date_column_fixer(data=data, datecol=col, f=f)
		except TypeError as e:
			logger.error(f"datafixer {type(e)} {e} {f} {col=}")
			raise e
		except AttributeError as e:
			logger.error(f"datafixer {type(e)} {e} {f} {col=}")
			raise e
		# fix bad values
		if not newdatecol.empty:
			try:
				data[col] = newdatecol
			except AttributeError as e:
				logger.error(f"datafixer {type(e)} {e} {f} {col=}")
				raise e
		else:
			logger.warning(f"empty column {col}")
			# logger.info(f'fixed {col} in {f}')
	return data  # fixed_data  if not fixed_data.empty else data

def cli_main(args):
	if args.dbinfo:
		logcount = 0
		try:
			engine, session = get_engine_session(args)
			logcount = session.execute(text(f"select count(*) from torqlogs"))
		except Exception as e:
			logger.error(f'error {type(e)} {e}')
			sys.exit(-1)
		finally:
			logger.info(f'{logcount=}')
	elif args.scanpath:
		# first collect sanatized column headers
		# ncc, errorfiles = new_columns_collector(logdir=args.logpath)
		# check if any of the files in args.logpath have been read, skip these
		# todo getfiles to read
		engine, session = get_engine_session(args)
		try:
			database_init(engine)
		except AssertionError as e:
			logger.error(f"[maindbinit] {e} exit")
			sys.exit(-1)

		with session.no_autoflush:
			newfiles = get_files_to_send(session, args=args)
		if args.db_limit:
			logger.debug(f'{args=}')
			newfiles = newfiles[: int(args.db_limit)]
		for idx, csvfilename in enumerate(newfiles):
			readstart = datetime.now()
			logger.debug(f'[{idx}/{len(newfiles)}] reading {Path(csvfilename).name} {Path(csvfilename).stat().st_size} bytes')
			try:
				data = read_csv_file(logfile=csvfilename, args=args)
			except Polarsreaderror as e:
				logger.error(f"polarsreaderror {type(e)} {e} for {csvfilename}")
				continue
			except Exception as e:
				logger.error(f"unhandled {type(e)} {e} for {csvfilename}")
				continue
			if len(data) == 0:
				logger.warning(f'no data in {csvfilename}')
				continue
			# if read ok, send data
			readt = (datetime.now()-readstart).total_seconds()
			# logger.info(f'[{idx}/{len(newfiles)}] readt: {readt}')
			sendstart = datetime.now()
			try:
				send_result = send_data_to_db(args, data, csvfilename)
			except Exception as e:
				logger.error(f"unhandled {type(e)} {e} for {csvfilename}")
				continue
			if send_result['sent_rows'] == 0:
				logger.warning(f'[{idx}/{len(newfiles)}] sent_rows = 0 {Path(csvfilename).name} {Path(csvfilename).stat().st_size} t: {datetime.now()-sendstart} res: {send_result}')
			else:
				logger.info(f'[{idx}/{len(newfiles)}] send {Path(csvfilename).name} {Path(csvfilename).stat().st_size} t: {datetime.now()-sendstart} res: {send_result["sent_rows"]}')
				# update torqfiles db with stats
				sendt = (datetime.now()-sendstart).total_seconds()
				fileinfo = {
					'fileid': send_result['fileid'],
					'sent_rows': send_result['sent_rows'],
					'sendtime': sendt,
					'readtime': readt,
					'dtripstart': data['gpstime'][0],
					'dtripend': data['gpstime'][len(data)-1],
					'dlatstart': float(data['latitude'][0]),
					'dlonstart': float(data['longitude'][0]),
					'dlatend': float(data['latitude'][len(data)-1]),
					'dlonend': float(data['longitude'][len(data)-1]),
				}
				session.close()
				try:
					upchk = update_torqfile(args, fileinfo)
					logger.info(f'[{idx}/{len(newfiles)}] updone: {upchk} tr: {datetime.now()-readstart} ts: {datetime.now()-sendstart} rtst:{readt}/{sendt}')
				except ValueError as e:
					logger.error(f"update_torqfile {type(e)} {e} for {csvfilename}")
		sys.exit(0)
	if args.fixer:
		# fixer mode
		# read all log files, fix bad chars, remove them
		# update database, mark the log file as fixed
		# TorqFile.fixed_flag = True
		# check_and_fix_logs(logfiles)
		run_fixer(args)
		sys.exit(0)
	if args.getcols:
		# get columns from all log files in the path
		stats, columns = get_cols(args.logpath, debug=args.debug)
		# columns = sorted(columns, key=lambda x: columns[x]['count'], reverse=True)
		columns = sorted(columns, key=lambda x: (columns[x]["count"], columns[x]), reverse=True)
		for c in columns:
			if "date" in c or "time" in c:
				lineout = f"{c} = Column('{c}', DateTime)"
			else:
				lineout = f"{c} = Column('{c}', DOUBLE)"  # Column('longitude', DOUBLE)
			logger.debug(f'{lineout=}')
		# print(f'{columns=}')
		sys.exit(0)
	if args.transfer:
		# oldlogpath root of the old tripLogs files, containing subfolder, each name as unix timestamp of the trip
		# each sub folder contains a log file and profile.properties file
		# step one, transfer older logs to new location with new filenames
		new_old_logs = transfer_older_logs(args)
		logger.debug(f"transfered {len(new_old_logs)} old logs")
		# step two, read each log file, remove bad chars
		fixed = check_and_fix_logs(new_old_logs, args)
		logger.debug(f"{fixed=}")
		sys.exit(0)


def get_args(appname):
	parser = get_parser(appname)

	args = parser.parse_args()
	return args


def main():
	args = get_args(appname="converter")
	cli_main(args)


if __name__ == "__main__":
	main()
