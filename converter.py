#!/usr/bin/python3
import argparse
import sys
from datetime import datetime
from hashlib import md5
from pathlib import Path
import pandas as pd
import polars as pl
from loguru import logger
import sqlalchemy
from sqlalchemy import text
from sqlalchemy.exc import  DataError, IntegrityError, OperationalError
from sqlalchemy.orm import sessionmaker
import sqlite3
from datamodels import TorqFile, database_init
from schemas import dataschema
from utils import get_parser, get_engine_session, MIN_FILESIZE, transfer_older_logs, convert_string_to_datetime
from fixers import run_fixer, get_cols, check_and_fix_logs
from updatetripdata import update_torqfile

pd.set_option("future.no_silent_downcasting", True)

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

# x = latitude y = longitude !

class Polarsreaderror(Exception):
	pass

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
		data0 = pl.read_csv(logfile, ignore_errors=True, try_parse_dates=True, truncate_ragged_lines=True, n_threads=4, use_pyarrow=True, null_values=nullvals, schema=dataschema)  # , infer_schema=True
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

	trip_start = convert_string_to_datetime(data_filter['gpstime'][0])
	engine, session = get_engine_session(args)
	ts_temp = session.query(TorqFile).filter(TorqFile.trip_start==trip_start).all()
	if len(ts_temp) > 0:
		logger.warning(f"skipping {logfile} already in db trip_start: {trip_start}")
		[logger.warning(f"\t{k.fileid=} trip_start: {k.trip_start}") for k in ts_temp]
		return pd.DataFrame()
	if tripdur > 86400:  # todo maybe split file here ?
		logger.warning(f'skipping {logfile} {tripdur=}')
		return pd.DataFrame()
	df = data_filter.to_pandas()

	# check if trip with same start time already in db
	# if so, skip this file
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
	fileinfo = {
		'dtripstart': data['gpstime'][0],
		'dtripend': data['gpstime'][len(data)-1],
		'dlatstart': float(data['latitude'][0]),
		'dlonstart': float(data['longitude'][0]),
		'dlatend': float(data['latitude'][len(data)-1]),
		'dlonend': float(data['longitude'][len(data)-1]),
		}
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

def gethash(filename: str):
	return md5(open(filename, "rb").read()).hexdigest()

def get_files_to_send(session: sessionmaker, args):
	"""
	scan logpath for csv files, check if they have already been sent to data base
	returns list of files not in the database, filenames as str, NOT Path!
	# todo determine if file is split or not, if split, split it and send both parts
	# todo check if file has been imported, if not, import it
	# e.g. check filename and/or hash
	"""
	alldbfiles = session.query(TorqFile).all()
	hashlist = [k.csvhash for k in alldbfiles]
	# csvfiles = [{'csvhash': gethash(k), 'csvfile':str(k)} for k in Path(args.logpath).glob("**/trackLog*.csv") if k.stat().st_size > MIN_FILESIZE]
	csvfiles = [{'csvhash': gethash(k), 'csvfile':str(k)} for k in Path(args.logpath).glob("**/trackLog*.csv") if k.stat().st_size > MIN_FILESIZE and gethash(k) not in hashlist]
	return [k['csvfile'] for k in csvfiles]

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
			except IndexError as e:
				logger.error(f"{type(e)} {e} for {csvfilename}")
				session.close()
				continue
			except Exception as e:
				logger.error(f"unhandled {type(e)} {e} for {csvfilename}")
				session.close()
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
