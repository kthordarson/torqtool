#!/usr/bin/python3
import asyncio
import argparse
import sys
from hashlib import md5
from pathlib import Path
import pandas as pd
import polars as pl
from loguru import logger
import sqlalchemy
from sqlalchemy import text
from sqlalchemy.exc import DataError, IntegrityError, OperationalError
from sqlalchemy.orm import sessionmaker
import sqlite3
from datamodels import TorqFile, database_init
from utils import get_parser, get_engine_session, MIN_FILESIZE, convert_string_to_datetime, read_csvs_to_dataframe_and_insert

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

async def read_csv_file(logfile:str, args:argparse.Namespace):
	"""
	Optimized version that combines filtering operations and reduces conversions
	"""
	nullvals = ['-','∞','340282346638528860000000000000000000000']
	try:
		# Use lazy evaluation to improve performance
		data = pl.scan_csv(logfile, ignore_errors=True, try_parse_dates=True, truncate_ragged_lines=True, null_values=nullvals)

		# Apply all filters in one operation
		data = data.filter((pl.col('gpstime') != '-') & (pl.col('gpstime') != 'GPS Time'))

		# Collect the data only once
		data = data.collect()

		# Early check for empty dataframe
		if data.is_empty():
			logger.warning(f'Empty dataset after filtering {logfile}')
			return pd.DataFrame()

		# Check trip duration more efficiently
		first_time = convert_string_to_datetime(data['gpstime'][0])
		last_time = convert_string_to_datetime(data['gpstime'][-1])
		tripdur = (last_time - first_time).total_seconds()

		if tripdur > 86400:
			logger.warning(f'Not Skipping {logfile} - trip duration too long: {tripdur}s')
			# return pd.DataFrame()

		# Check for duplicate trips in one database call
		engine, session = get_engine_session(args)
		try:
			ts_temp = session.query(TorqFile).filter(TorqFile.trip_start == first_time).all()
			if ts_temp:
				logger.warning(f"Skipping {logfile} - already in db with trip_start: {first_time}")
				return pd.DataFrame()

			return data.to_pandas()
		finally:
			session.close()

	except (pl.exceptions.ShapeError,
			pl.exceptions.ComputeError,
			pl.exceptions.DuplicateError) as e:
		logger.error(f"{type(e)} {e} {logfile}")
		raise e
	except pl.exceptions.NoDataError as e:
		msg = f"NoDataError {type(e)} {e} {logfile}"
		logger.error(msg)
		raise Polarsreaderror(msg)

async def send_data_to_db(args: argparse.Namespace, data: pd.DataFrame, csvfilename: str, insertid: bool = True):
	"""
	send this csvdata to database, catch all exceptions in here
	return dict {'fileid': fileid, 'rows': len(data)}
	"""
	engine, session = get_engine_session(args)
	csvhash = md5(open(csvfilename, "rb").read()).hexdigest()
	# fileinfo = {
	# 	'dtripstart': data['gpstime'][0],
	# 	'dtripend': data['gpstime'][len(data)-1],
	# 	'dlatstart': float(data['latitude'][0]),
	# 	'dlonstart': float(data['longitude'][0]),
	# 	'dlatend': float(data['latitude'][len(data)-1]),
	# 	'dlonend': float(data['longitude'][len(data)-1]),}
	# user only stem part of filename in db
	try:
		t = TorqFile(csvfile=Path(csvfilename).parts[-1], csvhash=csvhash)
		session.add(t)
		session.commit()
	except IntegrityError as e:
		# session.close()
		logger.error(f"{type(e)} {e} from {csvfilename}")
		return None
	# todropcols = []
	send_results = {'fileid': t.fileid, 'sent_rows': 0}
	fileidcol = pd.DataFrame([t.fileid for k in range(len(data))], columns=["fileid",],)
	data = pd.concat((data, fileidcol), axis=1)

	try:
		# _ = data.to_sql("torqlogs", con=engine, if_exists="append", index=False)
		_ = data.to_sql("torqlogs", con=engine, if_exists="append", index=False, method='multi', chunksize=args.sqlchunksize)
		send_results["sent_rows"] = session.execute(text(f"select count(*) from torqlogs where fileid={t.fileid} ; ")).one()[0]
		# logger.debug(f'fileid {t.fileid} sent {len(data)} rows to db  sent_rows: {send_results["sent_rows"]}')
	except DataError as e:
		logger.warning(f"{type(e)} {e.args[0]} {csvfilename=}")
	except (sqlalchemy.exc.OperationalError, OperationalError, sqlite3.OperationalError,) as e:
		logger.error(f"{type(e)} {e} {csvfilename=}")
	except Exception as e:
		logger.error(f"unhandled {type(e)} {e} ")
	finally:
		session.close()
		return send_results

async def calculate_hash(path):
	"""Calculate MD5 hash of a file asynchronously"""
	loop = asyncio.get_running_loop()
	return await loop.run_in_executor(
		None,
		lambda: md5(open(path, "rb").read()).hexdigest()
	)

async def get_files_to_send(session: sessionmaker, args):
	"""More efficient file processing that caches hashes using async"""
	# Get all hashes from database in one query
	alldbfiles = session.query(TorqFile).all()
	hashlist = set([k.csvhash for k in alldbfiles])  # Use set for O(1) lookups

	# Get all CSV files first
	csv_paths = list(Path(args.logpath).glob("**/trackLog*.csv"))
	logger.info(f"Found {len(csv_paths)} CSV files to process")

	# Filter by size first to avoid unnecessary hash calculations
	csv_paths = [p for p in csv_paths if p.stat().st_size > MIN_FILESIZE]
	logger.info(f"{len(csv_paths)} files exceed minimum size")

	# Calculate hashes concurrently
	tasks = [calculate_hash(path) for path in csv_paths]
	file_hashes = await asyncio.gather(*tasks)

	# Filter files that aren't in database
	result = []
	for path, file_hash in zip(csv_paths, file_hashes):
		if file_hash not in hashlist:
			result.append(str(path))

	return result

async def process_batch(batch_files, args):
	tasks = []
	for csvfilename in batch_files:
		tasks.append(process_single_file(csvfilename, args))
	return await asyncio.gather(*tasks, return_exceptions=True)

async def process_single_file(csvfilename, args):
	try:
		data = await read_csv_file(logfile=csvfilename, args=args)
		if len(data) == 0:
			logger.warning(f'no data in {csvfilename}')
			return None

		# Extract metadata here once
		metadata = None
		if not data.empty:
			metadata = {
				'dtripstart': data['gpstime'][0],
				'dtripend': data['gpstime'][len(data)-1],
				'dlatstart': float(data['latitude'][0]),
				'dlonstart': float(data['longitude'][0]),
				'dlatend': float(data['latitude'][len(data)-1]),
				'dlonend': float(data['longitude'][len(data)-1]),
			}

		send_result = await send_data_to_db(args, data, csvfilename)
		# Return metadata with the result
		return {'file': csvfilename, 'result': send_result, 'metadata': metadata}
	except Exception as e:
		logger.error(f"Error processing {csvfilename}: {type(e)} {e}")
		return None

async def cli_main(args):
	if args.dbinfo:
		logcount = 0
		try:
			engine = get_engine_session(args)  # , session
			with engine.connect() as conn:
				logcount = conn.execute(text("select count(*) from torqlogs")).all()
		except Exception as e:
			logger.error(f'error {type(e)} {e}')
			sys.exit(-1)
		finally:
			logger.info(f'{logcount=}')
	elif args.scanpath:
		try:
			engine = get_engine_session(args)  # , session
			database_init(engine)
			sess = sessionmaker(bind=engine)
			s = sess()
			logcount = s.execute(text("select count(*) from torqlogs")).all()
			logger.info(f'{logcount=}')
			s.close()
			read_csvs_to_dataframe_and_insert(args)
			# sess = sessionmaker(bind=engine)
			# s = sess()
			# populate_trips_and_update_files(s)
			# s.close()
			# _ = combined_df.to_sql("torqlogs", con=engine, if_exists="append", index=False, method='multi', chunksize=args.sqlchunksize)
			# send_result = await send_data_to_db(args, data, csvfilename)
		except Exception as e:
			logger.error(f'error {type(e)} {e}')
			sys.exit(-1)

def get_args(appname):
	parser = get_parser(appname)

	args = parser.parse_args()
	return args


def main():
	args = get_args(appname="converter")
	asyncio.run(cli_main(args))
	# cli_main(args)


if __name__ == "__main__":
	main()
