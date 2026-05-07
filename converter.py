#!/usr/bin/python3
import asyncio
import argparse
import sys
import time
from hashlib import md5
from pathlib import Path
import pandas as pd
import polars as pl
from loguru import logger
from sqlalchemy import text
from sqlalchemy.exc import DataError, IntegrityError, OperationalError
from sqlalchemy.orm import sessionmaker, Session
import sqlite3
from datamodels import TorqFile, database_init, stable_fileid_from_csvhash
from utils import get_parser, get_engine_session, MIN_FILESIZE, convert_string_to_datetime, read_csvs_to_dataframe_and_insert
from schemas import canonicalize_columns

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


def _normalized_col_name(value: str) -> str:
	return "".join(ch.lower() for ch in str(value) if ch.isalnum())


def _resolve_col_name(columns: list[str], candidates: list[str]) -> str | None:
	if not columns:
		return None

	# Exact match first.
	for candidate in candidates:
		if candidate in columns:
			return candidate

	# Fallback to normalized matching for odd encodings/spaces/symbols.
	norm_map = {_normalized_col_name(col): col for col in columns}
	for candidate in candidates:
		resolved = norm_map.get(_normalized_col_name(candidate))
		if resolved:
			return resolved

	return None

async def read_csv_file(logfile:str, args:argparse.Namespace):
	"""
	Optimized version that combines filtering operations and reduces conversions
	"""
	nullvals = ['-','∞','340282346638528860000000000000000000000']
	try:
		# Use lazy evaluation to improve performance
		data = pl.scan_csv(logfile, ignore_errors=True, try_parse_dates=True, truncate_ragged_lines=True, null_values=nullvals)
		columns = data.columns

		time_col = _resolve_col_name(columns, ['gpstime', 'GPS_Time', 'GPS Time'])
		if not time_col:
			logger.warning(f"Skipping {logfile} - missing GPS time column")
			return pd.DataFrame()

		# Apply all filters in one operation
		data = data.filter((pl.col(time_col) != '-') & (pl.col(time_col) != 'GPS Time'))

		# Collect the data only once
		data = data.collect()

		# Early check for empty dataframe
		if data.is_empty():
			logger.warning(f'Empty dataset after filtering {logfile}')
			return pd.DataFrame()

		# Check trip duration more efficiently
		first_time = convert_string_to_datetime(data[time_col][0])
		last_time = convert_string_to_datetime(data[time_col][-1])
		if first_time and last_time:
			tripdur = (last_time - first_time).total_seconds()
		else:
			tripdur = 0

		if tripdur > 86400:
			logger.warning(f'Not Skipping {logfile} - trip duration too long: {tripdur}s')
			# return pd.DataFrame()

		# Check for duplicate trips in one database call
		session = get_engine_session(args)
		try:
			ts_temp = session.query(TorqFile).filter(TorqFile.trip_start == first_time).all()
			if ts_temp:
				logger.warning(f"Skipping {logfile} - already in db with trip_start: {first_time}")
				return pd.DataFrame()

			df = data.to_pandas()
			df = df.rename(columns=canonicalize_columns(list(df.columns)))

			return df
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

async def send_data_to_db(args: argparse.Namespace, data: pd.DataFrame, csvfilename: str, insertid: bool = True, readtime: float | None = None):
	"""
	send this csvdata to database, catch all exceptions in here
	return dict {'fileid': fileid, 'rows': len(data)}
	"""
	session = get_engine_session(args)
	csvhash = md5(open(csvfilename, "rb").read()).hexdigest()
	stable_fileid = stable_fileid_from_csvhash(csvhash)
	# fileinfo = {
	# 	'dtripstart': data['gpstime'][0],
	# 	'dtripend': data['gpstime'][len(data)-1],
	# 	'dlatstart': float(data['latitude'][0]),
	# 	'dlonstart': float(data['longitude'][0]),
	# 	'dlatend': float(data['latitude'][len(data)-1]),
	# 	'dlonend': float(data['longitude'][len(data)-1]),}
	# user only stem part of filename in db
	try:
		t = session.query(TorqFile).filter(TorqFile.csvhash == csvhash).first()
		if t is None:
			existing_by_id = session.query(TorqFile).filter(TorqFile.fileid == stable_fileid).first()
			if existing_by_id and existing_by_id.csvhash != csvhash:
				logger.error(
					f"stable fileid collision for {csvfilename}: fileid={stable_fileid} "
					f"existing_hash={existing_by_id.csvhash} new_hash={csvhash}"
				)
				return None
			t = TorqFile(csvfile=Path(csvfilename).parts[-1], csvhash=csvhash, fileid=stable_fileid)
			session.add(t)
			session.commit()
	except IntegrityError as e:
		# session.close()
		logger.error(f"{type(e)} {e} from {csvfilename}")
		return None
	# todropcols = []
	send_results = {'fileid': t.fileid, 'sent_rows': 0, 'readtime': readtime, 'sendtime': None}
	fileidcol = pd.DataFrame([t.fileid for k in range(len(data))], columns=["fileid",],)
	data = pd.concat((data, fileidcol), axis=1)

	try:
		send_started = time.perf_counter()
		# _ = data.to_sql("torqlogs", con=engine, if_exists="append", index=False)
		_ = data.to_sql("torqlogs", con=session.get_bind(), if_exists="append", index=False, method='multi', chunksize=args.sqlchunksize)
		send_results["sent_rows"] = session.execute(text(f"select count(*) from torqlogs where fileid={t.fileid} ; ")).one()[0]
		send_results["sendtime"] = float(time.perf_counter() - send_started)
		t.sent_rows = send_results["sent_rows"]
		t.readtime = readtime
		t.sendtime = send_results["sendtime"]
		session.add(t)
		session.commit()
		# logger.debug(f'fileid {t.fileid} sent {len(data)} rows to db  sent_rows: {send_results["sent_rows"]}')
	except DataError as e:
		logger.warning(f"{type(e)} {e.args[0]} {csvfilename=}")
	except (OperationalError, sqlite3.OperationalError,) as e:
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

async def get_files_to_send(session: Session, args):
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
		read_started = time.perf_counter()
		data = await read_csv_file(logfile=csvfilename, args=args)
		read_elapsed = float(time.perf_counter() - read_started)
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

		send_result = await send_data_to_db(args, data, csvfilename, readtime=read_elapsed)
		if metadata is not None and send_result is not None:
			metadata["readtime"] = read_elapsed
			metadata["sendtime"] = send_result.get("sendtime")
			metadata["sent_rows"] = send_result.get("sent_rows")
		# Return metadata with the result
		return {'file': csvfilename, 'result': send_result, 'metadata': metadata}
	except Exception as e:
		logger.error(f"Error processing {csvfilename}: {type(e)} {e}")
		return None

async def cli_main(args):
	if args.dbinfo:
		tables = ['columnstats', 'filestats', 'speeds', 'torqfiles', 'torqtrips', 'endpos', 'startpos', 'mapimagecache', 'torqlogs']
		print(f'checking {len(tables)} tables in {args.dbmode}')
		logcount = 0
		try:
			session = get_engine_session(args)  # , session
			with session.get_bind().connect() as conn:  # type: ignore
				for t in tables:
					try:
						count = conn.execute(text(f"select count(*) from {t}")).one()[0]
						print(f'{t}: {count}')
					except Exception as e:
						logger.error(f'Error counting {t}: {type(e)} {e}')
				# logcount = conn.execute(text("select count(*) from torqlogs")).all()
		except Exception as e:
			logger.error(f'error {type(e)} {e}')
			sys.exit(-1)
	elif args.scanpath:
		logger.debug(f'using {args.dbmode} database at {args.dbfile}')
		try:
			session = get_engine_session(args)  # , session
			database_init(session.get_bind())
			sess = sessionmaker(bind=session.get_bind())
			s = sess()
			logcount = s.execute(text("select count(*) from torqlogs")).all()
			logger.info(f'{logcount=}')
			s.close()
			read_csvs_to_dataframe_and_insert(args)
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
