# utils and db things here
from math import radians, cos, sin, sqrt, atan2
import random
import os
import re
import sys
from datetime import datetime
from hashlib import md5
from pathlib import Path
import argparse
import pandas as pd
import pymysql
import pytz
from loguru import logger
from sqlalchemy import DateTime, Engine
from sqlalchemy import create_engine, text, MetaData, Table, Column, Float, String, Integer
from sqlalchemy.exc import ArgumentError, DataError,IntegrityError, InternalError, OperationalError, ProgrammingError
from sqlalchemy.orm import sessionmaker, Session
import sqlite3
from commonformats import fmt_20, fmt_24, fmt_26, fmt_28, fmt_30, fmt_34, fmt_36
from datamodels import database_init, COLUMN_TYPES

MIN_FILESIZE = 3000

def get_parser(appname):
	parser = argparse.ArgumentParser(description=appname)
	parser.add_argument("--scanpath", default=False, help="run scanpath", action="store_true", dest="scanpath", )
	parser.add_argument("--filestats", default=True, help="create filestats", action="store_true", dest="filestats", )
	parser.add_argument("--create-trips", default=False, help="create trip database", action="store_true", dest="create_trips", )
	parser.add_argument("--database_dropall", default=False, help="drop database", action="store_true", dest="database_dropall", )
	parser.add_argument("--dbhost", default="localhost", help="dbname", action="store")
	parser.add_argument("--dbmode", default="sqlite", help="sqlmode mysql/psql/sqlite/mariadb", action="store", dest="dbmode", )
	parser.add_argument("--dbname", default="torq", help="dbname", action="store")
	parser.add_argument("--dbpass", default="qrot", help="dbname", action="store")
	parser.add_argument("--dbuser", default="torq", help="dbname", action="store")
	parser.add_argument("--dbfile", default="torqdata.db", help="database file", action="store")
	parser.add_argument("--db_limit", default=False, help="db_limit", action="store", dest="db_limit")
	parser.add_argument("--file_limit", default=False, help="file_limit", action="store_true", dest="file_limit")
	parser.add_argument("--file", nargs="?", default=".", help="path to single csv file", action="store")
	parser.add_argument("--logpath", nargs="?", default=".", help="path to csv files", action="store")
	parser.add_argument("--sqlchunksize", nargs="?", default=1000, type=int, help="sql chunk", action="store")
	parser.add_argument("-i", "--info", "--dbinfo", default=False, help="show dbinfo", action="store_true", dest="dbinfo", )
	parser.add_argument("-d", "--debug", default=False, help="debugmode", action="store_true", dest="debug", )
	return parser

class TimeZoneAwareConstructorWarning:
	pass

def normalize_column_name(col):
	"""
	Normalize column names by stripping spaces, replacing multiple spaces, and removing problematic characters.
	"""
	col = str(col).strip()  # Convert to string and remove leading/trailing spaces
	col = re.sub(r'\s+', ' ', col)  # Replace multiple spaces with single space
	col = re.sub(r'[^\w\s]', '', col)  # Remove special characters (keep alphanumeric and spaces)
	return col.replace(' ', '_')

def get_table_columns(engine, table_name):
	"""
	Get the current columns of the table from the SQLite database.
	"""
	result = []
	with engine.connect() as conn:
		try:
			# your code here
			result = conn.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
		except Exception as e:
			logger.error(f"An error occurred: {e} {type(e)} while fetching columns for table {table_name}")
		return [row[1].lower() for row in result]  # Extract column names

def create_or_update_table(engine, table_name, columns, column_types):
	"""
	Create or update the table to include all provided columns.
	Handles duplicate columns and maintains existing schema.
	"""
	metadata = MetaData()

	# Check existing table columns and normalize to lowercase
	try:
		existing_columns = [col.lower() for col in get_table_columns(engine, table_name)]
		logger.debug(f"Existing columns: {len(existing_columns)}")
	except Exception as e:
		logger.error(f"Error checking existing columns: {e} {type(e)} table_name={table_name}")
		existing_columns = []

	# Create table definition with all columns
	table_columns = [Column(col, column_types.get(col, String)) for col in sorted(columns)]

	if not existing_columns:
		# Create new table if it doesn't exist
		logger.info(f"Creating new table {table_name} with {len(columns)} columns")
		Table(table_name, metadata, *table_columns, extend_existing=True)
		metadata.create_all(engine)
	else:
		# Add only new columns to existing table
		with engine.connect() as conn:
			# Convert all column names to lowercase for comparison
			new_columns = set(col.lower() for col in columns) - set(existing_columns)
			orig_col = ''
			if new_columns:
				logger.info(f"Adding {len(new_columns)} new columns to {table_name}")
				for col in new_columns:
					try:
						# Get original case version of column name
						orig_col = next(c for c in columns if c.lower() == col)
						sql_type = column_types.get(orig_col, String).__name__.lower()
						alter_sql = text(f'ALTER TABLE {table_name} ADD COLUMN "{orig_col}" {sql_type}')
						conn.execute(alter_sql)
						logger.debug(f"Added column: {orig_col} ({sql_type})")
					except sqlite3.OperationalError as e:
						if "duplicate column name" in str(e).lower():
							logger.debug(f"Column {orig_col} already exists, skipping")
							continue
						else:
							logger.warning(f"Could not add column {orig_col}: {e}")
			conn.commit()

	# Verify final column structure
	final_columns = [col.lower() for col in get_table_columns(engine, table_name)]
	logger.debug(f"Final table structure: {len(final_columns)} columns")

	# Return list of columns that couldn't be added
	missing_columns = set(col.lower() for col in columns) - set(final_columns)
	if missing_columns:
		logger.warning(f"Could not add columns: {missing_columns}")

	return list(missing_columns)

def haversine(lat1, lon1, lat2, lon2):
	"""
	Calculate the great-circle distance between two points on the Earth (specified in decimal degrees).
	Returns distance in meters.
	"""
	R = 6371000  # Earth radius in meters
	lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
	dlat = lat2 - lat1
	dlon = lon2 - lon1
	a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
	c = 2 * atan2(sqrt(a), sqrt(1-a))
	return R * c

def update_trip_and_file_for_fileid(conn, fileid):
	"""
	Update TorqFile and Torqtrips for a single fileid after inserting its data.
	"""
	# Aggregate trip info for this fileid
	sql = """
	SELECT
		fileid,
		MIN(GPS_Time) AS trip_start,
		MAX(GPS_Time) AS trip_end,
		MIN(GPS_Latitude) AS startlat,
		MIN(GPS_Longitude) AS startlon,
		MAX(GPS_Latitude) AS endlat,
		MAX(GPS_Longitude) AS endlon,
		COUNT(*) AS row_count
	FROM torqlogs
	WHERE fileid = :fileid
	GROUP BY fileid
	"""
	row = conn.execute(text(sql), {"fileid": fileid}).mappings().first()
	if not row:
		return

	trip_start = row["trip_start"]
	trip_end = row["trip_end"]
	startlat = row["startlat"]
	startlon = row["startlon"]
	endlat = row["endlat"]
	endlon = row["endlon"]
	row_count = row["row_count"]

	# Calculate trip duration
	trip_duration = None
	if trip_start and trip_end:
		try:
			trip_start_dt = convert_string_to_datetime(str(trip_start))
			trip_end_dt = convert_string_to_datetime(str(trip_end))
			if isinstance(trip_start_dt, datetime) and isinstance(trip_end_dt, datetime):
				trip_duration = (trip_end_dt - trip_start_dt).total_seconds()
			else:
				trip_duration = None

			# if not isinstance(trip_start, datetime):
			# 	# trip_start = convert_string_to_datetime(trip_start)
			# 	trip_start = convert_string_to_datetime(str(trip_start))
			# if not isinstance(trip_end, datetime):
			# 	# trip_end = convert_string_to_datetime(trip_end)
			# 	trip_end = convert_string_to_datetime(str(trip_end))
			# trip_duration = (trip_end - trip_start).total_seconds()
		except Exception as e:
			logger.error(f'{e} {type(e)} {trip_start=} {trip_end=}')
			trip_duration = None
	# Calculate trip distance (sum of all GPS point distances for this fileid)
	trip_distance = 0.0
	df_gps = pd.DataFrame()
	distances = []
	try:
		df_gps = pd.read_sql(
			"SELECT GPS_Latitude, GPS_Longitude FROM torqlogs WHERE fileid = ? ORDER BY GPS_Time ASC",
			conn,
			params=(fileid,)
		)
		if len(df_gps) > 1:
			distances = [
				haversine(
					df_gps.iloc[i-1]['GPS_Latitude'], df_gps.iloc[i-1]['GPS_Longitude'],
					df_gps.iloc[i]['GPS_Latitude'], df_gps.iloc[i]['GPS_Longitude']
				)
				for i in range(1, len(df_gps))
			]
			trip_distance = float(sum(distances))
		else:
			trip_distance = 0.0
	except Exception as e:
		logger.error(f"Error calculating trip_distance for fileid {fileid}: {e} {type(e)}")
		trip_distance = 0.0

	# Insert or update Torqtrips
	conn.execute(text("""
		INSERT OR IGNORE INTO torqtrips (fileid, tripdate, time, trip_distance)
		VALUES (:fileid, :trip_start, :trip_duration, :trip_distance)
	"""), {
		"fileid": fileid,
		"trip_start": trip_start,
		"trip_duration": trip_duration,
		"trip_distance": trip_distance
	})

	# Update TorqFile
	conn.execute(text("""
		UPDATE torqfiles
		SET trip_start = :trip_start,
			trip_end = :trip_end,
			trip_duration = :trip_duration,
			startlat = :startlat,
			startlon = :startlon,
			endlat = :endlat,
			endlon = :endlon,
			sent_rows = :row_count,
			trip_distance = :trip_distance
		WHERE fileid = :fileid
	"""), {
		"fileid": fileid,
		"trip_start": trip_start,
		"trip_end": trip_end,
		"trip_duration": trip_duration,
		"startlat": startlat,
		"startlon": startlon,
		"endlat": endlat,
		"endlon": endlon,
		"row_count": row_count,
		"trip_distance": trip_distance
	})
	logger.debug(f'Updated TorqFile and Torqtrips for fileid {fileid}: trip_start={trip_start}, trip_end={trip_end}, duration={trip_duration}, distance={trip_distance}, rows={row_count}')

def read_csvs_to_dataframe_and_insert(args, table_name='torqlogs'):
	"""
	Read all CSV files into a DataFrame, normalize column names, and insert into SQLite table.
	Handles varying columns, missing data, and extra spaces in column names.
	Returns the concatenated DataFrame and a dictionary of column stats.
	"""
	# Define column types based on sample data

	# Initialize dictionary to store column stats and file info
	pd_columns = {'stats': {}, 'files': {}}

	# Get list of CSV files
	csv_files = list(Path(args.logpath).glob("**/trackLog*.csv"))
	if not csv_files:
		logger.warning("No CSV files found")
		return None, pd_columns

	# First pass: Collect and validate headers from all files
	all_columns = set()
	valid_files = []
	csvhash = ''
	# engine = get_engine_session(args)
	engine = create_engine(
		f'sqlite:///{args.dbfile}',
		echo=False,
		connect_args={
			'timeout': 30,
			'isolation_level': None,  # Disable SQLite's autocommit mode
			'check_same_thread': False
		}
	)
	# Initialize database schema first
	try:
		database_init(engine)
	except Exception as e:
		logger.error(f"Error initializing database: {e}")
		return None, pd_columns

	for file_idx, csvfile in enumerate(csv_files):
		try:
			if csvfile.stat().st_size < MIN_FILESIZE:
				logger.warning(f"Skipping {csvfile} - file size too small {csvfile.stat().st_size} min {MIN_FILESIZE}")
				continue

			# Check if file has already been processed
			csvhash = md5(Path(csvfile).read_bytes()).hexdigest()
			with engine.connect() as conn:
				existing_file = conn.execute(text("SELECT fileid FROM torqfiles WHERE csvhash = :csvhash"),{"csvhash": csvhash}).first()

			if existing_file:
				logger.info(f"[{file_idx}/{len(csv_files)}] File {csvfile} already processed, skipping")
				continue

			# Read only the header row
			df = pd.read_csv(csvfile, nrows=0)

			# Normalize column names
			original_columns = df.columns.to_list()
			normalized_columns = [normalize_column_name(col) for col in original_columns]

			# Validate columns - check for empty or numeric column names
			if any(not col or col[0].isdigit() for col in normalized_columns):
				logger.warning(f"Skipping {csvfile} - invalid column names")
				continue
			all_columns.update(normalized_columns)
			valid_files.append((csvfile, normalized_columns))

			# Store file info
			pd_columns['files'][str(csvfile)] = {
				'filename': str(csvfile),
				'columns': normalized_columns
			}

		except Exception as e:
			logger.error(f"Error reading headers from {csvfile}: {e}")
			continue

	if not valid_files:
		logger.warning("No valid CSV files found after header validation")
		return None, pd_columns
	if args.file_limit:
		random.shuffle(valid_files)
		valid_files = [k for k in valid_files][0:10]
	logger.info(f"Found {len(valid_files)} valid CSV files with columns: {len(all_columns)}")
	column_types = COLUMN_TYPES.copy()
	for col in all_columns:
		col_lower = col.lower()
		if any(key in col_lower for key in ["time", "date"]):
			column_types[col] = DateTime

	# Update database schema if needed
	try:
		create_or_update_table(engine, table_name, all_columns, COLUMN_TYPES)
	except Exception as e:
		logger.error(f"Error updating table schema: {e} {type(e)}")
		return None, pd_columns

	# Second pass: Read and insert data from valid files
	df = pd.DataFrame()
	with engine.connect() as conn:
		conn.execute(text("PRAGMA journal_mode = WAL"))  # Use Write-Ahead Logging
		conn.execute(text("PRAGMA synchronous = NORMAL"))  # Reduce synchronization
		conn.execute(text("BEGIN TRANSACTION"))  # Start transaction

		try:
			for csv_idx, (csvfile, normalized_columns) in enumerate(valid_files):
				try:

					before_count = conn.execute(text("SELECT count(*) from torqlogs")).scalar()
					# Read CSV file
					df = pd.read_csv(csvfile, low_memory=False, on_bad_lines='skip', encoding='utf-8', encoding_errors='replace')

					for col in df.columns:
						col_lower = col.lower()
						if any(key in col_lower for key in ["time", "date"]):
							try:
								# df[col] = pd.to_datetime(df[col], errors='coerce')
								# df[col] = df[col].apply(lambda x: convert_string_to_datetime(x) if pd.notnull(x) else pd.NaT)
								df[col] = df[col].apply(lambda x: convert_string_to_datetime(x) if isinstance(x, str) and pd.notnull(x) else pd.NaT)  # type: ignore
							except Exception as e:
								logger.warning(f"Could not convert column {col} to datetime: {e} {type(e)} in {csvfile}")

					# Create TorqFile entry
					result = conn.execute(text("INSERT INTO torqfiles (csvfile, csvhash) VALUES (:csvfile, :csvhash) RETURNING fileid"),{"csvfile": str(csvfile), "csvhash": csvhash})
					fileid = result.scalar()

					# Add fileid column first
					df.insert(0, 'fileid', fileid)

					# Process columns and data
					df = df.rename(columns={col: normalize_column_name(col) for col in df.columns})
					df = df.replace(['-', '∞', 'inf', '-inf'], pd.NA)

					# Convert numeric columns
					for col in df.columns:
						if col in COLUMN_TYPES and COLUMN_TYPES[col] in [Float, Integer]:
							df[col] = pd.to_numeric(df[col], errors='coerce')

					# Remove rows that are identical to the header (possible repeated headers)
					header_row = list(df.columns)
					df = df[~df.apply(lambda row: list(row) == header_row, axis=1)]
					df = df[~df.apply(lambda row: row.astype(str).str.contains(' Device Time').any(), axis=1)]

					ordered_cols = ['fileid'] + [col for col in sorted(all_columns) if col != 'fileid' and col in df.columns]
					df = df[ordered_cols]

					# Insert data
					logger.info(f"[{csv_idx}/{len(valid_files)}] Sending {len(df)} rows from {csvfile} with fileid {fileid}")

					# Use smaller chunks for better memory management
					chunk_size = min(1000, args.sqlchunksize)
					for chunk_start in range(0, len(df), chunk_size):
						chunk = df.iloc[chunk_start:chunk_start + chunk_size]
						chunk.to_sql(table_name, conn, if_exists='append', index=False)

					# Update trip and file info for this fileid
					update_trip_and_file_for_fileid(conn, fileid)

					# Update TorqFile row count
					conn.execute(text("UPDATE torqfiles SET sent_rows = :rows WHERE fileid = :fileid"),{"rows": len(df), "fileid": fileid})
					after_count = conn.execute(text("SELECT count(*) from torqlogs")).scalar()
					logger.info(f"[{csv_idx}/{len(valid_files)}] Successfully inserted {len(df)} rows from {csvfile} before_count={before_count} after_count={after_count}")

				except Exception as e:
					logger.error(f"Error processing {csvfile}: {e}")
					if args.debug:
						logger.debug(f"DataFrame columns: {df.columns.tolist()}")
					continue

			conn.execute(text("COMMIT"))  # Commit all changes

		except Exception as e:
			logger.error(f"Transaction failed: {e}")
			conn.execute(text("ROLLBACK"))  # Rollback on error
			raise

	engine.dispose()
	return None, pd_columns

def check_split(logfile: Path, debug=False):
	"""
	check if file is damanaged, if so split it and save new log files
	if the file contains multiple column headers, split into multiple files for each column header line
	todo, check if time difference is small between headers, then ignore and assume its part of the same trip
	"""
	with open(logfile, "r") as f:
		data = f.readlines()
		splits = sum([k[0:4].lower().count("gps") for k in data])
	return splits

def get_csv_files(searchpath: str, args):
	# scan searchpath for csv files
	torqcsvfiles = [({"csvfile": k, "csvhash": md5(open(k, "rb").read()).hexdigest(), "size": os.stat(k).st_size, "dbmode": args.dbmode, }) for k in Path(searchpath).glob("**/*.csv") if k.stat().st_size >= MIN_FILESIZE]  # and not os.path.exists(f'{k}.fixed.csv')]
	return torqcsvfiles

def get_engine_session(args: argparse.Namespace) -> Session:
	dburl = None
	engine = None
	if args.dbmode == "mysql":
		dburl = f"mysql+pymysql://{args.dbuser}:{args.dbpass}@{args.dbhost}/{args.dbname}?charset=utf8mb4"
		engine = create_engine(dburl)
		# Session = sessionmaker(bind=engine)
		# session = Session()
	elif args.dbmode == "mariadb":
		dburl = f"mysql+pymysql://{args.dbuser}:{args.dbpass}@{args.dbhost}/{args.dbname}?charset=utf8mb4"
		engine = create_engine(dburl)
		# Session = sessionmaker(bind=engine)
		# session = Session()
	elif args.dbmode == "psql":
		dburl = f"postgresql://{args.dbuser}:{args.dbpass}@{args.dbhost}/{args.dbname}"
		engine = create_engine(dburl)
		# Session = sessionmaker(bind=engine)
		# session = Session()
	elif args.dbmode == "sqlite":
		dburl = f"sqlite:///{args.dbfile}"
		engine = create_engine(dburl, echo=False, connect_args={"check_same_thread": False})
		# Session = sessionmaker(bind=engine)
		# session = Session()
	if not engine:
		logger.error("no engine")
		sys.exit(-1)
	# s = sessionmaker(bind=engine)
	# session = s()
	try:
		database_init(engine)
	except AssertionError as e:
		logger.error(f"[maindbinit] {e}")
		sys.exit(-1)
	SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
	return SessionLocal()

def sqlsender_ppe(buffer, session, args):
	# engine = create_engine(url=dburl, echo=False)
	# Session = sessionmaker(bind=engine)
	# session = Session()
	results = {
		"fileid": buffer["fileid"], "csvfile": buffer["csvfile"], "status": "unknown", }
	try:
		if not isinstance(buffer["torqbuffer"], pd.DataFrame):
			tmpbuf = buffer["torqbuffer"].to_pandas()
		else:
			tmpbuf = buffer["torqbuffer"]
	except ValueError as e:
		logger.error(f"[tosql] tmpbuf {type(e)} {e}")
		raise ValueError(f"[tosql] tmpbuf {type(e)} {e}")
	# logger.info(f'[tosql] tmpbuf.is_empty() {buffer["torqbuffer"].is_empty()} ')
	# torqfile = (session.query(TorqFile).filter(TorqFile.fileid == results["fileid"]).first())
	try:
		# tmpbuf.to_sql("torqlogs", con=session.get_bind(), if_exists="append", index=False)
		tmpbuf.to_sql("torqlogs", con=session.get_bind(), if_exists="append", index=False, method='multi', chunksize=10000)
		results["status"] = "success"
		# torqfile = (session.query(TorqFile).filter(TorqFile.fileid == results["fileid"]).first())
	except (OperationalError, ProgrammingError, ArgumentError) as e:
		# todo handle db locks
		# todo handle unknown / new columns from csv files
		newcol = "unknown"
		if e.code == "e3q8" and "Unknown column" in e.args[0]:
			try:
				newcol = e.args[0].split()[4].replace("'", "")
			except IndexError as iexpt:
				logger.error(f"[tosql] {iexpt} while handling {e}")
			logger.warning(f'[tosql] {newcol=} code={e} args={e.args} r={results} csvfile={buffer["csvfile"]}')  # error:{e}
		elif e.code == "e3q8" and "database is locked" in e.args[0]:
			logger.warning(f'[tosql] {newcol=} code={e} args={e.args} r={results} csvfile={buffer["csvfile"]}')  # error:{e}
		else:
			logger.error(f'[tosql] code={e} r={results} csvfile={buffer["csvfile"]}')  # error:{e}
			results["status"] = "error"
	except InternalError as e:
		logger.error(f'[tosql] InternalError {e} r={results} csvfile={buffer["csvfile"]}')
		results["status"] = "error"
	except IntegrityError as e:
		logger.warning(f'[tosql] {type(e)} code={e} args={e.args[0]} r={results} csvfile={buffer["csvfile"]}')
		results["status"] = "error"
		# logger.warning(f'[tosql] {e.statement} {e.params}')
		# logger.warning(f'[tosql] {e}')
	except (pymysql.err.DataError, DataError) as e:
		# logger.error(f'[!]{type(e)}\n{e}\n')
		csvfile = buffer[
			"csvfile"
		]  # session.query(TorqFile).filter(TorqFile.fileid == results['fileid']).first()
		errmsg = e.args[0]
		err_row = errmsg.split("row")[-1].strip()
		err_row = errmsg.split(",")[1].split("at row")[1].strip().strip('")')
		if "Incorrect double value" in errmsg:
			err_col = errmsg.split()[8].split(".")[2].strip("`")
		else:
			err_col = errmsg.split(",")[1].split("at row")[0].split("'")[1]
		# logger.warning(f'\n[tosql] code={e}\nargs={e.args[0]}\nr={results}\nerr_row: {err_row}\nerr_col:{err_col}\ntorqfile={tf_err} csvfile={buffer["csvfile"]}\n')  # error:{e}
		logger.warning(f'\n[tosql] {type(e)} code={e} err_row: {err_row} err_col:{err_col} torqfile={csvfile} fileid:{buffer["fileid"]}')  # error:{e}
		# tmpbuf = tmpbuf.drop(columns=err_col)
		err_row = int(err_row)
		try:
			tmpbuf = tmpbuf.drop(index=err_row)
		except Exception as exc:
			logger.error(f'[torql] {type(exc)} {exc} err_row: {err_row} err_col:{err_col} torqfile={csvfile} fileid:{buffer["fileid"]}')
		try:
			# tmpbuf.to_sql('torqlogs', con=engine, if_exists='append', index=False)
			results["status"] = "warning"
		except (IndexError, KeyError, DataError) as ex:
			errmsg = ex.args[0]
			logger.error(f"[!] {type(ex)}\nerrmsg: {errmsg}\n")
	except (TypeError, ValueError) as e:
		logger.error(f"[!]{type(e)}\n{e}\n")
	return results

def send_torqtripdata(stats_data: dict, session: sessionmaker, args: argparse.Namespace):
	"""
	generate some stats from torqlogs and send to database
	param stats_data dict of stats, session sqlalchemy session, args
	"""
	# todo
	# send the data generated by generate_torqdata to database
	logger.info(f'{stats_data=}')


def get_time_stats(time_cols):
	stats = {}
	for c in time_cols:
		stats[c.name] = {
			"name": c.name, f"{c.name}.min": c.min(), f"{c.name}.mean": c.mean(), f"{c.name}.max": c.max(), f"{c.name}.tdelta": c.max() - c.min(), }
	return stats


def get_speed_stats(speed_cols):
	stats = {}
	for c in speed_cols:
		stats[c.name] = {
			"name": c.name, f"{c.name}.mean": c.mean(), f"{c.name}.max": c.max(), }
	return stats


def get_gps_stats(gpscols):
	stats = {}
	for c in gpscols:
		stats[c.name] = {
			"name": c.name, f"{c.name}.min": c.min(), f"{c.name}.mean": c.mean(), f"{c.name}.max": c.max(), }
	return stats


def get_cost_stats(cost_cols):
	stats = {}
	for c in cost_cols:
		stats[c.name] = {
			"name": c.name, f"{c.name}.min": c.min(), f"{c.name}.mean": c.mean(), f"{c.name}.max": c.max(), }
	return stats


def get_temp_stats(temp_cols):
	stats = {}
	for c in temp_cols:
		stats[c.name] = {
			"name": c.name, f"{c.name}.min": c.min(), f"{c.name}.mean": c.mean(), f"{c.name}.max": c.max(), }
	return stats

def get_tripfile_stats(fileid, session, args, limit=1000):
	"""
	collect some info about database columns
	"""
	skip_cols = ['id', 'fileid', 'devicetime', 'gpstime','time', 'csvfile', 'csvhash', 'read_flag', 'error_flag', 'send_flag', 'send_flag', 'data_flag', 'distance']
	df = pd.DataFrame(session.execute(text('select column_name from information_schema.columns where table_name = "torqlogs" order by table_name,ordinal_position')).all())
	column_names = sorted([k for k in set([k[0] for k in df.values]) if k not in skip_cols])
	logger.info(f'checking {fileid=} found {len(column_names)} columns in database, limit:{limit}')
	maxnlen = max([len(k) for k in column_names])
	for col in column_names:
		if args.debug:
			logger.debug(f'checking {col} limit:{limit} ')
		if not limit:
			df = pd.DataFrame(session.execute(text(f'select {col} from torqlogs where fileid={fileid}')).all())
		else:
			df = pd.DataFrame(session.execute(text(f'select {col} from torqlogs where fileid={fileid} limit {limit}')).all())
		try:
			nulls = df.isnull().sum().values[0]
		except (IndexError,AttributeError) as e:
			logger.error(f'{type(e)} {e} {col=} ')
			nulls = 0.0
		# nullratio = len(df)/df.isnull().sum().values[0]
		nr = 0.0
		if nulls > 0:
			try:
				nr = len(df)/nulls
			except (Exception, RuntimeError, ZeroDivisionError) as e:
				logger.error(f'{type(e)} {e} {col=} {df.describe()}')

		minval = df.min().values[0] or 0.0
		mednval = df.median().values[0] or 0.0
		meannval = df.mean().values[0] or 0.0
		maxnval = df.max().values[0] or 0.0
		logger.info(f'  {col:<{maxnlen}} nulls: {nulls:>3} nr: {nr:>3.3} {minval:>3.3} {mednval:>3.3} {meannval:>3.3} {maxnval:>3.3}')

def generate_torqdata(df: pd.DataFrame, session: sessionmaker, args: argparse.Namespace):
	# generate torqdata from torqlogs
	# df = pd.DataFrame([k.__dict__ for k in data])
	time_cols = [df[k] for k in df.columns if "gpstime" in k or "devicetime" in k]
	stats = {}
	stats["timestats"] = get_time_stats(time_cols)

	speed_cols = [df[k] for k in df.columns if "speed" in k]
	stats["speedstats"] = get_speed_stats(speed_cols)

	gps_cols = [df[k] for k in df.columns if "gps" in k]
	stats["gpsstats"] = get_gps_stats(gps_cols)

	cost_cols = [df[k] for k in df.columns if "cost" in k]
	stats["coststats"] = get_cost_stats(cost_cols)

	temp_cols = [df[k] for k in df.columns if "temp" in k]
	stats["tempstats"] = get_temp_stats(temp_cols)

	return stats

def convert_string_to_datetime(s: str) -> datetime:
	"""
	try to convert string to datetime, based on string length apply fmt
	param s string with datetime
	returns datetime object
	"""
	if not isinstance(s, str):
		logger.warning(f'{s} is not str but {type(s)}')
		s = str(s)
	fmt_selector = len(s)
	datetimeobject = s
	try:
		match fmt_selector:
			case 20:
				datetimeobject = datetime.strptime(s, fmt_20).astimezone(pytz.timezone("UTC"))
			case 24:
				datetimeobject = datetime.strptime(s, fmt_24).astimezone(pytz.timezone("UTC"))
			case 26:
				datetimeobject = datetime.strptime(s, fmt_26).astimezone(pytz.timezone("UTC"))
			case 28:
				datetimeobject = datetime.strptime(s, fmt_28).astimezone(pytz.timezone("UTC"))
			case 30:
				datetimeobject = datetime.strptime(s, fmt_30).astimezone(pytz.timezone("UTC"))
			case 34:
				datetimeobject = datetime.strptime(s, fmt_34).astimezone(pytz.timezone("UTC"))
			case 36:
				datetimeobject = datetime.strptime(s, fmt_36).astimezone(pytz.timezone("UTC"))
			case _:
				pass
	except (ValueError, TypeError, KeyError) as e:
		logger.error(f"dateconverter {type(e)} {e} {s=}")
	finally:
		pass
	return datetimeobject  # type: ignore

def populate_trips_and_update_files(session):
	"""
	Populate Torqtrips based on torqlogs, grouped by fileid.
	Also updates TorqFile fields based on aggregated torqlogs data.
	Uses raw SQL for aggregation and column discovery.
	"""
	sql = """
	SELECT
		fileid,
		MIN(GPS_Time) AS trip_start,
		MAX(GPS_Time) AS trip_end,
		MIN(GPS_Latitude) AS startlat,
		MIN(GPS_Longitude) AS startlon,
		MAX(GPS_Latitude) AS endlat,
		MAX(GPS_Longitude) AS endlon,
		COUNT(*) AS row_count
	FROM torqlogs
	GROUP BY fileid
	"""
	for row in session.execute(text(sql)):
		fileid = row.fileid
		trip_start = row.trip_start
		trip_end = row.trip_end
		startlat = row.startlat
		startlon = row.startlon
		endlat = row.endlat
		endlon = row.endlon
		row_count = row.row_count

		# Calculate trip_duration
		trip_duration = None
		if trip_start and trip_end:
			try:
				trip_start_dt = convert_string_to_datetime(str(trip_start))
				trip_end_dt = convert_string_to_datetime(str(trip_end))
				if isinstance(trip_start_dt, datetime) and isinstance(trip_end_dt, datetime):
					trip_duration = (trip_end_dt - trip_start_dt).total_seconds()
			except Exception as e:
				logger.error(f'{e} {type(e)} {trip_start=} {trip_end=}')
				trip_duration = None

		# Calculate trip_distance
		trip_distance = 0.0
		try:
			df_gps = pd.read_sql(
				"SELECT GPS_Latitude, GPS_Longitude FROM torqlogs WHERE fileid = ? ORDER BY GPS_Time ASC",
				session.bind,
				params=(fileid,)
			)
			if len(df_gps) > 1:
				distances = [
					haversine(
						df_gps.iloc[i-1]['GPS_Latitude'], df_gps.iloc[i-1]['GPS_Longitude'],
						df_gps.iloc[i]['GPS_Latitude'], df_gps.iloc[i]['GPS_Longitude']
					)
					for i in range(1, len(df_gps))
				]
				trip_distance = float(sum(distances))
			else:
				trip_distance = 0.0
		except Exception as e:
			logger.error(f"Error calculating trip_distance for fileid {fileid}: {e}")
			trip_distance = 0.0

		# Insert into Torqtrips (if not exists)
		session.execute(text("""
			INSERT OR IGNORE INTO torqtrips (
				fileid, tripdate, trip_end, startlat, startlon, endlat, endlon, row_count, trip_duration, trip_distance
			)
			VALUES (
				:fileid, :trip_start, :trip_end, :startlat, :startlon, :endlat, :endlon, :row_count, :trip_duration, :trip_distance
			)
		"""), {
			"fileid": fileid,
			"trip_start": trip_start,
			"trip_end": trip_end,
			"startlat": startlat,
			"startlon": startlon,
			"endlat": endlat,
			"endlon": endlon,
			"row_count": row_count,
			"trip_duration": trip_duration,
			"trip_distance": trip_distance
		})

		# Update TorqFile
		session.execute(text("""
			UPDATE torqfiles
			SET trip_start = :trip_start,
				trip_end = :trip_end,
				trip_duration = :trip_duration,
				startlat = :startlat,
				startlon = :startlon,
				endlat = :endlat,
				endlon = :endlon,
				sent_rows = :row_count,
				trip_distance = :trip_distance
			WHERE fileid = :fileid
		"""), {
			"fileid": fileid,
			"trip_start": trip_start,
			"trip_end": trip_end,
			"trip_duration": trip_duration,
			"startlat": startlat,
			"startlon": startlon,
			"endlat": endlat,
			"endlon": endlon,
			"row_count": row_count,
			"trip_distance": trip_distance
		})

	session.commit()
