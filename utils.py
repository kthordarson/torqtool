# utils and db things here

import os
import re
import shutil
import sys
from datetime import datetime
from hashlib import md5
from pathlib import Path
from pickle import PicklingError
import random
import argparse
import pandas as pd
import polars as pl
import pymysql
import pytz
from loguru import logger
from polars import ComputeError
from polars import read_csv as read_csv_polars
from polars.exceptions import ColumnNotFoundError, InvalidOperationError
from sqlalchemy import create_engine, text, MetaData, Table, Column, Float, String, Integer
from sqlalchemy.exc import ArgumentError, DataError,IntegrityError, InternalError, OperationalError, ProgrammingError
from sqlalchemy.orm import sessionmaker, Session
import sqlite3
from commonformats import fmt_20, fmt_24, fmt_26, fmt_28, fmt_30, fmt_34, fmt_36
from datamodels import database_init, TorqFile, COLUMN_TYPES

MIN_FILESIZE = 3000

def get_parser(appname):
	parser = argparse.ArgumentParser(description=appname)
	parser.add_argument("--find-optimal-batch-size", default=False, help="Run tests to find optimal batch size", action="store_true", dest="find_optimal_batch_size")
	parser.add_argument("--fixer", default=False, help="run fixer, set --bakpath", action="store_true", dest="fixer")
	parser.add_argument("--fixcsv", default=False, help="repair csv", action="store_true", dest="fixcsv")
	parser.add_argument("--getcols", default=False, help="prep cols", action="store_true", dest="getcols")
	parser.add_argument("--repairsplit", default=False, help="enable splitting of strange log files", action="store_true", dest="repairsplit", )
	parser.add_argument("--samplemode", default=False, help="use samplemode, select small random number of logs-for debugging", action="store_true", dest="samplemode", )
	parser.add_argument("--scanpath", default=False, help="run scanpath", action="store_true", dest="scanpath", )
	parser.add_argument("--old_scanpath", default=False, help="run old_scanpath", action="store_true", dest="old_scanpath", )
	parser.add_argument("--showdrops", default=False, help="show dropped columns", action="store_true", dest="showdrops", )
	parser.add_argument("--skipwrites", default=False, help="skipwrites", action="store_true", dest="skipwrites", )
	parser.add_argument("--filestats", default=True, help="create filestats", action="store_true", dest="filestats", )
	parser.add_argument("--testnewreader", default=False, help="run testnewreader", action="store_true", dest="testnewreader", )
	parser.add_argument("--threadmode", default="ppe", help="threadmode ppe/oldppe/tpe", action="store")
	parser.add_argument("--torqdata", default=False, help="create torqdata", action="store_true", dest="torqdata", )
	parser.add_argument("--transfer", default=False, help="transfer old logs, set oldlogpath to location of old triplogs", action="store_true", dest="transfer", )
	parser.add_argument("--bakpath", nargs="?", default="/home/kth/development/torq/backups3", help="where to put backups", action="store", )
	parser.add_argument("--check-file", default=False, help="check database", action="store_true", dest="check_file", )
	parser.add_argument("--chunks", nargs="?", default="4", help="chunks", action="store")
	parser.add_argument("--batch_size", nargs="?", default=5, type=int, help="batch_size", action="store")
	parser.add_argument("--combinecsv", default=False, help="make big csv", action="store_true", dest="combinecsv", )
	parser.add_argument("--create-trips", default=False, help="create trip database", action="store_true", dest="create_trips", )
	parser.add_argument("--check-db", default=False, help="check database", action="store_true", dest="check_db", )
	parser.add_argument("--database_dropall", default=False, help="drop database", action="store_true", dest="database_dropall", )
	parser.add_argument("--dbhost", default="localhost", help="dbname", action="store")
	parser.add_argument("--dbmode", default="sqlite", help="sqlmode mysql/psql/sqlite/mariadb", action="store", dest="dbmode", )
	parser.add_argument("--dbname", default="torq", help="dbname", action="store")
	parser.add_argument("--dbpass", default="qrot", help="dbname", action="store")
	parser.add_argument("--dbuser", default="torq", help="dbname", action="store")
	parser.add_argument("--dbfile", default="torqfiskur.db", help="database file", action="store")
	parser.add_argument("--db_limit", default=False, help="db_limit", action="store", dest="db_limit")
	parser.add_argument("--db_rowlimit", default=False, help="db_rowlimit", action="store", dest="db_rowlimit")
	parser.add_argument("--db_minrows", default=100, help="db_minrows", action="store", dest="db_minrows")
	parser.add_argument("--dump-db", nargs="?", default=None, help="dump database to file", action="store", )
	parser.add_argument("--file", nargs="?", default=".", help="path to single csv file", action="store")
	parser.add_argument("--logpath", nargs="?", default=".", help="path to csv files", action="store")
	parser.add_argument("--max_workers", nargs="?", default="4", help="max_workers", action="store")
	parser.add_argument("--oldlogpath", nargs="?", default=".", help="oldlogpath", action="store")
	parser.add_argument("--sqlchunksize", nargs="?", default=1000, type=int, help="sql chunk", action="store")
	parser.add_argument("--webstart", default=False, help="start web listener", action="store_true", dest="web", )
	parser.add_argument("-i", "--info", "--dbinfo", default=False, help="show dbinfo", action="store_true", dest="dbinfo", )
	parser.add_argument("-d", "--debug", default=False, help="debugmode", action="store_true", dest="debug", )
	parser.add_argument("--extradebug", default=False, help="extradebug", action="store_true", dest="extradebug", )
	# parser.add_argument("--gui", default=False, help="Run gui", action="store_true", dest='gui')
	# parser.add_argument("--init-db", default=False, help="init database", action="store_true", dest='init_db')

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
	with engine.connect() as conn:
		try:
			result = conn.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
		except Exception as e:
			logger.error(f"Error fetching table columns: {e} {type(e)} table_name={table_name}")
			return []
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
	table_columns = [Column(col, column_types.get(col, String)) for col in columns]

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
	logger.info(f"Found {len(valid_files)} valid CSV files with columns: {len(all_columns)}")
	# Update database schema if needed
	try:
		create_or_update_table(engine, table_name, all_columns, COLUMN_TYPES)
	except Exception as e:
		logger.error(f"Error updating table schema: {e} {type(e)}")
		return None, pd_columns

	# Create session
	# Session = sessionmaker(bind=engine)
	# session = Session()

	# Second pass: Read and insert data from valid files
	with engine.connect() as conn:
		conn.execute(text("PRAGMA journal_mode = WAL"))  # Use Write-Ahead Logging
		conn.execute(text("PRAGMA synchronous = NORMAL"))  # Reduce synchronization
		conn.execute(text("BEGIN TRANSACTION"))  # Start transaction

		try:
			for csv_idx, (csvfile, normalized_columns) in enumerate(valid_files):
				try:
					# Check if file has already been processed
					csvhash = md5(Path(csvfile).read_bytes()).hexdigest()
					existing_file = conn.execute(
						text("SELECT fileid FROM torqfiles WHERE csvhash = :csvhash"),
						{"csvhash": csvhash}
					).first()

					if existing_file:
						logger.info(f"[{csv_idx}/{len(valid_files)}] File {csvfile} already processed, skipping")
						continue

					# Read CSV file
					df = pd.read_csv(
						csvfile,
						low_memory=False,
						on_bad_lines='skip',
						encoding='utf-8',
						encoding_errors='replace'
					)

					# Create TorqFile entry
					result = conn.execute(
						text("INSERT INTO torqfiles (csvfile, csvhash) VALUES (:csvfile, :csvhash) RETURNING fileid"),
						{"csvfile": str(csvfile), "csvhash": csvhash}
					)
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

					# Insert data
					logger.info(f"[{csv_idx}/{len(valid_files)}] Sending {len(df)} rows from {csvfile} with fileid {fileid}")

					# Use smaller chunks for better memory management
					chunk_size = min(1000, args.sqlchunksize)
					for chunk_start in range(0, len(df), chunk_size):
						chunk = df.iloc[chunk_start:chunk_start + chunk_size]
						chunk.to_sql(
							table_name,
							conn,
							if_exists='append',
							index=False
						)

					# Update TorqFile row count
					conn.execute(
						text("UPDATE torqfiles SET sent_rows = :rows WHERE fileid = :fileid"),
						{"rows": len(df), "fileid": fileid}
					)

					logger.info(f"[{csv_idx}/{len(valid_files)}] Successfully inserted {len(df)} rows from {csvfile}")

				except Exception as e:
					logger.error(f"Error processing {csvfile}: {e}")
					if args.debug:
						logger.debug(f"DataFrame columns: {df.columns.tolist()}")
					continue

			conn.execute("COMMIT")  # Commit all changes

		except Exception as e:
			conn.execute("ROLLBACK")  # Rollback on error
			logger.error(f"Transaction failed: {e}")
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

def get_bad_vals(csvfile: str):
	with open(csvfile, "r") as reader:
		data = reader.readlines()
	for line in data:
		l0 = line.split(",")
		for lx in l0:
			try:
				lx.encode("ascii")
			except (UnicodeEncodeError, UnicodeDecodeError) as e:
				logger.error(f"unicodeerr: {e} in {csvfile} lt={type(line)} l={line}")
			except AttributeError as e:
				logger.error(f"AttributeError: {e} in {csvfile} lt={type(line)} l={line}")


def get_engine_session(args):
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
	return engine  # , session

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


def read_buff(csvfile, tf_fileid, debug=False):
	error_files = []
	rb = {
		"torqbuffer": pd.DataFrame(), "fileid": tf_fileid, "csvfile": csvfile, }
	column_mapping = {
			"GPS Time": "gpstime",
			" Device Time": "devicetime",
			" Longitude": "longitude",
			" Latitude": "latitude"
		}
	try:
		torqbuffer = read_csv_polars(csvfile, ignore_errors=True, try_parse_dates=True, truncate_ragged_lines=True, )  # , use_pyarrow=True ,  ) #, null_values=['NaN','-','0\x88\x9e'])
		torqbuffer = torqbuffer.rename(column_mapping)
		torqbuffer = torqbuffer.fill_null(0).fill_nan(0)

	except (InvalidOperationError, ValueError) as e:
		logger.error(f"[rb] {type(e)} {e} csvfile={csvfile}")
		return rb, error_files
	except ComputeError as e:
		logger.error(f"[rb] {type(e)} {e} csvfile={csvfile}")
		return rb, error_files
	if torqbuffer.is_empty():
		logger.error(f"[rb] torqbuffer is empty {csvfile}")
		return rb, error_files
	fileid_series = pl.Series("fileid", [tf_fileid for k in range(len(torqbuffer))])
	torqbuffer.insert_at_idx(1, fileid_series)
	rbx = None
	errf = None
	try:
		rbx, errf = fix_timestamps(torqbuffer, csvfile, tf_fileid)
	except Exception as e:
		logger.error(f"[rb] {type(e)} {e} in fix_timestamps {csvfile}\nrbx: {rbx}\n")
	if rbx:
		rb["torqbuffer"] = rbx["torqbuffer"]
	if errf:
		error_files.extend(errf)
	return rb, error_files


def fix_timestamps(torqbuffer, csvfile, tf_fileid):
	# todo fix gpstime and devicetime
	# drop rows where either values are null or missing
	error_files = []
	resultbuffer = {
		"torqbuffer": torqbuffer, "fileid": tf_fileid, "csvfile": csvfile, }
	try:
		idx = len(torqbuffer["devicetime"]) // 2  # get middle index to guess dateformat
	except (ColumnNotFoundError, ComputeError, ValueError) as e:
		logger.error(f"[rb] devicetime {type(e)} {e} csvfile: {csvfile}")
		idx = 10
	try:
		idx = len(torqbuffer["gpstime"]) // 2  # get middle index to guess dateformat
	except (ColumnNotFoundError, ComputeError, ValueError) as e:
		logger.error(f"[rb] gpstime {type(e)} {e} csvfile: {csvfile}")
		idx = 10
	gpstime = torqbuffer["gpstime"]
	devicetime = torqbuffer["devicetime"]
	try:
		if len(torqbuffer["devicetime"][idx]) == 28:
			devicetime = pl.Series("devicetime", [datetime.strptime(k, fmt_28).astimezone(pytz.timezone("UTC")) for k in torqbuffer["devicetime"] if k], )
		elif len(torqbuffer["devicetime"][idx]) == 24:
			devicetime = pl.Series("devicetime", [datetime.strptime(k, fmt_24).astimezone(pytz.timezone("UTC")) for k in torqbuffer["devicetime"] if k], )
		elif len(torqbuffer["devicetime"][idx]) == 26:
			devicetime = pl.Series("devicetime", [datetime.strptime(k, fmt_26).astimezone(pytz.timezone("UTC")) for k in torqbuffer["devicetime"] if k], )
		elif len(torqbuffer["devicetime"][idx]) == 20:
			devicetime = pl.Series("devicetime", [datetime.strptime(k, fmt_20).astimezone(pytz.timezone("UTC")) for k in torqbuffer["devicetime"] if k], )
		else:
			logger.error(f'[rb] devicetime format error! len = {len(torqbuffer["devicetime"][idx])} {idx=} buffer: {torqbuffer["devicetime"]}')
	except (ColumnNotFoundError, ComputeError, ValueError, TypeError) as e:
		logger.error(f'[rb] devicetime {type(e)} {e} csvfile: {csvfile} len = {len(torqbuffer["devicetime"][idx])} {idx=} ')
		error_files.append(csvfile)
	try:
		if len(torqbuffer["gpstime"][idx]) == 28:
			gpstime = pl.Series("gpstime", [datetime.strptime(k, fmt_28).astimezone(pytz.timezone("UTC")) for k in torqbuffer["gpstime"] if k], )
		elif len(torqbuffer["gpstime"][idx]) == 26:
			# gpstime = pl.Series('gpstime', [datetime.strptime(k,fmt_26).astimezone(pytz.timezone('UTC')) for k in torqbuffer['gpstime'] if k])
			gpstime = pl.Series("gpstime", [datetime.strptime(k, fmt_26).astimezone(pytz.timezone("UTC")) for k in torqbuffer["gpstime"] if k], )
		elif len(torqbuffer["gpstime"][idx]) == 34:
			# to fix TimeZoneAwareConstructorWarning
			gpstime = pl.Series("gpstime", [datetime.strptime(k, fmt_34).astimezone(pytz.timezone("UTC")) for k in torqbuffer["gpstime"] if k], )
		else:
			logger.error(f'[rb] gpstime format error ex: {torqbuffer["gpstime"]} len: {len(torqbuffer["gpstime"])}')
	except (ComputeError, ValueError, TypeError) as e:
		logger.error(f'[rb] {type(e)} {e} csvfile: {csvfile} len = {len(torqbuffer["devicetime"][idx])} {idx=} buf: {torqbuffer["gpstime"]}')
		error_files.append(csvfile)
		# raise e

	gpstime_err = [idx for idx, k in enumerate(torqbuffer["gpstime"]) if not k]
	devicetime_err = [idx for idx, k in enumerate(torqbuffer["devicetime"]) if not k]
	try:
		torqbuffer = torqbuffer.drop("devicetime")
		if len(torqbuffer) != len(devicetime):
			torqbuffer = torqbuffer[0:len(devicetime)]
		torqbuffer.insert_at_idx(4, devicetime)
	except (AttributeError, UnboundLocalError, pl.exceptions.ShapeError) as e:
		logger.error(f"[rb] {type(e)} {e} csvfile: {csvfile} tblen={len(torqbuffer)} glen={len(gpstime)} dlen={len(devicetime)} {gpstime_err=} {devicetime_err=}")
		error_files.append(csvfile)
	try:
		torqbuffer = torqbuffer.drop("gpstime")
		if len(torqbuffer) != len(gpstime):
			torqbuffer = torqbuffer[0:len(gpstime)]
		torqbuffer.insert_at_idx(3, gpstime)
	except (AttributeError, UnboundLocalError, pl.exceptions.ShapeError) as e:
		logger.error(f"[rb] {type(e)} {e} csvfile: {csvfile} tblen={len(torqbuffer)} glen={len(gpstime)} dlen={len(devicetime)} {gpstime_err=} {devicetime_err=}")
		error_files.append(csvfile)
	resultbuffer["torqbuffer"] = torqbuffer
	# resultbuffer = {
	# 	'torqbuffer' : torqbuffer, # 	'fileid' : tf_fileid, # 	'csvfile' : csvfile, # }
	return resultbuffer, error_files


async def torq_worker_ppe(tf, session, args):
	buffer = None
	results = None
	t0 = datetime.now()
	timetotal = 0
	try:
		buffer, error_files = read_buff(tf.csvfile, tf.fileid, args)
		if not buffer:
			logger.warning(f"[!] buffer is None tf={tf}")
		if args.debug:
			if len(error_files) > 0:
				logger.warning(f"error_files: {len(error_files)} ")  # pass # logger.debug(f'file {tf.csvfile} buffer: {len(buffer["torqbuffer"])}')
				_ = [logger.error(f"error in file: {k}") for k in error_files]
	except (TypeError,) as e:
		logger.error(f"[!] {type(e)} {e} in read_buff {tf.csvfile}")
		raise e
	except (InvalidOperationError, ValueError, PicklingError, ComputeError) as e:
		logger.error(f"[!] {type(e)} {e} in read_buff {tf.csvfile}")
		return None
	try:
		results = sqlsender_ppe(buffer, session, args)  # send triplog data
		timetotal += (datetime.now() - t0).seconds
		if args.debug:
			logger.debug(f't: {(datetime.now()-t0).seconds}/{timetotal} fileid {results.get("fileid")} {results.get("status")} buffer: {len(buffer["torqbuffer"])}')
	except (ValueError, TypeError, PicklingError) as e:
		logger.error(f'[!] {type(e)} {e} in sqlsender buffer.is_empty() {buffer["torqbuffer"].is_empty()}')
		return None


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

def check_database_columns(session, args=None, limit=1000):
	"""
	collect some info about database columns
	"""
	skip_cols = ['id', 'fileid', 'devicetime', 'gpstime','time', 'csvfile', 'csvhash', 'read_flag', 'error_flag', 'send_flag', 'send_flag', 'data_flag', 'distance']
	df = pd.DataFrame(session.execute(text('select column_name from information_schema.columns where table_name = "torqlogs" order by table_name,ordinal_position')).all())
	# df = pd.DataFrame(session.execute(text('select column_name from information_schema.columns where table_schema = "torq" order by table_name,ordinal_position')).all())
	# df2 = pd.DataFrame(session.execute(text('SELECT id,fileid,o2sensor1widerangecurrentma FROM torqlogs WHERE o2sensor1widerangecurrentma IS NULL  OR o2sensor1widerangecurrentma=";" ')).all())
	column_names = sorted([k for k in set([k[0] for k in df.values]) if k not in skip_cols])
	logger.info(f'found {len(column_names)} columns in database, limit:{limit}')
	maxnlen = max([len(k) for k in column_names])  # longest name, for formatting
	for col in column_names:
		if args.debug:
			logger.debug(f'checking {col} limit:{limit} ')
		if not limit:
			df = pd.DataFrame(session.execute(text(f'select {col} from torqlogs')).all())
		else:
			df = pd.DataFrame(session.execute(text(f'select {col} from torqlogs limit {limit}')).all())
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

def get_tripfile_stats(fileid, session, args=None, limit=1000):
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

def generate_torqdata(df: pd.DataFrame, session: sessionmaker = None, args: argparse.Namespace = None):
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

def convert_string_to_datetime(s: str):
	"""
	try to convert string to datetime, based on string length apply fmt
	param s string with datetime
	returns datetime object
	"""
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
		return datetimeobject

def read_profile(profile_fn: str):
	# read profile.properties file, to extract some data
	tripdate = None
	try:
		with open(profile_fn, "r") as f:
			data = f.readlines()
		if len(data) == 8 or len(data) == 6:
			# pdata_date = str(data[1][1:]).strip('\n')
			# tripdate = datetime.strptime(pdata_date ,'%a %b %d %H:%M:%S %Z%z %Y')
			if len(data[1]) == 30:
				tripdate = datetime.strptime((str(data[1][1:]).strip("\n")), fmt_30)
			elif len(data[1]) == 36:
				# Tue May 17 17:55:43 GMT+02:00 2022
				tripdate = datetime.strptime((str(data[1][1:]).strip("\n")), fmt_36)
			else:
				logger.warning(f"unknown date format {data[1]}")
				tripdate = data[1]
		else:
			logger.warning(f"profile.properties file {profile_fn} has {len(data)} lines {data}")
	except Exception as e:
		logger.error(f"unhandled {type(e)} {e}")
	finally:
		return tripdate


def transfer_older_logs(args):
	# transfer old tripLogs to new format
	# todo read more info from profile.properties file
	#

	old_dirs = [
		k
		for k in Path(args.oldlogpath).glob("*")
		if k.is_dir() and len(str(k.name)) == 13
	]
	# pick only directories with 13 digits

	transfered_logs = []
	# to keep track of the logs that have been transfered

	logger.debug(f"found {len(old_dirs)} old tripLogs")
	for od in old_dirs:
		profile_fn = os.path.join(od, "profile.properties")
		# old_timestamp = datetime.fromtimestamp(int(od.name)/1000).strftime("%Y-%b-%d_%H-%M-%S")
		if Path(profile_fn).exists():
			# read profile.properties file, to extract some data
			profiledata = read_profile(profile_fn)
		else:
			logger.warning(f"no profile.properties file found in {od}")
			profiledata = None
		# rename log file to new format
		if profiledata:
			trip_date = profiledata.strftime("%Y-%b-%d_%H-%M-%S")
			new_log_fn = Path(os.path.join(args.logpath, f"trackLog-{trip_date}.csv"))
			if len(new_log_fn.name) != 33:
				logger.warning(f"new log filename {new_log_fn} is not 33 chars long")
			if Path(new_log_fn).exists():
				logger.warning(f"file {new_log_fn} exists, skipping")
			else:
				old_log_name = os.path.join(od, "trackLog.csv")
				logger.debug(f"move/copy from {old_log_name} to {new_log_fn}")
				try:
					shutil.copyfile(old_log_name, new_log_fn)
					transfered_logs.append(new_log_fn)
				except Exception as e:
					logger.error(f"Error {type(e)} {e} {old_log_name} -> {new_log_fn}")
		else:
			logger.warning(f"could not extract profiledata from {profile_fn}")
	logger.info(f"transfered {len(transfered_logs)} of {len(old_dirs)} old tripLogs to {args.logpath}")
	return transfered_logs

if __name__ == "__main__":
	pass
