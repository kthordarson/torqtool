# utils and db things here
from math import radians, cos, sin, sqrt, atan2
import random
import os
import re
import sys
import time
from datetime import datetime
from hashlib import md5
from pathlib import Path
import argparse
import pandas as pd
import pytz
from loguru import logger
from sqlalchemy import DateTime
from sqlalchemy import create_engine, text, MetaData, Table, Column, Float, String, Integer
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError
from psycopg2.errors import UniqueViolation
from commonformats import fmt_20, fmt_24, fmt_26, fmt_28, fmt_30, fmt_34, fmt_36
from datamodels import database_init, COLUMN_TYPES
from schemas import canonicalize_column_name, canonicalize_columns
from schemas import TRIP_METRIC_COLUMNS

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
	parser.add_argument("--dbpass", default="qrot", help="dbpass", action="store")
	parser.add_argument("--dbuser", default="torq", help="dbuser", action="store")
	parser.add_argument("--dbfile", default="torqdata.db", help="database file", action="store")
	parser.add_argument("--db_limit", default=False, help="db_limit", action="store", dest="db_limit")
	parser.add_argument("--file_limit", default=False, help="file_limit", action="store_true", dest="file_limit")
	parser.add_argument("--file", nargs="?", default=".", help="path to single csv file", action="store")
	parser.add_argument("--logpath", nargs="?", default=".", help="path to csv files", action="store")
	parser.add_argument("--sqlchunksize", nargs="?", default=1000, type=int, help="sql chunk", action="store")
	parser.add_argument("-i", "--info", "--dbinfo", default=False, help="show dbinfo", action="store_true", dest="dbinfo", )
	parser.add_argument("-d", "--debug", default=False, help="debugmode", action="store_true", dest="debug", )
	parser.add_argument('--min_row_count', default=100, type=int, help="minimum row count for a file to be processed", action="store")
	if appname == "guitest2":
		parser.add_argument('--main-window', help="start main window", action="store_true", dest='main_window', default=True)
		parser.add_argument('--pos-manager', help="start position manager window", action="store_true", dest='pos_manager', default=False)
		parser.add_argument('--start-end-window', help="start start/end grouped window", action="store_true", dest='start_end_window', default=False)
		parser.add_argument('--tabbed-workspace', help="start tabbed workspace with main/positions/start-end", action="store_true", dest='tabbed_workspace', default=False)
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

def get_table_columns(session, table_name):
	"""
	Get the current columns of a table in a database-agnostic way.
	"""
	try:
		inspector = inspect(session.get_bind())
		return [str(col["name"]).lower() for col in inspector.get_columns(table_name)]
	except Exception as e:
		logger.error(f"An error occurred: {e} {type(e)} while fetching columns for table {table_name}")
		return []

def create_or_update_table(session, table_name, columns, column_types):
	"""
	Create or update the table to include all provided columns.
	Handles duplicate columns and maintains existing schema.
	"""
	metadata = MetaData()

	# Check existing table columns and normalize to lowercase
	try:
		existing_columns = [col.lower() for col in get_table_columns(session, table_name)]
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
		metadata.create_all(bind=session.get_bind())
	else:
		# Add only new columns to existing table
		with session.get_bind().connect() as conn:
			# Convert all column names to lowercase for comparison
			new_columns = set(col.lower() for col in columns) - set(existing_columns)
			orig_col = ''
			if new_columns:
				logger.info(f"Adding {len(new_columns)} new columns to {table_name}")
				for col in new_columns:
					try:
						# Get original case version of column name
						orig_col = next(c for c in columns if c.lower() == col)
						sqlalchemy_type = column_types.get(orig_col, String)
						if isinstance(sqlalchemy_type, type):
							sql_type = sqlalchemy_type().compile(dialect=conn.dialect)
						else:
							sql_type = sqlalchemy_type.compile(dialect=conn.dialect)
						alter_sql = text(f'ALTER TABLE {table_name} ADD COLUMN "{orig_col}" {sql_type}')
						conn.execute(alter_sql)
						logger.debug(f"Added column: {orig_col} ({sql_type})")
					except Exception as e:
						if "duplicate column" in str(e).lower() or "already exists" in str(e).lower():
							logger.debug(f"Column {orig_col} already exists, skipping")
							continue
						else:
							logger.warning(f"Could not add column {orig_col}: {e}")
			conn.commit()

	# Verify final column structure
	final_columns = [col.lower() for col in get_table_columns(session, table_name)]
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


def _normalize_col_name(value: str) -> str:
	return "".join(ch.lower() for ch in str(value) if ch.isalnum())


def _is_datetime_column_name(col_name: str) -> bool:
	"""
	Identify true timestamp/date columns and exclude duration/counter fields.
	"""
	normalized = _normalize_col_name(col_name)
	if not any(token in normalized for token in ("time", "date", "timestamp")):
		return False

	# Duration-like fields are numeric counters, not absolute datetimes.
	excluded_tokens = (
		"timesince",
		"duration",
		"elapsed",
		"stationary",
		"moving",
		"seconds",
		"millis",
		"milliseconds",
	)
	if any(token in normalized for token in excluded_tokens):
		return False

	return True


def _resolve_torqlogs_columns(conn, requested_columns: list[str]) -> dict[str, str]:
	inspector = inspect(conn)
	actual_columns = [str(col["name"]) for col in inspector.get_columns("torqlogs")]
	normalized_actual = {_normalize_col_name(col): col for col in actual_columns}
	resolved: dict[str, str] = {}
	for requested in requested_columns:
		actual = normalized_actual.get(_normalize_col_name(requested))
		if actual:
			resolved[requested] = actual
	return resolved


def _repair_postgres_column_type_mismatches(conn, table_name: str, column_types: dict):
	"""
	Repair known timestamp-vs-numeric schema mismatches in PostgreSQL.
	"""
	if conn.dialect.name != "postgresql":
		return

	inspector = inspect(conn)
	for col in inspector.get_columns(table_name):
		col_name = str(col["name"])
		actual_type = str(col.get("type", "")).lower()
		expected_type = column_types.get(col_name)
		if expected_type is None:
			expected_type = column_types.get(_normalize_col_name(col_name))

		expected_cls = expected_type if isinstance(expected_type, type) else type(expected_type)
		if expected_cls not in (Float, Integer):
			continue
		if "timestamp" not in actual_type:
			continue

		target_sql_type = "double precision" if expected_cls is Float else "bigint"
		alter_sql = text(
			f'''ALTER TABLE "{table_name}"
			ALTER COLUMN "{col_name}" TYPE {target_sql_type}
			USING CASE
				WHEN "{col_name}" IS NULL THEN NULL
				WHEN "{col_name}"::text ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN "{col_name}"::text::{target_sql_type}
				ELSE NULL
			END'''
		)
		logger.warning(
			f"Repairing PostgreSQL column type mismatch for {table_name}.{col_name}: "
			f"{actual_type} -> {target_sql_type}"
		)
		conn.execute(alter_sql)
	if conn.in_transaction():
		conn.commit()


def _collapse_duplicate_dataframe_columns(df: pd.DataFrame, csvfile: Path) -> pd.DataFrame:
	"""
	Collapse duplicate DataFrame column names by coalescing values left-to-right.
	This prevents `to_sql` from failing when multiple source headers map to the same canonical name.
	"""
	if not df.columns.duplicated().any():
		return df

	resolved_duplicates = [str(col) for col in pd.unique(df.columns[df.columns.duplicated()])]
	logger.warning(f"Resolved duplicate canonical columns for {csvfile}: {resolved_duplicates}")

	ordered_unique_cols: list[str] = []
	seen = set()
	for col in df.columns:
		col_name = str(col)
		if col_name in seen:
			continue
		seen.add(col_name)
		ordered_unique_cols.append(col_name)

	series_list: list[pd.Series] = []
	for col_name in ordered_unique_cols:
		col_block = df.loc[:, df.columns == col_name]
		if col_block.shape[1] == 1:
			series_list.append(col_block.iloc[:, 0].rename(col_name))
			continue

		# Treat whitespace-only strings as missing, then take first non-null value per row.
		coalesced = col_block.replace(r"^\s*$", pd.NA, regex=True).bfill(axis=1).iloc[:, 0]
		series_list.append(coalesced.rename(col_name))

	if not series_list:
		return pd.DataFrame(index=df.index)

	# Build all columns in one concat to avoid block fragmentation warnings.
	return pd.concat(series_list, axis=1).copy()


def _extract_trip_start_from_dataframe(df: pd.DataFrame) -> datetime | None:
	"""
	Extract the earliest trip timestamp from the full file content.
	Uses gpstime first, then devicetime.
	"""
	if df.empty:
		return None

	norm_col_map = {_normalize_col_name(c): c for c in df.columns}
	time_col_raw = norm_col_map.get('gpstime') or norm_col_map.get('devicetime')
	if not time_col_raw:
		return None

	parsed_datetimes: list[datetime] = []
	for value in df[time_col_raw].tolist():
		if pd.isna(value):
			continue
		dt = None
		if isinstance(value, pd.Timestamp):
			dt = value.to_pydatetime()
		elif isinstance(value, datetime):
			dt = value
		else:
			dt = convert_string_to_datetime(str(value))

		if dt is None:
			continue
		if dt.tzinfo is None:
			dt = dt.replace(tzinfo=pytz.UTC)
		else:
			dt = dt.astimezone(pytz.UTC)
		parsed_datetimes.append(dt)

	if not parsed_datetimes:
		return None

	return min(parsed_datetimes)



def _ensure_torqtrips_metric_columns(conn, metric_names: list[str]) -> None:
	inspector = inspect(conn)
	existing = {str(col["name"]).lower() for col in inspector.get_columns("torqtrips")}
	if conn.dialect.name == "postgresql":
		numeric_sql_type = "DOUBLE PRECISION"
	else:
		numeric_sql_type = "REAL"

	for metric in metric_names:
		for suffix in ("min", "max", "avg", "stdev"):
			col_name = f"{metric}_{suffix}"
			if col_name.lower() in existing:
				continue
			conn.execute(text(f'ALTER TABLE torqtrips ADD COLUMN "{col_name}" {numeric_sql_type}'))
			existing.add(col_name.lower())

def update_trip_and_file_for_fileid(conn, fileid):
	"""
	Update TorqFile and Torqtrips for a single fileid after inserting its data.
	"""
	resolved = _resolve_torqlogs_columns(conn, ['gpstime', 'latitude', 'longitude', *TRIP_METRIC_COLUMNS])
	time_col = resolved.get('gpstime')
	lat_col = resolved.get('latitude')
	lon_col = resolved.get('longitude')
	if not (time_col and lat_col and lon_col):
		logger.error(f'Missing required torqlogs columns for fileid {fileid}: {resolved}')
		return

	resolved_metric_pairs = [(metric, resolved[metric]) for metric in TRIP_METRIC_COLUMNS if metric in resolved]
	_ensure_torqtrips_metric_columns(conn, [metric for metric, _ in resolved_metric_pairs])

	# Aggregate trip info for this fileid
	metric_select_parts: list[str] = []
	for idx, (_, actual_col) in enumerate(resolved_metric_pairs):
		metric_select_parts.extend([
			f'MIN("{actual_col}") AS "m_{idx}_min"',
			f'MAX("{actual_col}") AS "m_{idx}_max"',
			f'AVG("{actual_col}") AS "m_{idx}_avg"',
			f'COUNT("{actual_col}") AS "m_{idx}_count"',
			f'AVG("{actual_col}" * "{actual_col}") AS "m_{idx}_avg_sq"',
		])
	metric_sql = (",\n\t\t" + ",\n\t\t".join(metric_select_parts)) if metric_select_parts else ""

	sql = f"""
	SELECT
		fileid,
		MIN("{time_col}") AS trip_start,
		MAX("{time_col}") AS trip_end,
		MIN("{lat_col}") AS startlat,
		MIN("{lon_col}") AS startlon,
		MAX("{lat_col}") AS endlat,
		MAX("{lon_col}") AS endlon,
		COUNT(*) AS row_count
		{metric_sql}
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

	metric_values: dict[str, float | None] = {}
	for idx, (metric_name, _) in enumerate(resolved_metric_pairs):
		min_val = row.get(f"m_{idx}_min")
		max_val = row.get(f"m_{idx}_max")
		avg_val = row.get(f"m_{idx}_avg")
		count_val = int(row.get(f"m_{idx}_count") or 0)
		avg_sq_val = row.get(f"m_{idx}_avg_sq")

		metric_values[f"{metric_name}_min"] = float(min_val) if min_val is not None else None
		metric_values[f"{metric_name}_max"] = float(max_val) if max_val is not None else None
		metric_values[f"{metric_name}_avg"] = float(avg_val) if avg_val is not None else None
		if count_val > 1 and avg_val is not None and avg_sq_val is not None:
			variance = max(0.0, float(avg_sq_val) - (float(avg_val) ** 2))
			metric_values[f"{metric_name}_stdev"] = variance ** 0.5
		else:
			metric_values[f"{metric_name}_stdev"] = None

	# Calculate trip duration
	trip_duration = None
	if trip_start and trip_end:
		try:
			trip_start_dt = convert_string_to_datetime(str(trip_start))
			trip_end_dt = convert_string_to_datetime(str(trip_end))
			if isinstance(trip_start_dt, datetime) and isinstance(trip_end_dt, datetime):
				trip_duration = (trip_end_dt - trip_start_dt).total_seconds()
			else:
				logger.warning(f"Could not parse trip_start or trip_end as datetime for fileid {fileid}: {trip_start} ({type(trip_start)}), {trip_end} ({type(trip_end)})")
				trip_duration = None
		except Exception as e:
			logger.error(f'{e} {type(e)} {trip_start=} {trip_end=}')
			trip_duration = None
	# Calculate trip distance (sum of point-to-point GPS distances for this fileid)
	trip_distance = 0.0
	try:
		if conn.dialect.name == "postgresql":
			# Compute the full path distance in SQL to avoid pulling thousands of rows to Python.
			distance_sql = text(f'''
				WITH ordered AS (
					SELECT
						"{lat_col}"::double precision AS lat,
						"{lon_col}"::double precision AS lon,
						LAG("{lat_col}"::double precision) OVER (ORDER BY "{time_col}" ASC) AS prev_lat,
						LAG("{lon_col}"::double precision) OVER (ORDER BY "{time_col}" ASC) AS prev_lon
					FROM torqlogs
					WHERE fileid = :fileid
				)
				SELECT COALESCE(SUM(
					6371000.0 * 2.0 * atan2(
						sqrt(
							pow(sin(radians((lat - prev_lat) / 2.0)), 2)
							+ cos(radians(prev_lat)) * cos(radians(lat))
							* pow(sin(radians((lon - prev_lon) / 2.0)), 2)
						),
						sqrt(
							GREATEST(
								0.0,
								1.0 - (
									pow(sin(radians((lat - prev_lat) / 2.0)), 2)
									+ cos(radians(prev_lat)) * cos(radians(lat))
									* pow(sin(radians((lon - prev_lon) / 2.0)), 2)
								)
							)
						)
					)
				), 0.0) AS trip_distance
				FROM ordered
				WHERE prev_lat IS NOT NULL AND prev_lon IS NOT NULL
			''')
			trip_distance = float(conn.execute(distance_sql, {"fileid": fileid}).scalar() or 0.0)
		else:
			df_gps = pd.read_sql(
				text(f'SELECT "{lat_col}" AS latitude, "{lon_col}" AS longitude FROM torqlogs WHERE fileid = :fileid ORDER BY "{time_col}" ASC'),
				conn,
				params={"fileid": fileid}
			)
			if len(df_gps) > 1:
				distances = [
					haversine(
						df_gps.iloc[i-1]['latitude'], df_gps.iloc[i-1]['longitude'],
						df_gps.iloc[i]['latitude'], df_gps.iloc[i]['longitude']
					)
					for i in range(1, len(df_gps))
				]
				trip_distance = float(sum(distances))
			else:
				trip_distance = 0.0
	except TypeError as e:
		logger.warning(f"Error calculating trip_distance for fileid {fileid}: {e} {type(e)}")
		trip_distance = 0.0

	except Exception as e:
		logger.error(f"Error calculating trip_distance for fileid {fileid}: {e} {type(e)}")
		trip_distance = 0.0

	# Insert if missing, then update all calculated fields.
	conn.execute(text("""
		INSERT INTO torqtrips (fileid, tripdate, time, trip_distance)
		SELECT :fileid, :trip_start, :trip_duration, :trip_distance
		WHERE NOT EXISTS (SELECT 1 FROM torqtrips WHERE fileid = :fileid)
	"""), {
		"fileid": fileid,
		"trip_start": trip_start,
		"trip_duration": trip_duration,
		"trip_distance": trip_distance
	})

	set_parts = [
		"tripdate = :trip_start",
		"time = :trip_duration",
		"trip_distance = :trip_distance",
	]
	for key in metric_values:
		set_parts.append(f'"{key}" = :{key}')
	update_sql = text(
		"UPDATE torqtrips SET " + ", ".join(set_parts) + " WHERE fileid = :fileid"
	)
	update_params = {
		"fileid": fileid,
		"trip_start": trip_start,
		"trip_duration": trip_duration,
		"trip_distance": trip_distance,
		**metric_values,
	}
	conn.execute(update_sql, update_params)

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
	logger.debug(f'Updated fileid {fileid}: trip_start={trip_start}, trip_end={trip_end}, duration={trip_duration}, distance={trip_distance}, rows={row_count}')
def read_csvs_to_dataframe_and_insert(args, table_name='torqlogs') -> None:
	"""
	Read all CSV files into a DataFrame, normalize column names, and insert into SQLite table.
	Handles varying columns, missing data, and extra spaces in column names.
	Returns the concatenated DataFrame and a dictionary of column stats.
	"""
	# Define column types based on sample data

	# Initialize dictionary to store column stats and file info
	pd_columns = {'stats': {}, 'files': {}}
	skipped_count = 0
	# Get list of CSV files
	csv_files = list(Path(args.logpath).glob("**/trackLog*.csv"))
	if not csv_files:
		logger.warning("No CSV files found")
		return None

	# First pass: Collect and validate headers from all files
	all_columns = set()
	valid_files = []
	csvhash = ''
	session = get_engine_session(args)
	# engine = create_engine(f'sqlite:///{args.dbfile}', echo=False, connect_args={'timeout': 30, 'isolation_level': None, 'check_same_thread': False})
	# Initialize database schema first
	try:
		database_init(session.get_bind())
	except Exception as e:
		logger.error(f"Error initializing database: {e} {type(e)}")
		return None

	for file_idx, csvfile in enumerate(csv_files):
		linecount = 0
		try:
			with open(csvfile, 'rb') as f:
				d = f.readlines()
			linecount = len(d)
		except Exception as e:
			logger.error(f"Error counting lines in {csvfile}: {e} {type(e)}")
			continue
		try:
			if csvfile.stat().st_size < MIN_FILESIZE or linecount < args.min_row_count:
				logger.warning(f"Skipping {csvfile} - file size too small {csvfile.stat().st_size} min {MIN_FILESIZE} lines {linecount}")
				continue

			# Check if file has already been processed
			csvhash = md5(Path(csvfile).read_bytes()).hexdigest()
			with session.get_bind().connect() as conn:  # type: ignore[union-attr]
				existing_file = conn.execute(text("SELECT fileid FROM torqfiles WHERE csvhash = :csvhash"),{"csvhash": csvhash}).first()

			if existing_file:
				skipped_count += 1
				# logger.info(f"[{file_idx}/{len(csv_files)}] File {csvfile} already processed, skipping")
				continue

			# Read header row plus one data row (for trip_start date detection)
			df = pd.read_csv(csvfile, nrows=1)

			# Normalize columns using shared Torq header mapping.
			original_columns = df.columns.to_list()
			normalized_columns = [canonicalize_column_name(col) for col in original_columns]

			# Validate columns - check for empty or numeric column names
			if any(not col or col[0].isdigit() for col in normalized_columns):
				logger.warning(f"Skipping {csvfile} - invalid column names")
				continue

			# Check if a file with the same trip_start datetime already exists in the DB.
			# Primary: parse datetime from first CSV data row (matches final trip_start derivation).
			# Fallback: extract datetime from filename (trackLog-YYYY-Mon-DD_HH-MM-SS.csv).
			trip_start_datetime = None
			if not df.empty:
				norm_col_map = {_normalize_col_name(c): c for c in df.columns}
				time_col_raw = norm_col_map.get('gpstime') or norm_col_map.get('devicetime')
				if time_col_raw is not None:
					first_val = df[time_col_raw].iloc[0]
					trip_start_dt = convert_string_to_datetime(str(first_val)) if pd.notna(first_val) else None
					if trip_start_dt is not None:
						trip_start_datetime = trip_start_dt
			if trip_start_datetime is None:
				filename_match = re.match(r'trackLog-(\d{4}-[A-Za-z]{3}-\d{2}_\d{2}-\d{2}-\d{2})', csvfile.stem)
				if filename_match:
					try:
						trip_start_datetime = datetime.strptime(filename_match.group(1), '%Y-%b-%d_%H-%M-%S').replace(tzinfo=pytz.UTC)
					except ValueError:
						trip_start_datetime = None
			if trip_start_datetime is not None:
				with session.get_bind().connect() as chk_conn:  # type: ignore[union-attr]
					if chk_conn.dialect.name == 'sqlite':
						date_sql = text("SELECT fileid FROM torqfiles WHERE trip_start IS NOT NULL AND datetime(trip_start) = datetime(:ts_dt)")
					else:
						date_sql = text("SELECT fileid FROM torqfiles WHERE trip_start IS NOT NULL AND trip_start = CAST(:ts_dt AS timestamp)")
					existing_trip = chk_conn.execute(date_sql, {"ts_dt": trip_start_datetime.strftime('%Y-%m-%d %H:%M:%S')}).first()
				if existing_trip:
					skipped_count += 1
					logger.warning(f"[{file_idx}/{len(csv_files)}] Skipping {csvfile} - trip_start datetime {trip_start_datetime} already in DB (fileid {existing_trip[0]})")
					continue

			all_columns.update(normalized_columns)
			valid_files.append((csvfile, normalized_columns, csvhash))

			# Store file info
			pd_columns['files'][str(csvfile)] = {
				'filename': str(csvfile),
				'columns': normalized_columns
			}

		except Exception as e:
			logger.error(f"Error reading headers from {csvfile}: {e} {type(e)}")
			continue
	if args.debug and skipped_count > 0:
		logger.debug(f'skipped {skipped_count} files that were already processed based on hash')
	if not valid_files:
		logger.warning("No valid CSV files found after header validation")
		return None
	if args.file_limit:
		random.shuffle(valid_files)
		valid_files = [k for k in valid_files][0:10]
	logger.info(f"Found {len(valid_files)} valid CSV files, skipped {skipped_count}. Columns: {len(all_columns)}")
	column_types = COLUMN_TYPES.copy()
	for col in all_columns:
		if col in column_types:
			continue
		if _is_datetime_column_name(col):
			column_types[col] = DateTime

	# Update database schema if needed
	try:
		create_or_update_table(session, table_name, all_columns, column_types)
	except Exception as e:
		logger.error(f"Error updating table schema: {e} {type(e)}")
		return None

	# Second pass: Read and insert data from valid files
	df = pd.DataFrame()
	if args.debug:
		logger.debug(f"Starting data insertion for {len(valid_files)} files into table {table_name} with {len(all_columns)} columns")
	with session.get_bind().connect() as conn:  # type: ignore[union-attr]
		if conn.dialect.name == "sqlite":
			conn.execute(text("PRAGMA journal_mode = WAL"))  # Use Write-Ahead Logging
			conn.execute(text("PRAGMA synchronous = NORMAL"))  # Reduce synchronization
		else:
			_repair_postgres_column_type_mismatches(conn, table_name, column_types)

		inspector = inspect(conn)
		actual_table_columns = [str(col["name"]) for col in inspector.get_columns(table_name)]
		normalized_actual_columns = {
			_normalize_col_name(col_name): col_name for col_name in actual_table_columns
		}
		fileid = None
		try:
			for csv_idx, (csvfile, normalized_columns, csvhash) in enumerate(valid_files):
				# SQLAlchemy 2.x may start a transaction implicitly (autobegin).
				# Ensure each file starts with a clean transaction boundary.
				if conn.in_transaction():
					conn.rollback()
				try:
					read_started = time.perf_counter()

					# Read CSV file
					df = pd.read_csv(csvfile, low_memory=False, on_bad_lines='skip', encoding='utf-8', encoding_errors='replace')

					for col in df.columns:
						if _is_datetime_column_name(col):
							try:
								# df[col] = pd.to_datetime(df[col], errors='coerce')
								# df[col] = df[col].apply(lambda x: convert_string_to_datetime(x) if pd.notnull(x) else pd.NaT)
								df[col] = df[col].apply(lambda x: convert_string_to_datetime(x) if isinstance(x, str) and pd.notnull(x) else pd.NaT)  # type: ignore
							except Exception as e:
								logger.warning(f"Could not convert column {col} to datetime: {e} {type(e)} in {csvfile}")

					# Pre-send duplicate check from full file content (uses minimum trip timestamp).
					trip_start_candidate = _extract_trip_start_from_dataframe(df)
					if trip_start_candidate is not None:
						if conn.dialect.name == 'sqlite':
							date_sql = text("SELECT fileid FROM torqfiles WHERE trip_start IS NOT NULL AND datetime(trip_start) = datetime(:ts_dt)")
						else:
							date_sql = text("SELECT fileid FROM torqfiles WHERE trip_start IS NOT NULL AND trip_start = CAST(:ts_dt AS timestamp)")
						existing_trip = conn.execute(date_sql, {"ts_dt": trip_start_candidate.strftime('%Y-%m-%d %H:%M:%S')}).first()
						if existing_trip:
							skipped_count += 1
							logger.warning(
								f"[{csv_idx}/{len(valid_files)}] Skipping {csvfile} - duplicate trip_start "
								f"{trip_start_candidate} already exists (fileid {existing_trip[0]})"
							)
							continue

					# Create TorqFile entry only after duplicate check passes.
					result = conn.execute(
						text("INSERT INTO torqfiles (csvfile, csvhash, import_date) VALUES (:csvfile, :csvhash, :import_date) RETURNING fileid"),
						{"csvfile": str(csvfile), "csvhash": csvhash, "import_date": datetime.now()}
					)
					fileid = result.scalar()

					# Add fileid column first
					df.insert(0, 'fileid', fileid)

					# Process columns and data
					df = df.rename(columns=canonicalize_columns(list(df.columns)))
					# Align canonicalized DataFrame columns with actual DB column names (case-sensitive in PostgreSQL).
					db_col_rename_map = {}
					for c in df.columns:
						actual_col = normalized_actual_columns.get(_normalize_col_name(c))
						if actual_col and actual_col != c:
							db_col_rename_map[c] = actual_col
					if db_col_rename_map:
						df = df.rename(columns=db_col_rename_map)
					df = _collapse_duplicate_dataframe_columns(df, csvfile)
					df = df.replace(['-', '∞', 'inf', '-inf'], pd.NA)

					# Convert numeric columns
					for col in df.columns:
						if col in COLUMN_TYPES and COLUMN_TYPES[col] in [Float, Integer]:
							df[col] = pd.to_numeric(df[col], errors='coerce')

					# Remove rows that are identical to the header (possible repeated headers)
					header_row = list(df.columns)
					df = df[~df.apply(lambda row: list(row) == header_row, axis=1)]
					df = df[~df.apply(lambda row: row.astype(str).str.contains(' Device Time').any(), axis=1)]
					read_elapsed = float(time.perf_counter() - read_started)

					pre_filter_columns = list(df.columns)
					allowed_cols = set(actual_table_columns)
					ordered_cols = [col for col in ['fileid'] if col in df.columns and col in allowed_cols]
					ordered_cols.extend(sorted([col for col in df.columns if col != 'fileid' and col in allowed_cols]))
					df = df[ordered_cols]
					if len(df.columns) == 0:
						raise ValueError(
							f"No matching columns remain after filtering for table {table_name}. "
							f"Input columns sample: {pre_filter_columns[:10]}"
						)

					# Insert data
					# SQLite limits bind variables to 999 (or 32766 on newer builds).
					# Use the conservative limit so chunksize * num_columns stays within it.
					SQLITE_MAX_VARS = 999
					safe_chunksize = max(1, SQLITE_MAX_VARS // len(df.columns))
					requested_chunksize = max(1, int(args.sqlchunksize))
					effective_chunksize = min(requested_chunksize, safe_chunksize)
					if len(df) >= args.min_row_count:
						# logger.info(f"[{csv_idx}/{len(valid_files)}] Sending {len(df)} rows from {csvfile} with fileid {fileid}")
						send_started = time.perf_counter()
						df.to_sql(table_name, conn, if_exists='append', index=False, method='multi', chunksize=effective_chunksize,)

						# Update trip and file info for this fileid
						update_trip_and_file_for_fileid(conn, fileid)

						send_elapsed = float(time.perf_counter() - send_started)

						# Update TorqFile import timings and row count
						conn.execute(
							text(
								"""
								UPDATE torqfiles
								SET sent_rows = :rows,
									readtime = :readtime,
									sendtime = :sendtime
								WHERE fileid = :fileid
								"""
							),
							{"rows": len(df), "readtime": read_elapsed, "sendtime": send_elapsed, "fileid": fileid}
						)
						logger.info(f"[{csv_idx}/{len(valid_files)}] Sent {len(df)} rows from {csvfile} fileid {fileid}")
						if conn.in_transaction():
							conn.commit()
					else:
						logger.warning(f"[{csv_idx}/{len(valid_files)}] Skipping {csvfile} with fileid {fileid} - row count {len(df)} below minimum threshold")
						conn.execute(text("DELETE FROM torqfiles WHERE fileid = :fileid"), {"fileid": fileid})
						if conn.in_transaction():
							conn.commit()
				except UniqueViolation as e:
					logger.warning(f"Duplicate entry for {csvfile} with fileid {fileid}, skipping: {e}")
					if conn.in_transaction():
						conn.rollback()
					continue
				except IntegrityError as e:
					if conn.in_transaction():
						conn.rollback()
					err = str(getattr(e, "orig", e)).lower()
					if "uq_torqfiles_trip_start" in err or "trip_start" in err and "duplicate" in err:
						logger.warning(f"Skipping {csvfile} with fileid {fileid} due to duplicate trip_start: {e}")
						continue
					logger.error(f"Integrity error for {csvfile} with fileid {fileid}: {e}")
					continue
				except Exception as e:
					if conn.in_transaction():
						conn.rollback()
					logger.error(f"Error processing {csvfile}: {e} {type(e)}")
					if args.debug:
						logger.error(f"DataFrame columns: {df.columns.tolist()}")
					continue

		except Exception as e:
			errmsg = f"Transaction failed: {e} {type(e)}"
			logger.error(errmsg)
			raise Exception(errmsg)
	# engine.dispose()
	return None

def get_csv_files(searchpath: Path, args):
	# scan searchpath for csv files
	torqcsvfiles = [({"csvfile": k, "csvhash": md5(open(k, "rb").read()).hexdigest(), "size": os.stat(k).st_size, "dbmode": args.dbmode, }) for k in searchpath.glob("**/*.csv") if k.stat().st_size >= MIN_FILESIZE]  # and not os.path.exists(f'{k}.fixed.csv')]
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

def send_torqtripdata(stats_data: dict, session: Session, args: argparse.Namespace):
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

def generate_torqdata(df: pd.DataFrame, session: Session, args: argparse.Namespace):
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

def convert_string_to_datetime(s: str) -> datetime | None:
	"""
	Convert string-like values to UTC datetime.
	Returns None for invalid/unparseable values so inserts become SQL NULL.
	"""
	if s is None:
		return None
	if not isinstance(s, str):
		s = str(s)
	s = s.strip()
	if not s:
		return None

	known_formats = {
		20: fmt_20,
		24: fmt_24,
		26: fmt_26,
		28: fmt_28,
		30: fmt_30,
		34: fmt_34,
		36: fmt_36,
	}
	try:
		fmt = known_formats.get(len(s))
		if fmt:
			dt = datetime.strptime(s, fmt)
			if dt.tzinfo is None:
				return dt.replace(tzinfo=pytz.UTC)
			return dt.astimezone(pytz.UTC)

		parsed = pd.to_datetime(s, errors="coerce", utc=True)
		if pd.isna(parsed):
			return None
		return parsed.to_pydatetime()
	except (ValueError, TypeError, KeyError) as e:
		logger.warning(f"dateconverter {type(e)} {e} {s=}")
		return None
