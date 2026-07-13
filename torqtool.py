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
from sqlalchemy.orm import sessionmaker
import sqlite3
from datamodels import TorqFile, database_init, stable_fileid_from_csvhash,Position, Label
from utils import get_parser, get_engine_session, convert_string_to_datetime, read_csvs_to_dataframe_and_insert
from schemas import canonicalize_columns

# tool to rename and import tripLogs from older versions of the app
# get tripdate from profile.properties file and rename the log file to the new format
# tripdate should match with foldername of each trip, named as unix timestamp (13 digits)
# timestamp is the start time of the trip, first line of the log file
# example :
# original path /torq/tripLogs/1708245165793
# new filenames are in the format: trackLog-2021-Dec-01_23-40-45.csv
# datetime.fromtimestamp(1708245165793/1000).strftime("%Y-%b-%d_%H-%M-%S")

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
		session = get_engine_session(args)  # , session
		database_init(session.get_bind())
		sess = sessionmaker(bind=session.get_bind())
		s = sess()
		logcount = s.execute(text("select count(*) from torqlogs")).all()
		logger.info(f'{logcount=}')
		s.close()
		read_csvs_to_dataframe_and_insert(args)

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
