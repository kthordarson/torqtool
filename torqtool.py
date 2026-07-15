#!/usr/bin/python3
import asyncio
import sys
from loguru import logger
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from datamodels import database_init
from utils import get_parser, get_engine_session, read_csvs_to_dataframe_and_insert

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
