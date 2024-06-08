#!/usr/bin/python
import os
import sys
import re
import argparse
from loguru import logger
from pathlib import Path
from datetime import datetime
import shutil
import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, inspect, select, Numeric, DateTime, text, BIGINT, BigInteger, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import OperationalError, DataError

# tool to rename and import tripLogs from older versions of the app
# get tripdate from profile.properties file and rename the log file to the new format
# tripdate should match with foldername of each trip, named as unix timestamp (13 digits)
# timestamp is the start time of the trip, first line of the log file
# example :
# original path /torq/tripLogs/1708245165793
# new filenames are in the format: trackLog-2021-Dec-01_23-40-45.csv
# datetime.fromtimestamp(1708245165793/1000).strftime("%Y-%b-%d_%H-%M-%S")

def read_profile(profile_fn):
	# read profile.properties file, to extract some data
	tripdate = None
	try:
		with open(profile_fn, 'r') as f:
			data = f.readlines()
		if len(data) == 8 or len(data) == 6:
			#pdata_date = str(data[1][1:]).strip('\n')
			#tripdate = datetime.strptime(pdata_date ,'%a %b %d %H:%M:%S %Z%z %Y')
			if len(data[1]) == 30:
				tripdate = datetime.strptime((str(data[1][1:]).strip('\n')), '%a %b %d %H:%M:%S %Z %Y')
			elif len(data[1]) == 36:
				#Tue May 17 17:55:43 GMT+02:00 2022
				tripdate = datetime.strptime((str(data[1][1:]).strip('\n')), '%a %b %d %H:%M:%S %Z%z %Y')
			else:
				logger.warning(f'unknown date format {data[1]}')
				tripdate = data[1]
		else:
			logger.warning(f'profile.properties file {profile_fn} has {len(data)} lines {data}')
	except Exception as e:
		logger.error(f'unhandled {type(e)} {e}')
	finally:
		return tripdate

def transfer_older_logs():
	oldlogpath = Path('/home/kth/development/torq/tripLogs')
	dest_log_path = Path('/home/kth/development/torq/torqueLogs') # where to put renamed log files

	old_dirs = old_dirs = [k for k in oldlogpath.glob('*') if k.is_dir() and len(str(k.name))==13]
	# pick only directories with 13 digits

	transfered_logs = []
	# to keep track of the logs that have been transfered

	logger.debug(f'found {len(old_dirs)} old tripLogs')
	for od in old_dirs:
		profile_fn = os.path.join(od, 'profile.properties')
		old_timestamp = datetime.fromtimestamp(int(od.name)/1000).strftime("%Y-%b-%d_%H-%M-%S")
		if Path(profile_fn).exists():
			# read profile.properties file, to extract some data
			profiledata = read_profile(profile_fn)
		else:
			logger.warning(f'no profile.properties file found in {od}')
			profiledata = None
		# rename log file to new format
		if profiledata:
			trip_date = profiledata.strftime("%Y-%b-%d_%H-%M-%S")
			new_log_fn = Path(os.path.join(dest_log_path, f'trackLog-{trip_date}.csv'))
			if len(new_log_fn.name) != 33:
				logger.warning(f'new log filename {new_log_fn} is not 33 chars long')
			if Path(new_log_fn).exists():
				logger.warning(f'file {new_log_fn} exists, skipping')
			else:
				#print(f'od: {od.name} -> {old_timestamp} pd: {profiledata} td: {trip_date}')
				old_log_name = os.path.join(od, 'trackLog.csv')
				print(f'move/copy from {old_log_name} to {new_log_fn}')
				try:
					shutil.copyfile(old_log_name, new_log_fn)
					transfered_logs.append(new_log_fn)
				except Exception as e:
					logger.error(f'Error {type(e)} {e} {old_log_name} -> {new_log_fn}')
		else:
			logger.warning(f'could not extract profiledata from {profile_fn}')
	logger.info(f'transfered {len(transfered_logs)} of {len(old_dirs)} old tripLogs to {dest_log_path}')
	return transfered_logs

def check_and_fix_logs(logfiles):
	# iterate all log files (that have not been fixed) , check for bad chars, remove them
	# skip files that have been fixed already, by checking in the database
	# return a list of log files that have been fixed, TorqFile.fixed_flag should be 1
	new_log_files = []
	dburl = 'sqlite:///torqfiskur.db'
	engine = create_engine(dburl, echo=False, connect_args={'check_same_thread': False})
	Session = sessionmaker(bind=engine)
	session = Session()
	for log in logfiles:
		# check if log file has been fixed already, if not fix it
		# mark the log file as fixed in the database, TorqFile.fixed_flag = 1
		pass

def get_cols(logpath, extglob="**/trackLog-*.csv", debug=False):
	"""
	collect all columns from all log files in the path
	params: logpath where to search, extglob glob pattern to use
	prep data base columns ....
	"""
	columns = {}
	stats = {}
	filestats = {}
	# '/home/kth/development/torq/torqueLogs.bakbak/').glob("**/trackLog-*.bak"
	for logfile in Path(logpath).glob(extglob):
		df = pd.read_csv(logfile, nrows=1)
		newcolnames = ','.join([re.sub(r'\W', '', col) for col in df.columns]).encode('ascii', 'ignore').decode()
		fs = {
			'filename': logfile.name,
			'newcolnames': newcolnames,
		}
		filestats[logfile.name] = fs
		for c in newcolnames.split(','):
			c = c.lower() # everything lowercase
			if len(c) == 0 or c[0].isnumeric():
				logger.warning(f'invalid/empty column {c} in {logfile}')
				continue
			if c not in columns:
				info = {'count': 1, 'files': [logfile.name]}
				stats[c] = info
				columns[c] = {'count':1}
				if debug:
					logger.debug(f'col: {c} {len(columns)} ')
			else:
				columns[c]['count'] += 1
				stats[c]['count'] += 1
				stats[c]['files'].append(logfile.name)
	if debug:
		avg_cols = 0
		avh_chars = 0
		total_cols = 0
		total_chars = 0
		for f in filestats:
			colcount = len(filestats[f].get('newcolnames').split(','))
			charcnt = len(filestats[f].get('newcolnames'))
			total_cols += colcount
			total_chars += charcnt
			# print(f'{f} {colcount} {charcnt}')
		print(f'{"="*25}')
		print(f'avg cols: {total_cols/len(filestats)} avg chars: {total_chars/len(filestats)}')
		for s in stats:
			scnt = stats[s]["count"]
			print(f'{s} {scnt}')
			if scnt == 1:
				for sf in stats[s]["files"]:
					print(f'\t - {sf}')
	print(f'{"="*25}')
	return stats, columns

def main():
	parser = argparse.ArgumentParser(description="converter ")

	parser.add_argument("--path", nargs="?", default=".", help="path", action="store")
	parser.add_argument('--getcols', default=False, help="prep cols", action="store_true", dest='getcols')
	parser.add_argument('--transfer', default=False, help="transfer old logs", action="store_true", dest='transfer')
	parser.add_argument('-d', '--debug', default=False, help="debugmode", action="store_true", dest='debug')

	args = parser.parse_args()
	if args.getcols:
		# get columns from all log files in the path
		stats, columns = get_cols(args.path, debug=args.debug)
		# columns = sorted(columns, key=lambda x: columns[x]['count'], reverse=True)
		columns = sorted(columns, key=lambda x: ( columns[x]['count'], columns[x]), reverse=True)
		for c in columns:
			if 'date' in c or 'time' in c:
				lineout = f"{c} = Column('{c}', DateTime)"
			else:
				lineout = f"{c} = Column('{c}', Float)" # Column('longitude', Float)
			print(lineout)
		# print(f'{columns=}')
		sys.exit(0)
	if args.transfer:
		# oldlogpath root of the old tripLogs files, containing subfolder, each name as unix timestamp of the trip
		# each sub folder contains a log file and profile.properties file
		# step one, transfer older logs to new location with new filenames
		new_old_logs = transfer_older_logs()
		logger.debug(f'transfered {len(new_old_logs)} old logs')
		# step two, read each log file, remove bad chars
		fixed = check_and_fix_logs(new_old_logs)
		print(f'{fixed=}')
		sys.exit(0)

if __name__ == '__main__':
	main()