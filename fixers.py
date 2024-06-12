#!/usr/bin/python

# fixers in here

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
from sqlalchemy import create_engine
from sqlalchemy.exc import DataError, OperationalError, NoResultFound
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import MultipleResultsFound
from commonformats import fmt_20, fmt_24, fmt_26, fmt_28, fmt_30, fmt_34, fmt_36
from datamodels import TorqFile, database_init
from schemas import ncc, schema_datatypes
from utils import get_engine_session, get_fixed_lines, get_sanatized_column_names,MIN_FILESIZE, convert_string_to_datetime

def replace_headers(newfiles:list, args):
	"""
	newfiles a list of new files we need to process / send
	strip leading spaces off the column headers
	returns dict with two list of files, successfully processed files, and error files
	"""

	res = {
		'files_to_read': [],
		'errorfiles': [],
	}
	for f in newfiles:
		if fix_column_names(f, args):
			res['files_to_read'].append(f)
		else:
			res['errorfiles'].append(f)
	if len(res['errorfiles']) > 0:
		logger.warning(f"errors: {len(res['errorfiles'])} {res['errorfiles']}")
	else:
		logger.info(f'fixed {len(res["files_to_read"])} files')
	return res


def fix_column_names(csvfile:str, args):
	"""
	strip leading spaces from column names and saves the fil
	# todo maybe renmame columns here ?
	"""
	subchars = [', ',',Â','∞','Â°F','Â°','â°','â']
	try:
		with open(csvfile,'r') as f:
			rawdata = f.readlines()
		#for badchar in subchars:
		rawdata[0] = get_sanatized_column_names(rawdata[0]) #re.sub(badchar,',',rawdata[0])
		# rawdata[0] = re.sub(', ',',',rawdata[0])
		# rawdata[0] = re.sub('Â','',rawdata[0])
		# rawdata[0] = re.sub('∞','',rawdata[0])
		# rawdata[0] = re.sub('Â°F','F',rawdata[0])
		# rawdata[0] = re.sub('Â°','',rawdata[0])
		# rawdata[0] = re.sub('â°','',rawdata[0])
		# rawdata[0] = re.sub('â','',rawdata[0])
		if not args.skipwrites:
			logger.info(f'writing columns to {csvfile}')
			shutil.copy(csvfile, f'{csvfile}.colfixbak')
			with open(csvfile,'w') as f:
				f.writelines(rawdata)
		else:
			logger.info(f'skipping write {csvfile}')
		return True
	except Exception as e:
		logger.error(f'{type(e)} {e} in {csvfile}')
		return False


def test_polars_csv_read(logdir,maxfiles=100):
	dfx = pd.DataFrame()
	errors=0
	readfiles=0
	files_with_errors = []
	filecount = len([k for k in Path(logdir).glob('*.csv')][0:maxfiles])
	x = filecount//10
	for idx,k in enumerate(Path(logdir).glob('*.csv')):
		if readfiles>=maxfiles:
			logger.warning(f'MAX: {maxfiles} {readfiles=}')
			break
		if idx % x == 0:
			logger.info(f'[{idx}/{filecount}] rf={readfiles} e:{errors}')
		try:
			d=pl.read_csv(k, ignore_errors=True, try_parse_dates=True,truncate_ragged_lines=True, n_threads=4, use_pyarrow=True)
			#dfx=pd.concat([d.to_pandas(),dfx])
			#print(f'{errors} {len(dfx)} {readfiles}')
			readfiles+=1
		except Exception as e:
			print(f'{errors} {type(e)} {e}')
			errors+=1
			files_with_errors.append(k)
	if errors>0:
		print(f'plErrors: {files_with_errors}')
	return dfx

def test_pandas_csv_read(logdir,maxfiles=100):
	dfx = pd.DataFrame()
	errors=0
	readfiles=0
	files_with_errors = []
	filecount = len([k for k in Path(logdir).glob('*.csv')][0:maxfiles])
	x = filecount//10
	for idx,k in enumerate(Path(logdir).glob('*.csv')):
		if readfiles>=maxfiles:
			logger.warning(f'MAX: {maxfiles} {readfiles=}')
			break
		if idx % x == 0:
			logger.info(f'[{idx}/{filecount}] rf={readfiles} e:{errors}')
		try:
			d=pd.read_csv(k,engine='pyarrow', na_values=['-'], on_bad_lines='skip')
			# dfx=pd.concat([d,dfx])
			#print(f'{errors} {len(dfx)} {readfiles}')
			readfiles+=1
		except Exception as e:
			print(f'{errors} {type(e)} {e}')
			errors+=1
			files_with_errors.append(k)
	if errors>0:
		print(f'pdErrors: {files_with_errors}')
	return dfx


def check_and_fix_logs(logfiles, args):
	# iterate all log files (that have not been fixed) , check for bad chars, remove them
	# skip files that have been fixed already, by checking in the database
	# return a list of log files that have been fixed, TorqFile.fixed_flag should be 1
	new_log_files = []
	#dburl = 'sqlite:///torqfiskur.db'
	#engine = create_engine(dburl, echo=False, connect_args={'check_same_thread': False})
	#Session = sessionmaker(bind=engine)
	#session = Session()
	engine, session = get_engine_session(args)
	for log in logfiles:
		# check if log file has been fixed already, if not fix it
		# mark the log file as fixed in the database, TorqFile.fixed_flag = True
		pass

def drop_bad_columns(logfile:str, savebackup=True):
	# drop  columns with funny names from log file
	# saves to new csv file
	df = pd.read_csv(logfile, nrows=1)
	needbackup = False
	for c in df.columns:
		if len(c) == 0 or c[0].isnumeric():
			logger.warning(f'invalid/empty column {c} in {logfile} dropping...')
			df = df.drop(columns=c)
			needbackup = True
	if needbackup and savebackup:
		bakname = Path(f'{logfile}.bak')
		shutil.copy(logfile, bakname)
		logger.info(f'backed up {logfile} to {bakname}')
		df.to_csv(logfile, index=False)

def drop_empty_columns(logfile:str, savebackup=True):
	# drop empty columns from log file
	# saves to new csv file
	df = pl.read_csv(logfile, ignore_errors=True, try_parse_dates=True,truncate_ragged_lines=True).to_pandas()
	needbackup = False
	for c in df.columns:
		lchk = len([k for k in df[c] if k == '-'])
		logger.debug(f'checking {c} in {logfile} l:{lchk} len:{len(df[c])}')
		if lchk == len(df[c]):
			logger.warning(f'column {c} full of - values in {logfile} dropping...')
			df = df.drop(columns=c)
			needbackup = True
	if needbackup and savebackup:
		bakname = Path(f'{logfile}.bak')
		shutil.copy(logfile, bakname)
		logger.info(f'backed up {logfile} to {bakname}')
		df.to_csv(logfile, index=False)
	else:
		logger.info(f'no empty columns in {logfile}')

def get_cols(logpath:str, extglob:str="**/*.csv", debug=False):
	"""
	collect all columns from all csv files in the path
	params: logpath where to search, extglob glob pattern to use
	prep data base columns ....
	"""
	columns = {}
	stats = {}
	filestats = {}
	# '/home/kth/development/torq/torqueLogs.bakbak/').glob("**/trackLog-*.bak"
	for logfile in Path(logpath).glob(extglob):
		df = pd.read_csv(logfile, nrows=1)
		# newcolnames = ','.join([re.sub(r'\W', '', col) for col in df.columns]).encode('ascii', 'ignore').decode()
		newcolnames = get_sanatized_column_names(df.columns)
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

def run_fixer(args):
	logger.debug(f'searching {args.logpath} for csv files')
	csvfiles = [k for k in Path(args.logpath).glob('**/trackLog-*.csv') ]
	logger.debug(f'found {len(csvfiles)} csv files')
	for f in csvfiles:
		bakname = Path(os.path.join(args.bakpath, Path(f).name))
		if bakname.exists():
			logger.warning(f'backup file {bakname} exists, skipping')
			continue
		else:
			csvlines = open(f, 'r').readlines()
			try:
				fixedlines = get_fixed_lines(f, debug=args.debug)
			except Exception as e:
				logger.error(f'error {type(e)} {e} {f}')
				continue
			shutil.copy(f, bakname)
			logger.debug(f'{f} {len(csvlines)} got {len(fixedlines)}  ')
			with open(f, 'w') as f:
				f.writelines(fixedlines)

def new_columns_collector(logdir:str):
	"""
	collect column names from all log files, sanitize names
	returns dict with old names mapped to new names
	"""
	errors=0
	readfiles=0
	files_with_errors = []
	all_columns = []
	filecount = len([k for k in Path(logdir).glob('*.csv')])
	x = filecount//10
	for idx,k in enumerate(Path(logdir).glob('*.csv')):
		if idx % x == 0: # progress indicator
			logger.info(f'[{idx}/{filecount}] rf={readfiles} e:{errors} ac: {len(all_columns)}')
		try:
			# only read first line of csv file and select columns
			columns = pl.read_csv(k, ignore_errors=True, try_parse_dates=True,truncate_ragged_lines=True, n_rows=1).columns
			# newcolnames = ','.join([re.sub(r'\W', '', col) for col in columns]).encode('ascii', 'ignore').decode().lower().split(',')
			newcolnames = get_sanatized_column_names(columns)
			all_columns.extend([k for k in newcolnames.split(',') if k not in all_columns and k[0].isalpha()])
			readfiles+=1
		except Exception as e:
			print(f'[{idx}/{filecount}] {type(e)} {e} {errors} in {k}')
			errors+=1
			files_with_errors.append(k)
	if errors>0:
		print(f'plErrors: {files_with_errors}')
	# r = dict([k for k in zip(columns, newcolnames)])
	nclist = [k.strip() for k in newcolnames.split(',')]
	r = dict([k for k in zip(columns, nclist)])
	# foo = dict( zip(columns, newcolnames))
	return r, files_with_errors

def get_raw_columns(logfile:str):
	"""
	get the raw header from a csv logfile
	returns dict with logfilename and info
	"""
	# coldata = sorted(coldata, key=lambda x: x['colcount'])
	with open(logfile, 'r') as f:
		rawh = f.readline()
	return {'logfile': logfile, 'header': rawh, 'colcount': len(rawh.split(','))}


def get_files_with_errors(logdir:str):
	"""
	scan logdir for csv files that have problems
	returns dict with old names mapped to new names
	"""

	# count length of each column in raw csv
	# _=[print(f'k:{k} len:{len(k)} at : {idx}') for idx,k in enumerate(rawdata[1].split(','))]

	errors=0
	readfiles=0
	files_with_errors = []
	all_columns = []
	filecount = len([k for k in Path(logdir).glob('*.csv')])
	x = filecount//10
	for idx,k in enumerate(Path(logdir).glob('*.csv')):
		if idx % x == 0:
			logger.info(f'[{idx}/{filecount}] rf={readfiles} e:{errors} ac: {len(all_columns)}')
		try:
			test_read = pl.read_csv(k,  try_parse_dates=True, ignore_errors=True)
			readfiles+=1
		except Exception as e:
			print(f'[{idx}/{filecount}] {type(e)} {e} {errors} in {k}')
			errors+=1
			files_with_errors.append(k)
	if errors>0:
		logger.warning(f'found {files_with_errors} problem files')
	else:
		logger.info('no problem files found')
	# foo = dict( zip(columns, newcolnames))
	return files_with_errors


def split_file(logfile:str, session=None):
	"""
	split a log file where multiple lines of column headers are present
	param: logfile = full path and name of file
	"""
	with open(logfile, 'r') as f:
		rawdata = f.readlines()

	# find all lines with gps in them, skip first line
	split_list = [{'linenumber':idx, 'linedata': k} for idx,k in enumerate(rawdata) if 'gps' in k.lower()][1:]

	# grab timestamps of lines before and after split to determine if split is needed or not
	# if timestamps are close, no split is needed, combine file and rescan else split into multiple files
	if len(split_list) == 0:
		logger.warning(f'no split markers found in {logfile} should not happen!')
		return
	logger.info(f'found {len(split_list)} split markers in {logfile}')
	gpstime_diff = 0
	devicetime_diff = 0
	try:
		for idx,marker in enumerate(split_list):
			gpstime_before_split = convert_string_to_datetime(rawdata[marker.get('linenumber')-1].split(',')[0])
			gpstime_after_split = convert_string_to_datetime(rawdata[marker.get('linenumber')+1].split(',')[0])
			devicetime_before_split = convert_string_to_datetime(rawdata[marker.get('linenumber')-1].split(',')[1])
			devicetime_after_split = convert_string_to_datetime(rawdata[marker.get('linenumber')+1].split(',')[1])

			gpstime_diff += (gpstime_after_split - gpstime_before_split).seconds
			devicetime_diff += (devicetime_after_split - devicetime_before_split).seconds

		if gpstime_diff <= 900: # 900=15minutes, combine file parts into one
			logger.info(f'Remove extra header lines from {logfile} at line  {gpstime_diff=} {devicetime_diff=} ') # \n\tgpsbefore: {gpstime_before_split} gpsafter: {gpstime_after_split}\n\tdevtimebefore: {devicetime_before_split} devtimeafter: {devicetime_after_split}' )
			# {marker.get("linenumber")}
			# remove lines that start with GPS, keep first line, write to file (overwrite)
			skip_lines = [k.get('linenumber') for k in split_list]
			shutil.copy(logfile, f'{logfile}.bak')
			with open(logfile, 'w') as f:
				for idx, line in enumerate(rawdata):
					if idx not in skip_lines:
						f.write(line.strip()+'\n')
			logger.debug(f'wrote fixed {logfile}')
			# mark the file as fixed in the database, TorqFile.fixed_flag = 1 and TorqFile.error_flag = 0, read again
			if session:
				torqfile = session.query(TorqFile).filter(TorqFile.csvfile == logfile).one()
				torqfile.fixed_flag = 1
				torqfile.error_flag = 0
				session.commit()
				logger.debug(f'database updated for {logfile} torqfileid: {torqfile.fileid}')
		elif gpstime_diff >= 900: # more that five seconds, split file
			# newbufferlen = len(rawdata[marker['linenumber']:])
			logger.warning(f'Splitting {logfile} {gpstime_diff=} {devicetime_diff=} ') # \n\tgpsbefore: {gpstime_before_split} gpsafter: {gpstime_after_split}\n\tdevtimebefore: {devicetime_before_split} devtimeafter: {devicetime_after_split}' )
			marker = split_list[0]
			# generate new filename for split
			newdate = convert_string_to_datetime(rawdata[marker.get('linenumber')+1].split(',')[1])
			basename = 'trackLog-' + newdate.strftime('%Y-%b-%d_%H-%M')
			newlogfile = os.path.join(Path(logfile).parent,basename)
			start_line = [k.get('linenumber') for k in split_list][0]
			new_rawdata = rawdata[start_line:]
			with open(newlogfile, 'w') as f:
				for idx, line in enumerate(new_rawdata):
					f.write(line.strip()+'\n')
			logger.debug(f'wrote fixed {basename}')
			# mark the file as fixed in the database, TorqFile.fixed_flag = 1 and TorqFile.error_flag = 0, read again
			if session:
				torqfile = TorqFile(csvfile=newlogfile, csvhash=md5(open(newlogfile, 'rb').read()).hexdigest())
				torqfile.fixed_flag = 1
				torqfile.error_flag = 0
				session.commit()
				logger.debug(f'database updated for {basename} torqfileid: {torqfile.fileid}')
			# write lines from split marker to end of file to new file
			# mark the file as fixed in the database, TorqFile.fixed_flag = 1 and TorqFile.error_flag = 0, read again


			# split file into multiple parts, keep first part of file, write rest to new files
			# mark the file as fixed in the database, TorqFile.fixed_flag = 1 and TorqFile.error_flag = 1, read again

	except TypeError as e:
		logger.error(f'splitter failed {e} {logfile=}')


if __name__ == '__main__':
	pass
#!/usr/bin/python

# fixers in here

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
from sqlalchemy import create_engine
from sqlalchemy.exc import DataError, OperationalError, NoResultFound
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import MultipleResultsFound
from commonformats import fmt_20, fmt_24, fmt_26, fmt_28, fmt_30, fmt_34, fmt_36
from datamodels import TorqFile, database_init
from schemas import ncc, schema_datatypes
from utils import get_engine_session, get_fixed_lines, get_sanatized_column_names,MIN_FILESIZE, convert_string_to_datetime

def replace_headers(newfiles:list, args):
	"""
	newfiles a list of new files we need to process / send
	strip leading spaces off the column headers
	returns dict with two list of files, successfully processed files, and error files
	"""

	res = {
		'files_to_read': [],
		'errorfiles': [],
	}
	for f in newfiles:
		if fix_column_names(f, args):
			res['files_to_read'].append(f)
		else:
			res['errorfiles'].append(f)
	if len(res['errorfiles']) > 0:
		logger.warning(f"errors: {len(res['errorfiles'])} {res['errorfiles']}")
	else:
		logger.info(f'fixed {len(res["files_to_read"])} files')
	return res


def fix_column_names(csvfile:str, args):
	"""
	strip leading spaces from column names and saves the fil
	# todo skip files that have been fixed already, by checking in the database
	# todo maybe renmame columns here ?
	"""
	subchars = [', ',',Â','∞','Â°F','Â°','â°','â']
	try:
		with open(csvfile,'r') as f:
			rawdata = f.readlines()
		#for badchar in subchars:
		rawdata[0] = get_sanatized_column_names(rawdata[0]) #re.sub(badchar,',',rawdata[0])
		# rawdata[0] = re.sub(', ',',',rawdata[0])
		# rawdata[0] = re.sub('Â','',rawdata[0])
		# rawdata[0] = re.sub('∞','',rawdata[0])
		# rawdata[0] = re.sub('Â°F','F',rawdata[0])
		# rawdata[0] = re.sub('Â°','',rawdata[0])
		# rawdata[0] = re.sub('â°','',rawdata[0])
		# rawdata[0] = re.sub('â','',rawdata[0])
		if not args.skipwrites:
			logger.info(f'writing columns to {csvfile}')
			shutil.copy(csvfile, f'{csvfile}.colfixbak')
			with open(csvfile,'w') as f:
				f.writelines(rawdata)
		else:
			logger.info(f'skipping write {csvfile}')
		return True
	except Exception as e:
		logger.error(f'{type(e)} {e} in {csvfile}')
		return False


def test_polars_csv_read(logdir,maxfiles=100):
	dfx = pd.DataFrame()
	errors=0
	readfiles=0
	files_with_errors = []
	filecount = len([k for k in Path(logdir).glob('*.csv')][0:maxfiles])
	x = filecount//10
	for idx,k in enumerate(Path(logdir).glob('*.csv')):
		if readfiles>=maxfiles:
			logger.warning(f'MAX: {maxfiles} {readfiles=}')
			break
		if idx % x == 0:
			logger.info(f'[{idx}/{filecount}] rf={readfiles} e:{errors}')
		try:
			d=pl.read_csv(k, ignore_errors=True, try_parse_dates=True,truncate_ragged_lines=True, n_threads=4, use_pyarrow=True)
			#dfx=pd.concat([d.to_pandas(),dfx])
			#print(f'{errors} {len(dfx)} {readfiles}')
			readfiles+=1
		except Exception as e:
			print(f'{errors} {type(e)} {e}')
			errors+=1
			files_with_errors.append(k)
	if errors>0:
		print(f'plErrors: {files_with_errors}')
	return dfx

def test_pandas_csv_read(logdir,maxfiles=100):
	dfx = pd.DataFrame()
	errors=0
	readfiles=0
	files_with_errors = []
	filecount = len([k for k in Path(logdir).glob('*.csv')][0:maxfiles])
	x = filecount//10
	for idx,k in enumerate(Path(logdir).glob('*.csv')):
		if readfiles>=maxfiles:
			logger.warning(f'MAX: {maxfiles} {readfiles=}')
			break
		if idx % x == 0:
			logger.info(f'[{idx}/{filecount}] rf={readfiles} e:{errors}')
		try:
			d=pd.read_csv(k,engine='pyarrow', na_values=['-'], on_bad_lines='skip')
			# dfx=pd.concat([d,dfx])
			#print(f'{errors} {len(dfx)} {readfiles}')
			readfiles+=1
		except Exception as e:
			print(f'{errors} {type(e)} {e}')
			errors+=1
			files_with_errors.append(k)
	if errors>0:
		print(f'pdErrors: {files_with_errors}')
	return dfx


def check_and_fix_logs(logfiles, args):
	# iterate all log files (that have not been fixed) , check for bad chars, remove them
	# skip files that have been fixed already, by checking in the database
	# return a list of log files that have been fixed, TorqFile.fixed_flag should be 1
	new_log_files = []
	#dburl = 'sqlite:///torqfiskur.db'
	#engine = create_engine(dburl, echo=False, connect_args={'check_same_thread': False})
	#Session = sessionmaker(bind=engine)
	#session = Session()
	engine, session = get_engine_session(args)
	for log in logfiles:
		# check if log file has been fixed already, if not fix it
		# mark the log file as fixed in the database, TorqFile.fixed_flag = True
		pass

def drop_bad_columns(logfile:str, savebackup=True):
	# drop  columns with funny names from log file
	# saves to new csv file
	df = pd.read_csv(logfile, nrows=1)
	needbackup = False
	for c in df.columns:
		if len(c) == 0 or c[0].isnumeric():
			logger.warning(f'invalid/empty column {c} in {logfile} dropping...')
			df = df.drop(columns=c)
			needbackup = True
	if needbackup and savebackup:
		bakname = Path(f'{logfile}.bak')
		shutil.copy(logfile, bakname)
		logger.info(f'backed up {logfile} to {bakname}')
		df.to_csv(logfile, index=False)

def drop_empty_columns(logfile:str, savebackup=True):
	# drop empty columns from log file
	# saves to new csv file
	df = pl.read_csv(logfile, ignore_errors=True, try_parse_dates=True,truncate_ragged_lines=True).to_pandas()
	needbackup = False
	for c in df.columns:
		lchk = len([k for k in df[c] if k == '-'])
		logger.debug(f'checking {c} in {logfile} l:{lchk} len:{len(df[c])}')
		if lchk == len(df[c]):
			logger.warning(f'column {c} full of - values in {logfile} dropping...')
			df = df.drop(columns=c)
			needbackup = True
	if needbackup and savebackup:
		bakname = Path(f'{logfile}.bak')
		shutil.copy(logfile, bakname)
		logger.info(f'backed up {logfile} to {bakname}')
		df.to_csv(logfile, index=False)
	else:
		logger.info(f'no empty columns in {logfile}')

def get_cols(logpath:str, extglob:str="**/*.csv", debug=False):
	"""
	collect all columns from all csv files in the path
	params: logpath where to search, extglob glob pattern to use
	prep data base columns ....
	"""
	columns = {}
	stats = {}
	filestats = {}
	# '/home/kth/development/torq/torqueLogs.bakbak/').glob("**/trackLog-*.bak"
	for logfile in Path(logpath).glob(extglob):
		df = pd.read_csv(logfile, nrows=1)
		# newcolnames = ','.join([re.sub(r'\W', '', col) for col in df.columns]).encode('ascii', 'ignore').decode()
		newcolnames = get_sanatized_column_names(df.columns)
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

def run_fixer(args):
	logger.debug(f'searching {args.logpath} for csv files')
	csvfiles = [k for k in Path(args.logpath).glob('**/trackLog-*.csv') ]
	logger.debug(f'found {len(csvfiles)} csv files')
	for f in csvfiles:
		bakname = Path(os.path.join(args.bakpath, Path(f).name))
		if bakname.exists():
			logger.warning(f'backup file {bakname} exists, skipping')
			continue
		else:
			csvlines = open(f, 'r').readlines()
			try:
				fixedlines = get_fixed_lines(f, debug=args.debug)
			except Exception as e:
				logger.error(f'error {type(e)} {e} {f}')
				continue
			shutil.copy(f, bakname)
			logger.debug(f'{f} {len(csvlines)} got {len(fixedlines)}  ')
			with open(f, 'w') as f:
				f.writelines(fixedlines)

def new_columns_collector(logdir:str):
	"""
	collect column names from all log files, sanitize names
	returns dict with old names mapped to new names
	"""
	errors=0
	readfiles=0
	files_with_errors = []
	all_columns = []
	filecount = len([k for k in Path(logdir).glob('*.csv')])
	x = filecount//10
	for idx,k in enumerate(Path(logdir).glob('*.csv')):
		if idx % x == 0: # progress indicator
			logger.info(f'[{idx}/{filecount}] rf={readfiles} e:{errors} ac: {len(all_columns)}')
		try:
			# only read first line of csv file and select columns
			columns = pl.read_csv(k, ignore_errors=True, try_parse_dates=True,truncate_ragged_lines=True, n_rows=1).columns
			# newcolnames = ','.join([re.sub(r'\W', '', col) for col in columns]).encode('ascii', 'ignore').decode().lower().split(',')
			newcolnames = get_sanatized_column_names(columns)
			all_columns.extend([k for k in newcolnames.split(',') if k not in all_columns and k[0].isalpha()])
			readfiles+=1
		except Exception as e:
			print(f'[{idx}/{filecount}] {type(e)} {e} {errors} in {k}')
			errors+=1
			files_with_errors.append(k)
	if errors>0:
		print(f'plErrors: {files_with_errors}')
	# r = dict([k for k in zip(columns, newcolnames)])
	nclist = [k.strip() for k in newcolnames.split(',')]
	r = dict([k for k in zip(columns, nclist)])
	# foo = dict( zip(columns, newcolnames))
	return r, files_with_errors

def get_raw_columns(logfile:str):
	"""
	get the raw header from a csv logfile
	returns dict with logfilename and info
	"""
	# coldata = sorted(coldata, key=lambda x: x['colcount'])
	with open(logfile, 'r') as f:
		rawh = f.readline()
	return {'logfile': logfile, 'header': rawh, 'colcount': len(rawh.split(','))}


def get_files_with_errors(logdir:str):
	"""
	scan logdir for csv files that have problems
	returns dict with old names mapped to new names
	"""

	# count length of each column in raw csv
	# _=[print(f'k:{k} len:{len(k)} at : {idx}') for idx,k in enumerate(rawdata[1].split(','))]

	errors=0
	readfiles=0
	files_with_errors = []
	all_columns = []
	filecount = len([k for k in Path(logdir).glob('*.csv')])
	x = filecount//10
	for idx,k in enumerate(Path(logdir).glob('*.csv')):
		if idx % x == 0:
			logger.info(f'[{idx}/{filecount}] rf={readfiles} e:{errors} ac: {len(all_columns)}')
		try:
			test_read = pl.read_csv(k,  try_parse_dates=True, ignore_errors=True)
			readfiles+=1
		except Exception as e:
			print(f'[{idx}/{filecount}] {type(e)} {e} {errors} in {k}')
			errors+=1
			files_with_errors.append(k)
	if errors>0:
		logger.warning(f'found {files_with_errors} problem files')
	else:
		logger.info('no problem files found')
	# foo = dict( zip(columns, newcolnames))
	return files_with_errors


def split_file(logfile:str, session=None):
	"""
	split a log file where multiple lines of column headers are present
	param: logfile = full path and name of file
	"""
	with open(logfile, 'r') as f:
		rawdata = f.readlines()

	# find all lines with gps in them, skip first line
	split_list = [{'linenumber':idx, 'linedata': k} for idx,k in enumerate(rawdata) if 'gps' in k.lower()][1:]

	# grab timestamps of lines before and after split to determine if split is needed or not
	# if timestamps are close, no split is needed, combine file and rescan else split into multiple files
	if len(split_list) == 0:
		logger.warning(f'no split markers found in {logfile} should not happen!')
		return
	logger.info(f'found {len(split_list)} split markers in {logfile}')
	gpstime_diff = 0
	devicetime_diff = 0
	try:
		for idx,marker in enumerate(split_list):
			gpstime_before_split = convert_string_to_datetime(rawdata[marker.get('linenumber')-1].split(',')[0])
			gpstime_after_split = convert_string_to_datetime(rawdata[marker.get('linenumber')+1].split(',')[0])
			devicetime_before_split = convert_string_to_datetime(rawdata[marker.get('linenumber')-1].split(',')[1])
			devicetime_after_split = convert_string_to_datetime(rawdata[marker.get('linenumber')+1].split(',')[1])

			gpstime_diff += (gpstime_after_split - gpstime_before_split).seconds
			devicetime_diff += (devicetime_after_split - devicetime_before_split).seconds

		if gpstime_diff <= 900: # 900=15minutes, combine file parts into one
			logger.info(f'Remove extra header lines from {logfile} at line  {gpstime_diff=} {devicetime_diff=} ') # \n\tgpsbefore: {gpstime_before_split} gpsafter: {gpstime_after_split}\n\tdevtimebefore: {devicetime_before_split} devtimeafter: {devicetime_after_split}' )
			# {marker.get("linenumber")}
			# remove lines that start with GPS, keep first line, write to file (overwrite)
			skip_lines = [k.get('linenumber') for k in split_list]
			shutil.copy(logfile, f'{logfile}.bak')
			with open(logfile, 'w') as f:
				for idx, line in enumerate(rawdata):
					if idx not in skip_lines:
						f.write(line.strip()+'\n')
			logger.debug(f'wrote fixed {logfile}')
			# mark the file as fixed in the database, TorqFile.fixed_flag = 1 and TorqFile.error_flag = 0, read again
			if session:
				torqfile = session.query(TorqFile).filter(TorqFile.csvfile == logfile).one()
				torqfile.fixed_flag = 1
				torqfile.error_flag = 0
				session.commit()
				logger.debug(f'database updated for {logfile} torqfileid: {torqfile.fileid}')
		elif gpstime_diff >= 900: # more that five seconds, split file
			# newbufferlen = len(rawdata[marker['linenumber']:])
			logger.warning(f'Splitting {logfile} {gpstime_diff=} {devicetime_diff=} ') # \n\tgpsbefore: {gpstime_before_split} gpsafter: {gpstime_after_split}\n\tdevtimebefore: {devicetime_before_split} devtimeafter: {devicetime_after_split}' )
			marker = split_list[0]
			# generate new filename for split
			newdate = convert_string_to_datetime(rawdata[marker.get('linenumber')+1].split(',')[1])
			basename = 'trackLog-' + newdate.strftime('%Y-%b-%d_%H-%M')
			newlogfile = os.path.join(Path(logfile).parent,basename)
			start_line = [k.get('linenumber') for k in split_list][0]
			new_rawdata = rawdata[start_line:]
			with open(newlogfile, 'w') as f:
				for idx, line in enumerate(new_rawdata):
					f.write(line.strip()+'\n')
			logger.debug(f'wrote fixed {basename}')
			# mark the file as fixed in the database, TorqFile.fixed_flag = 1 and TorqFile.error_flag = 0, read again
			if session:
				torqfile = TorqFile(csvfile=newlogfile, csvhash=md5(open(newlogfile, 'rb').read()).hexdigest())
				torqfile.fixed_flag = 1
				torqfile.error_flag = 0
				session.commit()
				logger.debug(f'database updated for {basename} torqfileid: {torqfile.fileid}')
			# write lines from split marker to end of file to new file
			# mark the file as fixed in the database, TorqFile.fixed_flag = 1 and TorqFile.error_flag = 0, read again


			# split file into multiple parts, keep first part of file, write rest to new files
			# mark the file as fixed in the database, TorqFile.fixed_flag = 1 and TorqFile.error_flag = 1, read again

	except TypeError as e:
		logger.error(f'splitter failed {e} {logfile=}')


if __name__ == '__main__':
	pass
