#!/usr/bin/python
import argparse
import os
import re
import shutil
import sys
from datetime import datetime
from hashlib import md5
from pathlib import Path

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
from schemas import ncc
from utils import get_engine_session, get_fixed_lines, get_sanatized_column_names

pd.set_option('future.no_silent_downcasting', True)

class Polarsreaderror(Exception):
	pass

# tool to rename and import tripLogs from older versions of the app
# get tripdate from profile.properties file and rename the log file to the new format
# tripdate should match with foldername of each trip, named as unix timestamp (13 digits)
# timestamp is the start time of the trip, first line of the log file
# example :
# original path /torq/tripLogs/1708245165793
# new filenames are in the format: trackLog-2021-Dec-01_23-40-45.csv
# datetime.fromtimestamp(1708245165793/1000).strftime("%Y-%b-%d_%H-%M-%S")

def Convert(lst):
	# convert list to dict
    res_dct = map(lambda i: (lst[i], lst[i+1]), range(len(lst)-1)[::2])
    return dict(res_dct)

def read_profile(profile_fn:str):
	# read profile.properties file, to extract some data
	tripdate = None
	try:
		with open(profile_fn, 'r') as f:
			data = f.readlines()
		if len(data) == 8 or len(data) == 6:
			#pdata_date = str(data[1][1:]).strip('\n')
			#tripdate = datetime.strptime(pdata_date ,'%a %b %d %H:%M:%S %Z%z %Y')
			if len(data[1]) == 30:
				tripdate = datetime.strptime((str(data[1][1:]).strip('\n')), fmt_30)
			elif len(data[1]) == 36:
				#Tue May 17 17:55:43 GMT+02:00 2022
				tripdate = datetime.strptime((str(data[1][1:]).strip('\n')), fmt_36)
			else:
				logger.warning(f'unknown date format {data[1]}')
				tripdate = data[1]
		else:
			logger.warning(f'profile.properties file {profile_fn} has {len(data)} lines {data}')
	except Exception as e:
		logger.error(f'unhandled {type(e)} {e}')
	finally:
		return tripdate

def transfer_older_logs(args):
	# transfer old tripLogs to new format
	# todo read more info from profile.properties file
	#

	old_dirs = [k for k in Path(args.oldlogpath).glob('*') if k.is_dir() and len(str(k.name))==13]
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
			new_log_fn = Path(os.path.join(args.logpath, f'trackLog-{trip_date}.csv'))
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

def new_polars_csv_reader(logfile):
	"""
	read csv file
	param: logfile = full path and name of file
	param: schema to use
	param: newcolumns = dict with sanatized column names, generated with new_columns_collector
	returns pandas dataframe, with sanatized column names
	raises Polarsreaderror if something goes wrong
	"""
	so = {
    " Latitude": pl.Float64,
    " Longitude": pl.Float64,
    "Latitude": pl.Float64,
    "Longitude": pl.Float64,
	}
	try: # schema_overrides=so,
		data = pl.read_csv(logfile,  ignore_errors=True, try_parse_dates=True,truncate_ragged_lines=True, n_threads=4, use_pyarrow=True) # .to_pandas()
		#, schema=schema)
	except pl.exceptions.NoDataError as e:
		msg = f'NoDataError {type(e)} {e} {logfile}'
		logger.error(msg)
		raise Polarsreaderror(msg)
	df = data.to_pandas()
	try:
		ncren = {k:ncc[k] for k in df.columns if k in ncc} # get columns to rename
		# df = df.rename(ncren) # rename them
		df = df.rename(columns=ncren)
	except KeyError as e:
		msg = f'keyerror {type(e)} {e} {logfile}\ndatacols: {df.columns} \n'
		logger.warning(msg)
		raise Polarsreaderror(msg)
	for col in df.columns:
		# df[col] = df[col].replace('.',',')
		df[col] = df[col].replace('-',0)
		df[col] = df[col].replace('Â','')
		df[col] = df[col].replace('â','')
		df[col] = df[col].replace('°','')
		df[col] = df[col].replace('₂','')
		df[col] = df[col].replace('∞','')
		df[col] = df[col].replace('£','')
		# df[col] = rcol
	# data = df.fill_null(0).fill_nan(0)
	# df = data.to_pandas()
	# df1 = df.rename(columns=ncc)
	dfout = df.fillna(0)
	return dfout

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

def send_filename_to_db(args, filename):
	"""
	send this filename to database, catch all exceptions in here
	return True if ok, else False
	"""
	engine, session = get_engine_session(args)
	with session.no_autoflush:
		try:
			csvhash = md5(open(filename, 'rb').read()).hexdigest()
			t = TorqFile(csvfile=filename,csvhash=csvhash)
			session.add(t)
			session.commit()
			session.close()
			return True
		except Exception as e:
			session.close()
			logger.error(f'unhandled {type(e)} {e} from {filename}')
			return False

def send_csv_data_to_db(args, data:pd.DataFrame, f:str):
	"""
	send this csvdata to database, catch all exceptions in here
	return True if ok, else False
	"""
	engine, session = get_engine_session(args)
	fileid = session.query(TorqFile.fileid).filter(TorqFile.csvfile==f).one()[0]
	# fileid_series = pl.Series("fileid", [fileid for k in range(len(data))])
	data.insert(column='fileid', loc=0, value=fileid)
	logger.debug(f'sending {len(data)} from {f} id:{fileid} to database {args.dbmode}')
	try:
		# data.to_sql('torqlogs', con=session.get_bind(), if_exists='append', index=False)
		data.to_sql('torqlogs', con=engine, if_exists='append', index=False)
	except (DataError, OperationalError) as e:
		logger.warning(f'{type(e)} {e.args[0]} {f=}')
		return False
	except Exception as e:
		logger.error(f'unhandled {type(e)} {e} ')
		return False
	session.close()
	return True

def get_files_to_send(session:sessionmaker, args):
	"""
	scan logpath for csv files, check if they have already been sent to data base
	returns list of files not in the database, filenames as str, NOT Path!
	"""
	files_to_send = []
	try:
		unread_dbfiles = session.query(TorqFile).filter(TorqFile.read_flag==0).all()
		read_dbfiles = session.query(TorqFile).filter(TorqFile.read_flag==1).all()
		csvfiles = [str(k) for k in Path(args.logpath).glob('*.csv')]
		files_to_send = set(csvfiles)-set([k.csvfile for k in read_dbfiles])
	except Exception as e:
		logger.error(f'unhandled {type(e)} {e} from {args.logpath} {args.dbmode}')
	finally:
		session.close()
		if len(files_to_send) > 0:
			logger.info(f'found {len(files_to_send)} files_to_send,  unread_dbfiles:{len(unread_dbfiles)} files to process and send...')
		else:
			logger.warning(f'no files found in {args.logpath} ! dbfiles:{len(unread_dbfiles)} ...')

		files_to_send = sorted(files_to_send) # sort by filename (date)
		return files_to_send

def date_column_fixer(data:pd.DataFrame=None, datecol:str=None, f:str=None):
	"""
	fixes timedatestamp format in dataframe
	param: data Dataframe, datecol name of date column, f filename (for ref)
	"""
	testdate = data[datecol][len(data)//2]
	fmt_selector = len(testdate) # use value in middle to check.....
	fixed_datecol = data[datecol]# copy pd.DataFrame()
	# chk = [k for k in fixed_datecol if isinstance(k,str) and '-' in k]
	# chk_g = [k for k in fixed_datecol if isinstance(k,str) and 'G' in k]
	#if len(chk)>0:
		# logger.warning(f'CHECK {datecol=} {len(chk)}/{len(fixed_datecol)} chk_g:{len(chk_g)} things in {f} ')
	#	fixed_datecol = fixed_datecol.replace('-',0) # copy pd.DataFrame()
	try:
		match fmt_selector:
			case 20:
				fixed_datecol = pd.DataFrame({datecol:[datetime.strptime(k,fmt_20).astimezone(pytz.timezone('UTC')) for k in fixed_datecol if isinstance(k, str) ]})
			case 24:
				fixed_datecol = pd.DataFrame({datecol:[datetime.strptime(k,fmt_24).astimezone(pytz.timezone('UTC')) for k in fixed_datecol if isinstance(k, str) ]})
			case 26:
				fixed_datecol = pd.DataFrame({datecol:[datetime.strptime(k,fmt_26).astimezone(pytz.timezone('UTC')) for k in fixed_datecol if isinstance(k, str) ]})
			case 28:
				fixed_datecol = pd.DataFrame({datecol:[datetime.strptime(k,fmt_28).astimezone(pytz.timezone('UTC')) for k in fixed_datecol if isinstance(k, str) ]})
			case 30:
				fixed_datecol = pd.DataFrame({datecol:[datetime.strptime(k,fmt_30).astimezone(pytz.timezone('UTC')) for k in fixed_datecol if isinstance(k, str) ]})
			case 34:
				fixed_datecol = pd.DataFrame({datecol:[datetime.strptime(k,fmt_34).astimezone(pytz.timezone('UTC')) for k in fixed_datecol if isinstance(k, str) ]})
			case 36:
				fixed_datecol = pd.DataFrame({datecol:[datetime.strptime(k,fmt_36).astimezone(pytz.timezone('UTC')) for k in fixed_datecol if isinstance(k, str) ]})
			case _:
				pass # logger.warning(f'could not match format for fmt_selector {fmt_selector} for {datecol} {f=}.\n sample:first= {df0[datecol][0]} middle= {df0[datecol][len(data)//2]} last= {df0[datecol][len(df0)-1]}\n')
	except (ValueError,TypeError,KeyError) as e:
		logger.error(f'datefix {type(e)} {e} data: {type(data)} {data} dc: {type(datecol)} datecol: {datecol} fmt: {fmt_selector} \n sample:first= {data[datecol][0]} middle= {data[datecol][len(data)//2]} \n') # last= {df0[datecol][len(df0)-1]}
		return data[datecol]
	# chk = [k for k in fixed_datecol[datecol] if '-' in k]
	# fixed_datecol[datecol] = fixed_datecol[datecol].replace('-',0)
	#chk = [k for k in fixed_datecol[datecol] if isinstance(k,str)]
	#chk3 = [k for k in fixed_datecol[datecol] if isinstance(k,str)]
	#if len(chk)>0:
	#	logger.warning(f'CHECK2 {len(chk)} things in {f} {datecol=}')
	return fixed_datecol


def fix_bad_values(data:pd.DataFrame, f:str):
	"""
	search and replace bad values from databuffer
	param: data dataframe, f filename (for ref)
	returns fixed data if possible, else orginal
	"""
	# fixed_data = pd.DataFrame()
	# 'â\x88\x9e' found in tracklog-2021-jul-05_17-53-16.csv
	# badhex
	# C3 A2 C2 88 C2 9E
	# C3 82 C2 B0
	# C3 A2 C2 82 C2 82
	# C3 82 C2 B0
	# Â°
	try:
		needs_fix = [k for k in data.columns if '-' in data[k].values]
		fixcount = 0
		for fix in needs_fix:
			# data[fix] = data[fix].replace('-',0)
			data[fix] = data[fix].replace('340282346638528860000000000000000000000',0)
			data[fix] = data[fix].replace('-3402823618710077500000000000000000000',0)
			data[fix] = data[fix].replace('612508207723425200000000000000000000000',0)
			data[fix] = data[fix].replace('â\x88\x9e',0)
			fixcount += 1
		if fixcount>0:
			logger.debug(f'fixed {fixcount} things in {f}')
		return data
	except Exception as e:
		logger.error(f'error in fixer: {type(e)} {e} for {f}')
		raise e

def split_check(csvfile:str):
	"""
	check if file is damaged and needs splitting...
	todo write splitter and refresh file list .....
	"""
	with open(csvfile, 'r') as f:
		data = f.readlines()
	gpscount = 0
	for line in data[1:]:
		if 'gps' in line.lower():
			gpscount += 1
	if gpscount > 0:
		# logger.warning(f'splits: {gpscount=} in {csvfile} ')
		return True
	return False

def data_fixer(data:pd.DataFrame, f):

	# check if file needs splitting
	# fix dates
	newdatecol = pd.DataFrame()
	# fixed_data = pd.DataFrame()
	date_columns = ['gpstime','devicetime']
	for col in date_columns:
		newdatecol = date_column_fixer(data=data, datecol=col, f=f)
	# for col in data.columns:
	# 	if 'timestamp' in col.lower():
	# 		# skip timestamp columns
	# 		logger.warning(f'skipping {col} in {f}')
	# 		continue
	# 	if 'gps time' in col.lower() or 'device time' in col.lower() or 'gpstime' in col.lower() or 'devicetime' in col.lower():
	# 		try:
	# 			newdatecol = date_column_fixer(data=data, datecol=col, f=f)
	# 		except Exception as e:
	# 			logger.error(f'datefixer failed for {f} {col=} {type(e)} {e}')
	# 	else:
	# 		logger.warning(f'skipping {col} in {f}')
	# 		continue

	# fix bad values
		if not newdatecol.empty:
			data[col] = newdatecol
		else:
			logger.warning(f'empty column {col}')
			# logger.info(f'fixed {col} in {f}')
	#fixed_data = fix_bad_values(data,f)
	return data # fixed_data  if not fixed_data.empty else data

def fix_column_names(csvfile:str):
	"""
	strip leading spaces from column names and saves the fil
	# todo maybe renmame columns here ?
	"""
	subchars = [', ',',Â','∞','Â°F','Â°','â°','â']
	try:
		with open(csvfile,'r') as f:
			rawdata = f.readlines()
		for badchar in subchars:
			rawdata[0] = get_sanatized_column_names(rawdata[0]) #re.sub(badchar,',',rawdata[0])
		# rawdata[0] = re.sub(', ',',',rawdata[0])
		# rawdata[0] = re.sub('Â','',rawdata[0])
		# rawdata[0] = re.sub('∞','',rawdata[0])
		# rawdata[0] = re.sub('Â°F','F',rawdata[0])
		# rawdata[0] = re.sub('Â°','',rawdata[0])
		# rawdata[0] = re.sub('â°','',rawdata[0])
		# rawdata[0] = re.sub('â','',rawdata[0])
		with open(csvfile,'w') as f:
			f.writelines(rawdata)
		return True
	except Exception as e:
		logger.error(f'{type(e)} {e} in {csvfile}')
		return False

def replace_headers(newfiles:list):
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
		if fix_column_names(f):
			res['files_to_read'].append(f)
		else:
			res['errorfiles'].append(f)
	if len(res['errorfiles']) > 0:
		logger.warning(f"errors: {len(res['errorfiles'])} {res['errorfiles']}")
	else:
		logger.info(f'fixed {len(res["files_to_read"])} files')
	return res

def db_set_file_flag(session, filename=None, flag=None, flagval=None):
	"""
	set flag on file in database
	"""
	csvhash = md5(open(filename, 'rb').read()).hexdigest()
	try:
		torqfile = session.query(TorqFile).filter(TorqFile.csvhash == csvhash).one()
	except MultipleResultsFound as e:
		logger.error(f'{e} {filename} {csvhash} multiple entries in db, aborting...')
		return None
	except NoResultFound as e:
		logger.warning(f'{e} {filename} not found in db while trying to set {flag}, creating entry...')
		torqfile = TorqFile(csvfile=filename, csvhash=csvhash)
		session.add(torqfile)
		torqfile.error_flag = 6
	match flag:
		case 'readok':
			torqfile.read_flag = 1
		case 'ok': # when data has been sent to db, all ok!
			torqfile.error_flag = 0
			torqfile.send_flag = 1
			torqfile.read_flag = 1
		case 'split':
			torqfile.error_flag = 2
			torqfile.read_flag = 0
			torqfile.send_flag = 0
		case 'headfixerr':
			torqfile.error_flag = 3
			torqfile.read_flag = 0
			torqfile.send_flag = 0
		case 'fixerror':
			torqfile.error_flag = 4
			torqfile.read_flag = 0
			torqfile.send_flag = 0
		case 'senderror':
			torqfile.error_flag = 5
			torqfile.read_flag = 0
			torqfile.send_flag = 0
		case 'polarreaderror':
			torqfile.error_flag = 6
			torqfile.read_flag = 0
			torqfile.send_flag = 0
		case _:
			logger.warning(f'unknown flag {flag} for {filename} {csvhash}')
	session.commit()

def convert_string_to_datetime(s:str):
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
				datetimeobject = datetime.strptime(s,fmt_20).astimezone(pytz.timezone('UTC'))
			case 24:
				datetimeobject = datetime.strptime(s,fmt_24).astimezone(pytz.timezone('UTC'))
			case 26:
				datetimeobject = datetime.strptime(s,fmt_26).astimezone(pytz.timezone('UTC'))
			case 28:
				datetimeobject = datetime.strptime(s,fmt_28).astimezone(pytz.timezone('UTC'))
			case 30:
				datetimeobject = datetime.strptime(s,fmt_30).astimezone(pytz.timezone('UTC'))
			case 34:
				datetimeobject = datetime.strptime(s,fmt_34).astimezone(pytz.timezone('UTC'))
			case 36:
				datetimeobject = datetime.strptime(s,fmt_36).astimezone(pytz.timezone('UTC'))
			case _:
				pass # logger.warning(f'could not match format for fmt_selector {fmt_selector} for {datecol} {f=}.\n sample:first= {df0[datecol][0]} middle= {df0[datecol][len(data)//2]} last= {df0[datecol][len(df0)-1]}\n')
	except (ValueError,TypeError,KeyError) as e:
		logger.error(f'dateconverter {type(e)} {e} {s=}')
	finally:
		return datetimeobject


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

		if gpstime_diff <= 5: # five seconds, combine file parts into one
			logger.info(f'Remove extra header lines from {logfile} at line  {gpstime_diff=} {devicetime_diff=} ') # \n\tgpsbefore: {gpstime_before_split} gpsafter: {gpstime_after_split}\n\tdevtimebefore: {devicetime_before_split} devtimeafter: {devicetime_after_split}' )
			# {marker.get("linenumber")}
			# remove lines that start with GPS, keep first line, write to file (overwrite)
			skip_lines = [k.get('linenumber') for k in split_list]
			with open(logfile, 'w') as f:
				for idx, line in enumerate(rawdata):
					if idx not in skip_lines:
						f.write(line)
			logger.debug(f'wrote fixed {logfile}')
			# mark the file as fixed in the database, TorqFile.fixed_flag = 1 and TorqFile.error_flag = 0, read again
			if session:
				torqfile = session.query(TorqFile).filter(TorqFile.csvfile == logfile).one()
				torqfile.fixed_flag = 1
				torqfile.error_flag = 0
				session.commit()
				logger.debug(f'database updated for {logfile} torqfileid: {torqfile.fileid}')
		elif gpstime_diff >= 5: # more that five seconds, split file
			# newbufferlen = len(rawdata[marker['linenumber']:])
			logger.warning(f'Splitting {logfile} {gpstime_diff=} {devicetime_diff=} ') # \n\tgpsbefore: {gpstime_before_split} gpsafter: {gpstime_after_split}\n\tdevtimebefore: {devicetime_before_split} devtimeafter: {devicetime_after_split}' )
			# split file into multiple parts, keep first part of file, write rest to new files
			# mark the file as fixed in the database, TorqFile.fixed_flag = 1 and TorqFile.error_flag = 1, read again

	except TypeError as e:
		logger.error(f'splitter failed {e} {logfile=}')

def cli_main(args):
	if args.scanpath:
		# first collect sanatized column headers
		# ncc, errorfiles = new_columns_collector(logdir=args.logpath)
		# check if any of the files in args.logpath have been read, skip these
		# todo getfiles to read
		engine, session = get_engine_session(args)
		broken_files = [] # maybe add directly to db ?
		try:
			database_init(engine)
		except AssertionError as e:
			logger.error(f'[maindbinit] {e}')
			sys.exit(-1)
		with session.no_autoflush:
			newfiles = get_files_to_send(session, args=args)
		# todo fix colum names, some files have colum names with a leading space (eg ''GPS Time, Device Time, Longitude, Latitude,GPS Speed(km/h), Horizontal Dilution of Precision, Altitude(m), Bearing,')
		# maybe replace this before read_csv ?
		fixed_newfiles = replace_headers(newfiles)
		broken_files.extend(fixed_newfiles['errorfiles'])
		if len(fixed_newfiles['errorfiles'])>0:
			logger.warning(f'errorfiles: {fixed_newfiles["errorfiles"]} total broken_files: {len(broken_files)}')
			for errfile in fixed_newfiles['errorfiles']:
				db_set_file_flag(session, filename=errfile, flag='headfixerr')
		for idx,f in enumerate(fixed_newfiles['files_to_read']):
			logger.debug(f'[{idx}/{len(fixed_newfiles["files_to_read"])}] reading {f}')
			try:
				data = new_polars_csv_reader(logfile=f)
			except Polarsreaderror as e:
				logger.error(f'polarsreaderror {type(e)} {e} for {f}')
				broken_files.append(f)
				db_set_file_flag(session, filename=f, flag='polarreaderror')
				continue
			# if successful, make TorqFile entry in database
			if send_filename_to_db(args, f):
				db_set_file_flag(session, filename=f, flag='readok')
				if split_check(f):
					logger.warning(f'{f} needs splitting...')
					broken_files.append(f)
					db_set_file_flag(session, filename=f, flag='split')
					continue
				# ok to send csvdata
				try:
					fixed_data = data_fixer(data, f)
				except Exception as e:
					logger.error(f'error in datafixer {e} for {f}')
					broken_files.append(f)
					db_set_file_flag(session, filename=f, flag='fixerror')
					continue
				try:
					if send_csv_data_to_db(args, fixed_data, f):
						# todo maybe update torqfile flags in database
						db_set_file_flag(session, filename=f, flag='ok') # pass # logger.debug(f'Sent data from {f} to database')
					else:
						logger.warning(f'Could not send data from {f} to db...')
						db_set_file_flag(session, filename=f, flag='senderror')
						broken_files.append(f)
				except Exception as e:
					logger.error(f'unhandled {type(e)} {e} from send_csv_data_to_db {f}')
					broken_files.append(f)
		if len(broken_files)>0:
			print(f'{len(broken_files)} {broken_files}')
			sys.exit(1)
		sys.exit(0)
	if args.testnewreader:
		maxfiles = 100
		t0 = datetime.now()
		df_pd = test_polars_csv_read(logdir=args.logpath, maxfiles=maxfiles)
		t_pd = (datetime.now()-t0)

		t0 = datetime.now()
		df_pl = test_pandas_csv_read(logdir=args.logpath, maxfiles=maxfiles)
		t_pl = (datetime.now()-t0)
		print(f'time: pl {t_pl} pd {t_pd} dfx: pd= {len(df_pd)} pl= {len(df_pl)} finished')
		sys.exit(0)
	if args.fixer:
		# fixer mode
		# read all log files, fix bad chars, remove them
		# update database, mark the log file as fixed
		# TorqFile.fixed_flag = True
		# check_and_fix_logs(logfiles)
		run_fixer(args)
		sys.exit(0)
	if args.getcols:
		# get columns from all log files in the path
		stats, columns = get_cols(args.logpath, debug=args.debug)
		# columns = sorted(columns, key=lambda x: columns[x]['count'], reverse=True)
		columns = sorted(columns, key=lambda x: ( columns[x]['count'], columns[x]), reverse=True)
		for c in columns:
			if 'date' in c or 'time' in c:
				lineout = f"{c} = Column('{c}', DateTime)"
			else:
				lineout = f"{c} = Column('{c}', DOUBLE)" # Column('longitude', DOUBLE)
			print(lineout)
		# print(f'{columns=}')
		sys.exit(0)
	if args.transfer:
		# oldlogpath root of the old tripLogs files, containing subfolder, each name as unix timestamp of the trip
		# each sub folder contains a log file and profile.properties file
		# step one, transfer older logs to new location with new filenames
		new_old_logs = transfer_older_logs(args)
		logger.debug(f'transfered {len(new_old_logs)} old logs')
		# step two, read each log file, remove bad chars
		fixed = check_and_fix_logs(new_old_logs, args)
		print(f'{fixed=}')
		sys.exit(0)



def main():
	parser = argparse.ArgumentParser(description="converter ")

	parser.add_argument("--logpath", nargs="?", default=".", help="logpath", action="store")
	parser.add_argument("--oldlogpath", nargs="?", default=".", help="oldlogpath", action="store")
	parser.add_argument("--bakpath", nargs="?", default="/home/kth/development/torq/backups2", help="where to put backups", action="store")
	parser.add_argument('--getcols', default=False, help="prep cols", action="store_true", dest='getcols')
	parser.add_argument('--transfer', default=False, help="transfer old logs, set oldlogpath to location of old triplogs", action="store_true", dest='transfer')
	parser.add_argument('--fixer', default=False, help="run fixer, set --bakpath", action="store_true", dest='fixer')
	parser.add_argument('--testnewreader', default=False, help="run testnewreader", action="store_true", dest='testnewreader')
	parser.add_argument('--scanpath', default=False, help="run scanpath", action="store_true", dest='scanpath')
	parser.add_argument("--dbmode", default="sqlite", help="sqlmode mysql/postgresql/sqlite/mariadb", action="store")
	parser.add_argument('--dburl', default='sqlite:///torqfiskur.db', help='database url', action='store')
	parser.add_argument('-d', '--debug', default=False, help="debugmode", action="store_true", dest='debug')

	args = parser.parse_args()
	cli_main(args)

if __name__ == '__main__':
	main()