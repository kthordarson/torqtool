#!/usr/bin/python
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
import sqlalchemy
import pymysql
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DataError, OperationalError, NoResultFound
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import MultipleResultsFound
from psycopg2.errors import UndefinedColumn
import sqlite3
from commonformats import fmt_19, fmt_20, fmt_24, fmt_26, fmt_28, fmt_30, fmt_34, fmt_36
from datamodels import TorqFile, database_init
from schemas import ncc, schema_datatypes
from utils import get_engine_session, get_fixed_lines, get_sanatized_column_names,MIN_FILESIZE
from fixers import split_file, test_pandas_csv_read, test_polars_csv_read, run_fixer, get_cols, check_and_fix_logs, fix_column_names, replace_headers

pd.set_option('future.no_silent_downcasting', True)
# x = latitude y = longitude !

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

# move small logs
# for f in $(find /home/kth/development/torq/torqueLogs/ -type f ); do linecount=$(cat $f | wc -l); if [ $linecount -lt 10 ]; then echo "file $f lc=$linecount";fi;done;

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
		# old_timestamp = datetime.fromtimestamp(int(od.name)/1000).strftime("%Y-%b-%d_%H-%M-%S")
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
	logger.info(f'transfered {len(transfered_logs)} of {len(old_dirs)} old tripLogs to {args.logpath}')
	return transfered_logs


def read_csv_file(logfile, args):
	"""
	read csv file
	param: logfile = full path and name of file
	param: schema to use
	param: newcolumns = dict with sanatized column names, generated with new_columns_collector
	returns pandas dataframe, with sanatized column names
	raises Polarsreaderror if something goes wrong
	"""
	# todo handle missing gpstime, if not present, copy from devicetime
	try: # schema_overrides=so, # schema_overrides=schema_datatypes,
		data = pl.read_csv(logfile,  ignore_errors=True, try_parse_dates=True,truncate_ragged_lines=True, n_threads=4, use_pyarrow=True) # .to_pandas()
		#, schema=schema)
	except TypeError as e:
		logger.error(f'typeerror {type(e)} {e} {logfile}')
		raise e
	except pl.exceptions.NoDataError as e:
		msg = f'NoDataError {type(e)} {e} {logfile}'
		logger.error(msg)
		raise Polarsreaderror(msg)
	#filtered_data = data.drop_nulls() # drop columns with all null values
	df = data.to_pandas()

	# do the fixing here ˆž 0ˆž â  Ã¢ÂÂ â  â° âÂ°F â
	asbadval = 'Ã¢Â\x88Â\x9e'
	bc = 'â\x88\x9e'

	bcsymbs = [k for k in df.columns if bc in df[k].values]
	for col in bcsymbs:
		df[col] = df[col].replace(bc,0)

	aasymbs = [k for k in df.columns if asbadval in df[k].values]
	for col in aasymbs:
		df[col] = df[col].replace(asbadval,0)

	asymbs = [k for k in df.columns if 'â' in df[k].values]
	for col in asymbs:
		df[col] = df[col].replace('â',0)

	zsymbs = [k for k in df.columns if 'ž' in df[k].values]
	for col in zsymbs:
		df[col] = df[col].replace('ž',0)

	zsymbs = [k for k in df.columns if '0ˆž' in df[k].values]
	for col in zsymbs:
		df[col] = df[col].replace('0ˆž',0)

	infsymbs = [k for k in df.columns if '∞' in df[k].values]
	for col in infsymbs:
		df[col] = df[col].replace('∞',0)

	dashfix = [k for k in df.columns if '-' in df[k].values]
	for col in dashfix:
		df[col] = df[col].replace('-',0)

	nanfix = [k for k in df.columns if 'NaN' in df[k].values]
	for col in nanfix:
		df[col] = df[col].replace('NaN',0)

	# bigvalfix = [k for k in df.columns if '340282346638528860000000000000000000000' in df[k].values or '612508207723425200000000000000000000000' in df[k].values]
	#for col in bigvalfix:
	#	df[col] = df[col].replace('340282346638528860000000000000000000000',0)
	#	df[col] = df[col].replace('612508207723425200000000000000000000000',0)

	# bigval check v2
	longcheck = None
	# todo ....
	# find strings in dataframe
	# should be converted to float or number
	string_check = []
	for col in df.columns:
		# must be longer than 13, must be str and not contain 'time'
		# chk = [print(f'{col} {k} {type(k)}') for k in df[col]  if isinstance(k,str) and len(k) > 13 and 'time' not in col]
		longcheck = None
		if 'time' not in col: # skip gpstime and devicetime, check other columns
			string_check.extend([{'col':col, 'value': k, 'idx': idx} for idx,k in enumerate(df[col]) if isinstance(k,str)])
			longcheck = [k for k in df[col] if isinstance(k,str) and len(k) > 18]
			if longcheck:
				# remove the long bad values
				df[col] = df[col].replace(longcheck,0)
	if longcheck:
		logger.warning(f'replaced {len(longcheck)} long values in {logfile}')# column: {col} lc:  {longcheck[0:1]}')

	columns_with_wrong_dtype = set([k['col'] for k in string_check])
	if len(columns_with_wrong_dtype) > 0 and args.extradebug:
		logger.warning(f'found {len(columns_with_wrong_dtype)} columns with {len(string_check)} string values in {logfile}')# columns: {columns_with_wrong_dtype=} ')

	# for col in df.columns:
	#	if 'time' not in col:
	#		string_check = [print(f'col:{col} value: {k} {type(k)}') for k in df[col] if not isinstance(k,str)]

	# bignumbers:
	# 340282346638528860000000000000000000000
	# 612508207723425200000000000000000000000

	#subchars = [',Â','∞','Â°F','Â°','â°','â', '-', 'NaN',] # ', ',
	#need_to_fix = [k for k in df.columns if df[k].values.any() in subchars]
	#need_to_fix = [k[1] for k in df[col].items() if '-' in k for col in df.columns]
	#for col in need_to_fix:
	#	df[col] = df[col].replace(subchars,0)
	#	logger.debug(f'fixes: {len(need_to_fix)} f:{logfile} {col}')
	#if len(need_to_fix) > 0:
		#logger.debug(f'fixes: {len(need_to_fix)} in {logfile}')
	# drop columns with all null values
	#df = df_.dropna(axis='columns', how='all')

	# df = fix_bad_values(df,logfile) # fix bad values, broken maybe...
	#pldrops = len(data.columns)-len(filtered_data.columns)
	#pddrops = len(df_.columns)-len(df.columns)


	#if pldrops != 0 or pddrops != 0:
	#	logger.warning(f'filtered pl: {pldrops} pd:{pddrops} columns in {logfile}')

	try:
		ncren = {k:ncc[k] for k in df.columns if k in ncc} # get columns to rename
		# df = df.rename(ncren) # rename them
		df = df.rename(columns=ncren)
	except KeyError as e:
		msg = f'keyerror {type(e)} {e} {logfile}\ndatacols: {df.columns} \n'
		logger.warning(msg)
		raise Polarsreaderror(msg)
	# dfout = df.fillna(0)
	return df

def colreplacer(df):
	# todo for checking try :
	# test = [float(k) for k in data['enginecoolanttemperaturef'].values ] # raises exception if not float
	# test = [float(k) for k in data[columntocheck].values ] # raises exception if not float

	for col in df.columns:
		# df[col] = df[col].replace('.',',')
		df[col] = df[col].replace('-',0)
		df[col] = df[col].replace('Â','')
		df[col] = df[col].replace('â','')
		df[col] = df[col].replace('°','')
		df[col] = df[col].replace('₂','')
		df[col] = df[col].replace('∞','')
		df[col] = df[col].replace('£','')
		df[col] = df[col].replace('\n','')
		df[col] = df[col].replace('612508207723425200000000000000000000000',0)
		df[col] = df[col].replace('340282346638528860000000000000000000000',0)
		df[col] = df[col].replace('-3402823618710077500000000000000000000',0)
		df[col] = df[col].replace('6.125082077234252e+38',0)
		df[col] = df[col].replace('3.4028234663852886e+38',0)
		df[col] = df[col].replace('-5.481e-05',0)
		df[col] = df[col].replace('â\x88\x9e',0)
		# â\x88\x9e
		#-5.481e-05
		#6.125082077234252e+38
		#3.4028234663852886e+38
		# 6.125082077234252e+38
		# 612508207723425200000000000000000000000
		# df[col] = rcol
	# data = df.fill_null(0).fill_nan(0)
	# df = data.to_pandas()
	# df1 = df.rename(columns=ncc)



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

def send_csv_data_to_db(args:argparse.Namespace, data:pd.DataFrame, csvfilename:str, insertid:bool=True):
	"""
	send this csvdata to database, catch all exceptions in here
	return True if ok, else False
	"""
	engine, session = get_engine_session(args)
	csvhash = md5(open(csvfilename, 'rb').read()).hexdigest()
	fileid = session.query(TorqFile.fileid).filter(TorqFile.csvhash==csvhash).one()[0]
	#fileid = session.query(TorqFile.fileid).filter(TorqFile.csvfile==csvfilename).one()[0]
	# fileid_series = pl.Series("fileid", [fileid for k in range(len(data))])

	if insertid:
		data.insert(column='fileid', loc=0, value=fileid)
	fn = Path(csvfilename).name
	datacols = [k for k in data.columns] # get column names
	# todropcols = [k for k in datacols if k not in schema_datatypes]
	todropcols_ = list(set(datacols)-set(schema_datatypes))
	todropcols = [k for k in todropcols_ if k not in ['gpstime','fileid','devicetime']]
	if len(todropcols) > 0:
		org_col_len = len(data.columns)
		data = data.drop(columns=todropcols)
		if args.showdrops:
			logger.warning(f'Dropped {len(todropcols)} columns {org_col_len}->{len(data.columns)} in {csvfilename}\n datacols:{datacols}\n dropcolumns {todropcols}')
	try:
		# data.to_sql('torqlogs', con=session.get_bind(), if_exists='append', index=False)
		data.to_sql('torqlogs', con=engine, if_exists='append', index=False)
	except (DataError) as e:
		logger.warning(f'{type(e)} {e.args[0]} {csvfilename=}')
		return 0
	except (sqlalchemy.exc.OperationalError, OperationalError,sqlite3.OperationalError) as e:
		# todo sent error_flag=7 for unknown column
		# <class 'sqlalchemy.exc.OperationalError'> (pymysql.err.OperationalError) (1054, "Unknown column '1000kphtimes' in 'field list'") f='/home/kth/development/torq/torqueLogs/trackLog-2021-Jun-05_09-52-19.csv
		# pymysql.err.OperationalError: (1054, "Unknown column 'egrerror' in 'field list'")
		errmsg = e.args[0]
		retrysentrows = 0
		colname = None
		logger.error(f'{e.args[0]} {csvfilename=}')
		if 'sqlite3.OperationalError' in errmsg and 'no column named' in errmsg:
			colname = errmsg.split('no column named')[1].strip()
		elif 'pymysql.err.OperationalError' in errmsg and 'Unknown column' in errmsg:
			colname = re.findall(r"'(.*?)'", errmsg)[0]
		elif 'sqlalchemy.exc.OperationalError' in errmsg and 'Unknown column' in errmsg:
			# find column name
			colname = re.findall(r"'(.*?)'", errmsg)[0]
			# todo set error_flag=7 for unknown column
			#return 0
		elif '1040' in errmsg:
			logger.error(f'1040  {type(e)} {e.args[0]} {csvfilename=}')
			return 0
		if colname:
			session.close()
			logger.warning(f'dropping unknown column {colname} in {csvfilename}')
			df0 = data.drop(columns=[colname])
			retrysentrows = send_csv_data_to_db(args=args, data=df0, csvfilename=csvfilename, insertid=False)
			if retrysentrows > 0:
				return retrysentrows
			else:
				logger.warning(f'retrysendfailed {retrysentrows=} col: {colname} in {csvfilename}')
				return retrysentrows
		return 0
	except Exception as e:
		logger.error(f'unhandled {type(e)} {e} ')
		return 0
	session.close()
	if args.debug:
		logger.debug(f'Sent {len(data)} rows from {fn} id:{fileid}  to database {args.dbmode}')
	return len(data)

def get_files_to_send(session:sessionmaker, args):
	"""
	scan logpath for csv files, check if they have already been sent to data base
	returns list of files not in the database, filenames as str, NOT Path!
	"""
	files_to_send = []
	all_db_files = []
	unread_dbfiles = []
	csvfiles = [str(k) for k in Path(args.logpath).glob('*.csv') if k.stat().st_size > MIN_FILESIZE]
	smallcsvfiles = [str(k) for k in Path(args.logpath).glob('*.csv') if k.stat().st_size < MIN_FILESIZE]
	try:
		all_db_files = session.query(TorqFile).count()
		unread_dbfiles = session.query(TorqFile).filter(TorqFile.read_flag==0).filter(TorqFile.error_flag==0).all()
		read_dbfiles = session.query(TorqFile).filter(TorqFile.read_flag==1).all()
		error_dbfiles = session.query(TorqFile).filter(TorqFile.error_flag!=0).all()
		files_to_send = set(csvfiles)-set([k.csvfile for k in read_dbfiles])
	except UndefinedColumn as e:
		logger.warning(f'UndefinedColumn {type(e)} {e} from {args.logpath} {args.dbmode}')
	except Exception as e:
		logger.error(f'unhandled {type(e)} {e} from {args.logpath} {args.dbmode}')
	finally:
		session.close()
		if len(files_to_send) > 0:
			logger.info(f'found {len(files_to_send)} files_to_send, skipping {len(smallcsvfiles)} files under MIN_FILESIZE - unread_dbfiles:{len(unread_dbfiles)} error_dbfiles: {len(error_dbfiles)} all_db_files:{all_db_files}')
		else:
			logger.warning(f'no valid files found in {args.logpath} ! dbfiles: all= {len(all_db_files)} / unread= {len(unread_dbfiles)} csvfiles:{len(csvfiles)} ... Exit!')
			sys.exit(-1)

		files_to_send = sorted(files_to_send) # sort by filename (date)
		if args.samplemode:
			# select small number of random logs
			files_to_send = [random.choice(files_to_send) for k in range(random.randint(10,30))]

		return files_to_send

def date_column_fixer(data:pd.DataFrame=None, datecol:str=None, f:str=None):
	"""
	fixes timedatestamp format in dataframe
	param: data Dataframe, datecol name of date column, f filename (for ref)
	"""
	testdate = data[datecol][len(data)//2]
	if testdate == 0:
		errmsg = f'invalid testdate {testdate} {datecol=} datalen: {len(data)} f: {f}'
		logger.error(errmsg)
		raise TypeError(errmsg)
	try:
		fmt_selector = len(testdate) # use value in middle to check.....
	except TypeError as e:
		logger.error(f'datefix {type(e)} {e}  {type(datecol)} datecol: {datecol} {testdate=}')
		raise e
	fixed_datecol = data[datecol]# copy pd.DataFrame()
	# chk = [k for k in fixed_datecol if isinstance(k,str) and '-' in k]
	# chk_g = [k for k in fixed_datecol if isinstance(k,str) and 'G' in k]
	#if len(chk)>0:
		# logger.warning(f'CHECK {datecol=} {len(chk)}/{len(fixed_datecol)} chk_g:{len(chk_g)} things in {f} ')
	#	fixed_datecol = fixed_datecol.replace('-',0) # copy pd.DataFrame()
	try:
		match fmt_selector:
			case 19:
				fixed_datecol = pd.DataFrame({datecol:[datetime.strptime(k,fmt_19).astimezone(pytz.timezone('UTC')) for k in fixed_datecol if isinstance(k, str) ]})
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
		#needs_fix = [k for k in data.columns if '-' in data[k].values]
		for c in data:
			data[c] = data[c].replace('-',0)
			data[c] = data[c].replace('∞',0)
			data[c] = data[c].replace('NaN',0)
			data[c] = data[c].replace('6.125082077234252e+38',0)
			#6.125082077234252e+38
		# fixcount = 0
		# for fix in needs_fix:

		# 	data[fix] = data[fix].replace('340282346638528860000000000000000000000',0)
		# 	data[fix] = data[fix].replace('-3402823618710077500000000000000000000',0)
		# 	data[fix] = data[fix].replace('612508207723425200000000000000000000000',0)
		# 	data[fix] = data[fix].replace('â\x88\x9e',0)
		# 	fixcount += 1
		# if fixcount>0:
		# 	logger.debug(f'fixed {fixcount} things in {f}')
		return data
	except Exception as e:
		logger.error(f'error in fixer: {type(e)} {e} for {f}')
		raise e

def split_check(csvfile:str):
	"""
	check if file is damaged and needs splitting...
	todo write splitter and refresh file list .....
	"""
	try:
		with open(csvfile, 'r') as f:
			data = f.readlines()
	except FileNotFoundError as e:
		logger.error(f'{e} {csvfile} ...')
		raise e
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
	droprows = [idx for idx,k in enumerate(data.gpstime) if k=='-']
	if len(droprows) > 0:
		data = data.drop(droprows)
		logger.warning(f'dropping invalid gpstimerows {droprows} from {f}')
	for col in date_columns:
		try:
			newdatecol = date_column_fixer(data=data, datecol=col, f=f)
		except TypeError as e:
			logger.error(f'datafixer {type(e)} {e} {f} {col=}')
			raise e
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
	return data # fixed_data  if not fixed_data.empty else data



def db_set_file_flag(session, filename=None, flag=None, flagval=None, sent_rows=None, readtime=None, sendtime=None):
	"""
	set flag on file in database
	"""
	csvhash = md5(open(filename, 'rb').read()).hexdigest()
	try:
		torqfile = session.query(TorqFile).filter(TorqFile.csvhash == csvhash).one()
	except MultipleResultsFound as e:
		# todo more checking here....
		logger.error(f'{e} {filename} {csvhash} multiple entries in db, {flag=} {flagval=} aborting...')
		torqfiles = session.query(TorqFile).filter(TorqFile.csvhash == csvhash).all()
		for t in torqfiles:
			t.error_flag = 1
		session.commit()
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
			torqfile.sent_rows = sent_rows
			torqfile.readtime = readtime
			torqfile.sendtime = sendtime
			# get trip start and end times... # datetime.fromisoformat(
			try:
				datemin = session.execute(text(f'select gpstime from torqlogs where fileid={torqfile.fileid} order by gpstime limit 1 ')).all()[0][0]
				if not datemin:
					datemin = session.execute(text(f'select devicetime from torqlogs where fileid={torqfile.fileid} order by devicetime limit 1 ')).all()[0][0]
					logger.warning(f'no gpstime found for {filename} using devicetime {datemin=}')
				if isinstance(datemin, str):
					datemin = datetime.fromisoformat(datemin)
				datemax = session.execute(text(f'select gpstime from torqlogs where fileid={torqfile.fileid} order by gpstime desc limit 1 ')).all()[0][0]
				if not datemax:
					datemax = session.execute(text(f'select devicetime from torqlogs where fileid={torqfile.fileid} order by devicetime desc limit 1 ')).all()[0][0]
					logger.warning(f'no gpstime found for {filename} using devicetime {datemax=}')
				if isinstance(datemax, str):
					datemax = datetime.fromisoformat(datemax)
				torqfile.trip_start = datemin
				torqfile.trip_end = datemax
				torqfile.trip_duration = (datemax - datemin).total_seconds()
			except TypeError as e:
				logger.warning(f'{e} while calculating trip_duration for {filename} ') # {datemin=} {datemax=}
				torqfile.trip_duration = 0

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
			logger.error(f'[maindbinit] {e} exit')
			sys.exit(-1)
		if args.dbmode == 'sqlite':
			session.execute(text('PRAGMA journal_mode=WAL;'))
			session.execute(text('pragma synchronous = normal;'))
			session.execute(text('pragma temp_store = memory;'))
			session.execute(text('pragma mmap_size = 30000000000;'))

		with session.no_autoflush:
			newfiles = get_files_to_send(session, args=args)
		# todo fix colum names, some files have colum names with a leading space (eg ''GPS Time, Device Time, Longitude, Latitude,GPS Speed(km/h), Horizontal Dilution of Precision, Altitude(m), Bearing,')
		# maybe replace this before read_csv ?
		if args.db_limit:
			print(args)
			newfiles = newfiles[:int(args.db_limit)]
		fixed_newfiles = replace_headers(newfiles, args)
		broken_files.extend(fixed_newfiles['errorfiles'])
		if len(fixed_newfiles['errorfiles'])>0:
			logger.warning(f'errorfiles: {fixed_newfiles["errorfiles"]} total broken_files: {len(broken_files)}')
			for errfile in fixed_newfiles['errorfiles']:
				db_set_file_flag(session, filename=errfile, flag='headfixerr')
		for idx,f in enumerate(fixed_newfiles['files_to_read']):
			spchk = None
			readstart = datetime.now()
			logger.debug(f'[{idx}/{len(fixed_newfiles["files_to_read"])}] reading {Path(f).name}')
			try:
				spchk = split_check(f)
			except FileNotFoundError as e:
				logger.error(f'{e} {f}')
				continue
			if spchk:
				if not args.repairsplit:
					logger.warning(f'{f} needs splitting...')
					broken_files.append(f)
					db_set_file_flag(session, filename=f, flag='split')
					continue
				elif args.repairsplit:
					logger.warning(f'{f} needs splitting...')
					try:
						fsplit = split_file(f, session)
					except Exception as e:
						logger.error(f'unhandled error while splitting {f} {e} {type(e)}')
						continue
					if fsplit:
						f = fsplit
					else:
						logger.warning(f'splitting failed for {f}')
						broken_files.append(f)
						db_set_file_flag(session, filename=f, flag='split')
						continue
			try:
				data = read_csv_file(logfile=f, args=args)
			except Polarsreaderror as e:
				logger.error(f'polarsreaderror {type(e)} {e} for {f}')
				broken_files.append(f)
				db_set_file_flag(session, filename=f, flag='polarreaderror')
				continue
			# if successful, make TorqFile entry in database
			if send_filename_to_db(args, f):
				db_set_file_flag(session, filename=f, flag='readok')
				# ok to send csvdata
				try:
					fixed_data = data_fixer(data, f)
				except Exception as e:
					logger.error(f'error in datafixer {type(e)} {e} for {f}')
					broken_files.append(f)
					db_set_file_flag(session, filename=f, flag='fixerror')
					continue
				try:
					readtime = (datetime.now()-readstart).total_seconds()
					sendstart = datetime.now()
					sent_rows = send_csv_data_to_db(args=args, data=fixed_data, csvfilename=f)
					if sent_rows>0:
						# todo maybe update torqfile flags in database
						sendtime = (datetime.now()-sendstart).total_seconds()
						db_set_file_flag(session, filename=f, flag='ok', sent_rows=sent_rows, readtime=readtime, sendtime=sendtime) # pass # logger.debug(f'Sent data from {f} to database')
					else:
						logger.warning(f'Sent rows = {sent_rows} from {f} to db...')
						db_set_file_flag(session, filename=f, flag='senderror')
						broken_files.append(f)
				except TypeError as e:
					logger.error(f'{type(e)} {e} from send_csv_data_to_db {f}')
					broken_files.append(f)
				except Exception as e:
					logger.error(f'unhandled {type(e)} {e} from send_csv_data_to_db {f}')
					broken_files.append(f)
		if len(broken_files)>0:
			print(f'{len(broken_files)} {broken_files}')
			# todo select split files from db and fix them, for now use the list

			if args.repairsplit:
				files_to_split = session.execute(text('select csvfile from torqfiles where error_flag=2')).all()
				logger.info(f'found {len(files_to_split)} files to split')
				for idx,f in enumerate(files_to_split):
					logger.debug(f'[{idx}/{len(files_to_split)}] splitting {f}')
					try:
						split_file(f, session)
					except Exception as e:
						logger.error(f'unhandled error while splitting {f} {e} {type(e)}')
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

def get_parser(appname):
	parser = argparse.ArgumentParser(description=appname)
	parser.add_argument('--scanpath', default=False, help="run scanpath", action="store_true", dest='scanpath')

	parser.add_argument("--logpath", nargs="?", default=".", help="logpath", action="store")
	parser.add_argument("--oldlogpath", nargs="?", default=".", help="oldlogpath", action="store")
	parser.add_argument("--bakpath", nargs="?", default="/home/kth/development/torq/backups2", help="where to put backups", action="store")

	parser.add_argument('--getcols', default=False, help="prep cols", action="store_true", dest='getcols')
	parser.add_argument('--transfer', default=False, help="transfer old logs, set oldlogpath to location of old triplogs", action="store_true", dest='transfer')
	parser.add_argument('--fixer', default=False, help="run fixer, set --bakpath", action="store_true", dest='fixer')
	parser.add_argument('--repairsplit', default=False, help="enable splitting of strange log files", action="store_true", dest='repairsplit')
	parser.add_argument('--skipwrites', default=False, help="skipwrites", action="store_true", dest='skipwrites')
	parser.add_argument('--showdrops', default=False, help="show dropped columns", action="store_true", dest='showdrops')

	parser.add_argument('--testnewreader', default=False, help="run testnewreader", action="store_true", dest='testnewreader')
	parser.add_argument('--samplemode', default=False, help="use samplemode, select small random number of logs-for debugging", action="store_true", dest='samplemode')
	parser.add_argument("--dbmode", default="sqlite", help="sqlmode mysql/psql/sqlite/mariadb", action="store")
	parser.add_argument('--dbfile', default='torqfiskur.db', help='database file', action='store')
	parser.add_argument("--dbname", default="torq", help="dbname", action="store")
	parser.add_argument("--dbhost", default="localhost", help="dbname", action="store")
	parser.add_argument("--dbuser", default="torq", help="dbname", action="store")
	parser.add_argument("--dbpass", default="qrot", help="dbname", action="store")
	parser.add_argument('-d', '--debug', default=False, help="debugmode", action="store_true", dest='debug')
	parser.add_argument('--db_limit', default=False, help="db_limit", action="store", dest='db_limit')
	parser.add_argument('--extradebug', default=False, help="extradebug", action="store_true", dest='extradebug')

	return parser

def get_args(appname):
	parser = get_parser(appname)


	args = parser.parse_args()
	return args

def main():
	args = get_args(appname='converter')
	cli_main(args)

if __name__ == '__main__':
	main()