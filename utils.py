# utils and db things here
import os
import sys
import re
from hashlib import md5
from pathlib import Path
from loguru import logger

from sqlalchemy import create_engine, MetaData, select, update
from sqlalchemy.exc import IntegrityError, OperationalError, ProgrammingError, DataError
from psycopg2.errors import DatatypeMismatch
from sqlalchemy.ext.declarative import declarative_base

from dateutil.parser._parser import ParserError
from datetime import datetime

from pandas import to_datetime, DataFrame, Series, concat
from datamodels import TorqTrip
logger.add('tool.log')

from sqlalchemy.exc import OperationalError

MIN_FILESIZE = 4096

from datamodels import TorqFile, TorqTrip, TorqLogEntry

def get_csv_files(searchpath:Path, recursive=True, dbmode=None):
	if not isinstance(searchpath, Path):
		searchpath = Path(searchpath)
	if not isinstance(searchpath, Path):
		logger.debug(f'[getcsv] err: searchpath {searchpath} is {type(searchpath)} need Path object')
		return []
	else: 
		# csvhash = md5(open(csv, 'rb').read()).hexdigest()
		torqcsvfiles = [({'csvfilename':k, 'size':os.stat(k).st_size, 'hash':md5(open(k, 'rb').read()).hexdigest(), 'dbmode':dbmode}) for k in searchpath.glob("**/trackLog.csv") if k.stat().st_size >= MIN_FILESIZE]
		for idx,tf in enumerate(torqcsvfiles):
			with open(tf['csvfilename'], 'r') as reader:
				data = reader.readlines()
			lines = [str(k.replace(',- ',',0')) for k in data[0:]]
			orgcol = data[0].split(',')
			newcolname = ','.join([re.sub(r'\W', '', col) for col in orgcol]).encode('ascii','ignore').decode()
			newcolname += '\n'
			lines[0] = newcolname
			nf = f"{tf['csvfilename']}.fix"
			# logger.debug(f"[f] {tf['csvfilename']} {nf} nclen:{len(newcolname)} og:{len(orgcol)} l:{len(lines)} l0:{len(lines[0])} dl0:{len(data[0])}")
			with open(nf, 'w') as writer:
				writer.writelines(lines)
				#d = DataFrame(columns=[newcolname], data=lines)
				#dflist.append(d)
				#d.to_csv(nf, quoting=False, index=False)
			newcolname=newcolname.strip()
			torqcsvfiles[idx] = {'csvfilename':tf['csvfilename'], 'csvfilefixed':Path(nf), 'buflen':len(lines),'size':os.stat(tf['csvfilename']).st_size, 'hash':md5(open(tf['csvfilename'], 'rb').read()).hexdigest(), 'dbmode':dbmode, 'newcolumns':newcolname}
		logger.info(f'[getcsv] t:{len(torqcsvfiles)}')
		return torqcsvfiles


def read_torq_profile(filename):
	p_filename = os.path.join(filename.parent, 'profile.properties')
	with open(p_filename, 'r') as f:
		pdata_ = f.readlines()
	if len(pdata_) == 8:
		pdata = [l.strip('\n') for l in pdata_ if not l.startswith('#')]
		try:
			pdata_date = str(pdata_[1][1:]).strip('\n')
			tripdate = to_datetime(pdata_date).to_pydatetime()
		except (OperationalError, Exception) as e:
			logger.error(f'[readsend] {e}')
			tripdate = None
		trip_profile = dict([k.split('=') for k in pdata])
		torq_trip = TorqTrip()
		torq_trip.fuelCost = float(trip_profile['fuelCost'])
		torq_trip.fuelUsed = float(trip_profile['fuelUsed'])
		torq_trip.distanceWhilstConnectedToOBD = float(trip_profile['distanceWhilstConnectedToOBD'])
		torq_trip.distance = float(trip_profile['distance'])
		torq_trip.time = float(trip_profile['time'])
		torq_trip.filename = p_filename
		torq_trip.tripdate = tripdate
		torq_trip.profile = trip_profile['profile']
		return torq_trip
	else:
		logger.warning(f'[p] {filename} len={len(pdata_)}')

def size_format(b):
    if b < 1000:
              return '%i' % b + 'B'
    elif 1000 <= b < 1000000:
        return '%.1f' % float(b/1000) + 'KB'
    elif 1000000 <= b < 1000000000:
        return '%.1f' % float(b/1000000) + 'MB'
    elif 1000000000 <= b < 1000000000000:
        return '%.1f' % float(b/1000000000) + 'GB'
    elif 1000000000000 <= b:
        return '%.1f' % float(b/1000000000000) + 'TB'


def database_init(engine):
	# close_all_sessions()
	Base = declarative_base()
	meta = MetaData(engine)
	t1 = datetime.now()
	#TorqEntry = type('TorqEntry', (Base,), torqentry_attr_dict)
	#t=TorqEntry()
	#temptables=[k.name for k in t.__mapper__.tables[0].columns]
	# tt2=[k.name for k in t.__mapper__.tables[0].columns]
	try:
		logger.debug(f'[dbinit] {(datetime.now() - t1).total_seconds()} dropping from {meta}')
		# Base.metadata.drop_all(bind=engine, checkfirst=True)
		# Base.metadata.drop_all(bind=engine, tables=[TorqLogEntry.__table__, TorqEntry.__table__, TorqFile.__table__, TorqTrip.__table__], checkfirst=True)
	except (OperationalError, ProgrammingError) as e:
		logger.error(f'dropall {e}')
	try:
		logger.debug(f'[dbinit] {(datetime.now() - t1).total_seconds()} create {meta}')
		# Base.metadata.create_all(bind=engine, checkfirst=False)
		# Base.metadata.create_all(bind=engine, tables=[TorqLogEntry.__table__, TorqEntry.__table__, TorqFile.__table__, TorqTrip.__table__], checkfirst=False)
	except OperationalError as e:
		logger.error(f'metacreateall {e}')
	logger.debug(f'[dbinit] {(datetime.now() - t1).total_seconds()} done')


def chunks(l, n):
	"""Yield n number of sequential chunks from l."""
	d, r = divmod(len(l), n)
	for i in range(n):
		si = (d + 1) * (i if i < r else r) + d * (0 if i < r else i - r)
		yield l[si:si + (d + 1 if i < r else d)]


def clean_files(filelist):
	newfilelist = {}
	dflist = []
	for idx, tf in enumerate(filelist):
		with open(tf['filename'], 'r') as reader:
			data = reader.readlines()
		# lines= [(k.replace('  ','')) for idx,k in enumerate(data[1:]) if k.count('-') <=9 and len(k)>10]
		# lines0 = [(k.replace('  ','')).encode('ascii', 'ignore') for idx,k in enumerate(data[1:]) if k.count('-') <=11]
		#lines0 = [str((k.replace('   ','\n').encode('ascii', 'ignore'))) for k in data]]
		lines = [str(k.replace(',- ',',0')) for k in data[0:]]
		# lines = [k.encode('ascii', 'ignore') for k in lines_0]
		orgcol = data[0].split(',')
		newcolname = ','.join([re.sub(r'\W', '', col) for col in orgcol]).encode('ascii','ignore').decode()
		newcolname += '\n'
		#newcolname=[str(k).encode('ascii','ignore').decode() for k in newcolname.split(',')]
		#newcolname = f'id,{newcolname}'
		if newcolname.count(',') > 70 or 'CO' in newcolname or '2022' in orgcol: # or len(newcolname) == 1301:
			logger.warning(f'[22] nc {newcolname} oc:{orgcol}')
		# lines.insert(0, newcolname)
		lines[0] = newcolname
		nf = f"{tf['filename']}.fix"
		logger.debug(f"[f] {tf['filename']} {nf} nclen:{len(newcolname)} og:{len(orgcol)} l:{len(lines)} l0:{len(lines[0])} dl0:{len(data[0])}")
		with open(nf, 'w') as writer:
			writer.writelines(lines)
			#d = DataFrame(columns=[newcolname], data=lines)
			#dflist.append(d)
			#d.to_csv(nf, quoting=False, index=False)
		newfilelist[idx] = ({'filename':nf, 'buflen':len(lines), 'hash':md5(open(nf, 'rb').read()).hexdigest(), 'dbmode':'mysql'})
	return newfilelist
	# torqcsvfiles = [({'filename':k, 'size':os.stat(k).st_size, 'hash':md5(open(k, 'rb').read()).hexdigest(), 'dbmode':dbmode}) for k in searchpath.glob("**/trackLog.csv") if k.stat().st_size >= MIN_FILESIZE]


def get_col_names(buffer):
	name_list = []
	for col in buffer: # :.columns:
		newname_ = col
		newname_ = re.sub(r'\W', '', col)
		newname = newname_.encode('ascii', 'ignore').decode()
		name_list.append({'name':newname, 'oldname':col})
	return name_list

def fixdates(buffer):
	t0 = datetime.now()
	try:
		GPSTime = to_datetime(buffer['GPSTime'], errors='ignore', infer_datetime_format=False)
		buffer['GPSTime'] = GPSTime
	except (ParserError, KeyError) as e:
		logger.warning(f'[fgpstime] err {e}')# {buffer.__dict__}')
		#newbuff.GPSTime = to_datetime(buffer['GPS Time'], format="%a %b %m %H:%M:%S %Z %Y", errors='ignore', infer_datetime_format=False)
		# raw_data['Mycol'] =  pd.to_datetime(raw_data['Mycol'], format='%d%b%Y:%H:%M:%S.%f')
		# buffer['GPSTime'] = to_datetime(datetime.now(), errors='raise', infer_datetime_format=True)
	try:
		DeviceTime = to_datetime(buffer['DeviceTime'], errors='ignore', infer_datetime_format=False)
		buffer['DeviceTime'] = DeviceTime
	except (ParserError, KeyError) as e:
		logger.warning(f'[fDeviceTime] err {e}')
		#newbuff.DeviceTime = to_datetime(buffer['Device Time'], format="%a %b %m %H:%M:%S %Z %Y", errors='ignore', infer_datetime_format=False)
		# newbuff['DeviceTime'] = to_datetime(datetime.now(), errors='ignore', infer_datetime_format=True)
	logger.info(f'[fixb] done time: {(datetime.now() - t0).seconds} b:{len(buffer)} ')
#	torqentry = TorqEntry()
#	for k in torqentry.__mapper__.tables[0].columns:
#		torqentry.__dict__[k] = newbuff[k]
	# [k for k in torqentry.__mapper__.tables[0].columns]
	return buffer



def read_torq_trip(filename):
	p_filename = os.path.join(filename.parent, 'profile.properties')
	with open(p_filename, 'r') as f:
		pdata_ = f.readlines()
	if len(pdata_) == 8:
		pdata = [l.strip('\n') for l in pdata_ if not l.startswith('#')]
		try:
			pdata_date = str(pdata_[1][1:]).strip('\n')
			tripdate = to_datetime(pdata_date).to_pydatetime()
		except (OperationalError, Exception) as e:
			logger.error(f'[readsend] {e}')
			tripdate = None
		trip_profile = dict([k.split('=') for k in pdata])
		torq_trip = TorqTrip()
		torq_trip.fuelCost = float(trip_profile['fuelCost'])
		torq_trip.fuelUsed = float(trip_profile['fuelUsed'])
		torq_trip.distanceWhilstConnectedToOBD = float(trip_profile['distanceWhilstConnectedToOBD'])
		torq_trip.distance = float(trip_profile['distance'])
		torq_trip.time = float(trip_profile['time'])
		torq_trip.filename = p_filename
		torq_trip.tripdate = tripdate
		torq_trip.profile = trip_profile['profile']
		return torq_trip
	else:
		logger.warning(f'[p] {filename} len={len(pdata_)}')


def fixbuffer(buffer=None):
	t0 = datetime.now()
	old_len = len(buffer)
	
	# newbuff = DataFrame()
	# newbuff = TorqEntry()
	# bcol_list = get_col_list(buffer)
	ser_list = []
	# s0 = Series
	# b1=DataFrame(buffer,columns=[b['name'] for b in bcol_list], index=[k for k in range(len(buffer))])
	# b1=DataFrame()
	# newbuff = DataFrame()
	frames = []
	for col in buffer: # :.columns:
		newname_ = col
		newname_ = re.sub(r'\W', '', col)
		newname = newname_.encode('ascii', 'ignore').decode()
		#s1=Series(buffer[col].values)
		s2=DataFrame(Series(buffer[col].values), index=buffer[col].index, columns=[newname])
		frames.append(s2)

		#newbuff.add(Series(buffer[col],name=newname))
	newbuff = concat(frames)
	return newbuff
		#buffer[col].rename(newname, inplace=True)
		#buffer[col].index.name = 'id'		
#		b1 = concat([b1, buffer[col]])

	# for b in col_list:
	# 	s1 = Series(b['newcol'])
	# 	# s1.set_axis(s1, inplace=True)
	# 	ser_list.append(s1)
	#ser_list = [Series(k['newcol']) for k in col_list]
	#s2 = DataFrame(ser_list)
	
	# newbuff = DataFrame(columns=[k['name'] for k in col_list], data=[k['newcol'] for k in col_list])
	# newbuff = {} # DataFrame()
	# for col in col_list:
	# 	newser = Series(col['newcol'])
	# 	newbuff[col['name']] = newser
	# 	#newcol = buffer[col['oldname']]		
	# 	#newbuff[newser] = buffer[col].index
	# return newbuff
	
	# for col in buffer.columns:
	# 	newname_ = col
	# 	newname_ = re.sub(r'\W', '', col)
	# 	newname = newname_.encode('ascii', 'ignore').decode()
#		for b in BADVALS:
#			buffer[col].replace(to_replace=b, value=0, regex=True, inplace=True)
#			newcol = buffer[col]
#			newcol.index.name = 'id'
			

def get_col_list(buffer):
	col_list = []
	for col in buffer: # :.columns:
		newname_ = col
		newname_ = re.sub(r'\W', '', col)
		newname = newname_.encode('ascii', 'ignore').decode()
		buffer[col].name = newname
		# buffer[col].index.name = 'id'		
		#newcol.name = newname
		#col_list.append({'name':newname, 'newcol': newcol, 'oldname':col})
		#logger.info(f'[gcl] newcol:{newcol.name} col:{col} newname:{newname} c:{len(col_list)} b:{len(buffer)}')
	return buffer
	#newbuff = DataFrame(columns=[c for c in col_list])
	#newbuff.set_axis(col_list, axis=1, inplace=True)
# Incorrect datetime value: 'Fri Dec 10 18:31:58 GMT+01:00 2021' for column `torqfiskur`.`torqlogs`.`GPSTime` at row 1") tripid:2 profid:



	# torqlogentry = TorqLogEntry()
	# torqlogentry.tripid = tfile.tripid
	# torqlogentry.torqfileid = tfile.torqfileid
	# session.add(torqlogentry)
	# session.commit()

	#sbuffer = Series(data=[k for k in range(len(buffer))], dtype='int')
	# buffer['id'] = DataFrame(data=[k for k in range(len(buffer))], dtype='int')

	#sbuffer = Series(data=[tfile.tripid for k in range(len(buffer))], dtype='int')
	#buffer['tripid'] = DataFrame(data=[sbuffer], dtype='int')

	#sbuffer = Series(data=[tfile.torqfileid for k in range(len(buffer))], dtype='int')
	#buffer['torqfileid'] = DataFrame(data=[sbuffer], dtype='int')
	# logger.info(f'[tt] {csvfile} b:{len(buffer)} tb:{len(sbuffer)}')

	# logger.debug(f'[rs] sending c:{csvfile} tlid:{torqlogentry.torqlog_entry} tfid:{tfile.tripid}')
