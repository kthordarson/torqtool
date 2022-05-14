# utils and db things here
import os
import sys
import re
from pathlib import Path
from loguru import logger

from datamodels import TorqProfile
logger.add('tool.log')
# import inspect
# from re import T, search, sub
from pandas import to_datetime

from sqlalchemy.exc import OperationalError

MIN_FILESIZE = 4096

def get_csv_files(searchpath:Path, recursive=True):
	# todo fix globbing....
	# csvlist = searchpath.glob('tracklog*.csv')
	if not isinstance(searchpath, Path):
		searchpath = Path(searchpath)
	if not isinstance(searchpath, Path):
		logger.debug(f'[getcsv] err: searchpath {searchpath} is {type(searchpath)} need Path object')
		return []
	else:
		torqcsvfiles = [k for k in searchpath.glob("**/trackLog.csv") if k.stat().st_size >= MIN_FILESIZE]
		return torqcsvfiles


def read_torq_profile(filename, tripid):
	p_filename = os.path.join(filename.parent, 'profile.properties')
	with open(p_filename, 'r') as f:
		pdata_ = f.readlines()
	if len(pdata_) == 8:
		pdata = [l.strip('\n') for l in pdata_ if not l.startswith('#')]
		try:
			pdata_date = str(pdata_[1][1:]).strip('\n')
			tripdate = to_datetime(pdata_date).to_pydatetime()
			# logger.info(f'[rs] pd:{pdata_date} {type(pdata_date)} td:{tripdate} {type(tripdate)}')
		except (OperationalError, Exception) as e:
			logger.error(f'[readsend] {e}')
			tripdate = None
		trip_profile = dict([k.split('=') for k in pdata])
		torqprofile = TorqProfile()
		torqprofile.fuelCost = float(trip_profile['fuelCost'])
		torqprofile.fuelUsed = float(trip_profile['fuelUsed'])
		torqprofile.distanceWhilstConnectedToOBD = float(trip_profile['distanceWhilstConnectedToOBD'])
		torqprofile.distance = float(trip_profile['distance'])
		torqprofile.time = float(trip_profile['time'])
		torqprofile.filename = p_filename
		torqprofile.tripdate = tripdate
		torqprofile.profile = trip_profile['profile']
		torqprofile.tripid = tripid
		# trip_profile = DataFrame([trip_profile])
		return torqprofile
	else:
		logger.warning(f'[p] {filename} len={len(pdata_)}')
