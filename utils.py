# utils and db things here

import os
import re
from datetime import datetime
from hashlib import md5
from pathlib import Path

from loguru import logger

MIN_FILESIZE = 2500



def replace_all(text, dic):
	for i, j in dic.items():
		text = text.replace(i, j)
	return text

def fix_csv_file(tf):
	# read csv file, replace badvals and fix column names
	# returns a buff with the fixed csv file
	with open(tf['csvfilename'], 'r') as reader:
		try:
			data = reader.readlines()
		except UnicodeDecodeError as e:
			logger.error(f'[fix] {e} {tf["csvfilename"]}')
			return None
	badvals = {
		#'-': '0',
		"'-'": '0',
		'"-"': '0',
		',-,': ',0,',
		'∞': '0',
		#',-' : ',0',
		'â': '0',
		'₂': '',
		'°': '',
		'Â°': '0',
		'Â': '0',
		'612508207723425200000000000000000000000': '0',
		'340282346638528860000000000000000000000': '0',
		'-3402823618710077500000000000000000000': '0'}
	lines0 = [k for k in data if not k.startswith('-')]
	lines = [replace_all(b, badvals) for b in lines0]
	orgcol = data[0].split(',')
	newcolname = ','.join([re.sub(r'\W', '', col) for col in orgcol]).encode('ascii', 'ignore').decode()
	newcolname += '\n'
	newcolname = newcolname.lower()
	column_count = newcolname.count(',')
	for idx,line in enumerate(lines):
		if line.count(',') != column_count:
			logger.warning(f'[csvfixer] column_count {tf["csvfilename"]} idx: {idx} line: {line.count(",")} != {column_count}')
	if 'co0ingkmaveragegkm' in newcolname:
		# logger.warning(f'co0ingkmaveragegkm in {tf["csvfilename"]}')
		newcolname = newcolname.replace('co0ingkmaveragegkm', 'coingkmaveragegkm')
	if 'co0ingkminstantaneousgkm' in newcolname:
		# logger.warning(f'co0ingkminstantaneousgkm in {tf["csvfilename"]}')
		newcolname = newcolname.replace('co0ingkminstantaneousgkm', 'coingkminstantaneousgkm')
	lines[0] = newcolname
	if len(data) != len(lines) or len(lines) != len(lines0):
		logger.warning(f'[csvfixer] fn: {tf["csvfilename"]} data: {len(data)} l0: {len(lines0)} l: {len(lines)}')
	return lines

def get_csv_files(searchpath: Path,  dbmode=None):
	# scan searchpath for csv files
	torqcsvfiles = [({
		'csvfilename': k, # original csv file
		'csvfilefixed': f'{k}.fixed.csv', # fixed csv file
		'size': os.stat(k).st_size,
		'dbmode': dbmode}) for k in searchpath.glob("**/trackLog.csv") if k.stat().st_size >= MIN_FILESIZE] # and not os.path.exists(f'{k}.fixed.csv')]
	for idx, tf in enumerate(torqcsvfiles):
		if os.path.exists(tf['csvfilefixed']):
			# logger.warning(f'[g] {tf["csvfilefixed"]} exists')
			with open(tf['csvfilefixed'], 'r') as reader:
				fixedlines = reader.readlines()
		else:
			fixedlines = fix_csv_file(tf)
			with open(file=tf['csvfilefixed'], mode='w', encoding='utf-8', newline='') as writer:
				writer.writelines(fixedlines)
			logger.debug(f'[gcv] {idx}/{len(torqcsvfiles)} {tf["csvfilefixed"]} saved')

		csvhash = md5(open(torqcsvfiles[idx]['csvfilename'], 'rb').read()).hexdigest()
		fixedhash = md5(open(torqcsvfiles[idx]['csvfilefixed'], 'rb').read()).hexdigest()
		torqcsvfiles[idx] = {
			'csvfilename': tf['csvfilename'],
			'csvfilefixed' : tf['csvfilefixed'],
			'csvhash': csvhash,
			'fixedhash': fixedhash,
			'csvtimestamp': f'{datetime.fromtimestamp(int(tf["csvfilename"].parts[-2][0:10]))}',
			'dbmode': dbmode,
		}
	return torqcsvfiles

def get_bad_vals(csvfile: str):
	with open(csvfile, 'r') as reader:
		data = reader.readlines()
	for line in data:
		l0 = line.split(',')
		for lx in l0:
			try:
				l1 = lx.encode('ascii')
			except (UnicodeEncodeError, UnicodeDecodeError) as e:
				logger.error(f'unicodeerr: {e} in {csvfile} lt={type(line)} l={line}')
			except AttributeError as e:
				logger.error(f'AttributeError: {e} in {csvfile} lt={type(line)} l={line}')
