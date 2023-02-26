from pathlib import Path
from loguru import logger


def get_bad_vals(csvfile: str):
	badvals = []
	with open(csvfile, 'r') as reader:
		data = reader.readlines()
	for line in data:
		l0 = line.split(',')
		for lx in l0:
			try:
				l1 = lx.encode('ascii')
			except (UnicodeEncodeError, UnicodeDecodeError) as e:
				# logger.warning(f'unicodeerr: {e} in {csvfile} obj={e.object} start={e.start} end={e.end} reason={e.reason}')# lt={type(line)} l={line}')
				badvals.append(e.object[e.start:e.end])
			except AttributeError as e:
				logger.error(f'AttributeError: {e} in {csvfile} lt={type(line)} l={line}')
	badvals = set(badvals)
	if len(badvals) > 0:
		logger.info(f'file: {csvfile} badvals: {len(badvals)}')
	return badvals


if __name__ == '__main__':
	allbads = []
	searchpath = Path('/home/kth/development/torqlogs0/')
	csvfiles = [k for k in searchpath.glob("**/trackLog.csv")]
	for csv in csvfiles:
		bads = get_bad_vals(csv)
		allbads.extend(bads)
	allbads = set(allbads)
	logger.info(f'allbads = {len(allbads)}')
	print(allbads)
