from sqlalchemy import create_engine
import pandas as pd

# from datamodels import TorqFile

TORQDBHOST = 'localhost'
TORQDBUSER = 'torq'
TORQDBPASS = 'dzt3f5jCvMlbUvRG'

dburl = f"mysql+pymysql://{TORQDBUSER}:{TORQDBPASS}@{TORQDBHOST}/torq?charset=utf8mb4"
engine = create_engine(dburl)

max_results = 10
toptrips = pd.read_sql(f'select * from torqtrips order by distance desc limit {max_results}', engine)

for trip in toptrips.id:
	chk_cols = ['speedgpskmh', 'speedobdkmh', 'averagetripspeedwhilststoppedormovingkmh', 'distancetravelledwithmilcellitkm', 'kilometersperlitrelongtermaveragekpl', 'averagetripspeedwhilstmovingonlykmh',
	            'litresper100kilometerlongtermaveragel100km', 'tripdistancestoredinvehicleprofilekm', 'tripdistancekm', 'triptimesincejourneystarts', 'costpermilekmtripkm', 'tripaveragelitres100kml100km', 'enginekwatthewheelskw',
	            'costpermilekminstantkm', 'tripaveragempgmpg', 'tripaveragekplkpl', 'triptimewhilstmovings', 'actualenginetorque', 'triptimewhilststationarys']
	print(f'[trip] {trip}')
	for c in chk_cols[0:4]:
		print(f'[c] c:{c} t:{trip}')
		res = pd.read_sql(f'SELECT MIN({c}), MAX({c}), AVG({c}) FROM torqlogs WHERE tripid = "{trip}"', engine)
		if None not in res:
			print(f'[r] {res}')
		else:
			pass


if __name__ == '__main__':
	pass
