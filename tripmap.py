from decimal import DivisionByZero
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from PIL import Image, ImageDraw
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, inspect, select, Numeric, DateTime, text, BIGINT, BigInteger, Float
from sqlalchemy.exc import OperationalError, DataError

from hashlib import md5
from utils import get_csv_files
from threading import Thread, active_count
import psycopg2
from datamodels import TorqTrip, TorqFile, TorqEntry

from loguru import logger


class GPSVis(object):
    """
        Class for GPS data visualization using pre-downloaded OSM map in image format.
    """
    def __init__(self, data_path, map_path, points):
        """
        :param data_path: Path to file containing GPS records.
        :param map_path: Path to pre-downloaded OSM map in image format.
        :param points: Upper-left, and lower-right GPS points of the map (lat1, lon1, lat2, lon2).
        """
        self.data_path = data_path
        self.points = points
        self.map_path = map_path

        self.result_image = Image
        self.x_ticks = []
        self.y_ticks = []

    def plot_map(self, output='save', save_as='resultMap.png'):
        """
        Method for plotting the map. You can choose to save it in file or to plot it.
        :param output: Type 'plot' to show the map or 'save' to save it.
        :param save_as: Name and type of the resulting image.
        :return:
        """
        self.get_ticks()
        fig, axis1 = plt.subplots(figsize=(10, 10))
        axis1.imshow(self.result_image)
        axis1.set_xlabel('Longitude')
        axis1.set_ylabel('Latitude')
        axis1.set_xticklabels(self.x_ticks)
        axis1.set_yticklabels(self.y_ticks)
        axis1.grid()
        if output == 'save':
            plt.savefig(save_as)
        else:
            plt.show()

    def create_image(self, color, width=2):
        """
        Create the image that contains the original map and the GPS records.
        :param color: Color of the GPS records.
        :param width: Width of the drawn GPS records.
        :return:
        """
        data = pd.read_csv(self.data_path, names=['LATITUDE', 'LONGITUDE'], sep=',')

        self.result_image = Image.open(self.map_path, 'r')
        img_points = []
        gps_data = tuple(zip(data['LATITUDE'].values, data['LONGITUDE'].values))
        for d in gps_data:
            x1, y1 = self.scale_to_img(d, (self.result_image.size[0], self.result_image.size[1]))
            img_points.append((x1, y1))
        draw = ImageDraw.Draw(self.result_image)
        draw.line(img_points, fill=color, width=width)

    def scale_to_img(self, lat_lon, h_w):
        """
        Conversion from latitude and longitude to the image pixels.
        It is used for drawing the GPS records on the map image.
        :param lat_lon: GPS record to draw (lat1, lon1).
        :param h_w: Size of the map image (w, h).
        :return: Tuple containing x and y coordinates to draw on map image.
        """
        # https://gamedev.stackexchange.com/questions/33441/how-to-convert-a-number-from-one-min-max-set-to-another-min-max-set/33445
        old = (self.points[2], self.points[0])
        new = (0, h_w[1])
        y = ((lat_lon[0] - old[0]) * (new[1] - new[0]) / (old[1] - old[0])) + new[0]
        old = (self.points[1], self.points[3])
        new = (0, h_w[0])
        x = ((lat_lon[1] - old[0]) * (new[1] - new[0]) / (old[1] - old[0])) + new[0]
        # y must be reversed because the orientation of the image in the matplotlib.
        # image - (0, 0) in upper left corner; coordinate system - (0, 0) in lower left corner
        return int(x), h_w[1] - int(y)

    def get_ticks(self):
        """
        Generates custom ticks based on the GPS coordinates of the map for the matplotlib output.
        :return:
        """
        self.x_ticks = map(
            lambda x: round(x, 4),
            np.linspace(self.points[1], self.points[3], num=7))
        y_ticks = map(
            lambda x: round(x, 4),
            np.linspace(self.points[2], self.points[0], num=8))
        # Ticks must be reversed because the orientation of the image in the matplotlib.
        # image - (0, 0) in upper left corner; coordinate system - (0, 0) in lower left corner
        self.y_ticks = sorted(y_ticks, reverse=True)

def scale_to_imgold(lat_lon, h_w):
	"""
	Conversion from latitude and longitude to the image pixels.
	It is used for drawing the GPS records on the map image.
	:param lat_lon: GPS record to draw (lat1, lon1).
	:param h_w: Size of the map image (w, h).
	:return: Tuple containing x and y coordinates to draw on map image.
	"""
	# https://gamedev.stackexchange.com/questions/33441/how-to-convert-a-number-from-one-min-max-set-to-another-min-max-set/33445

	newy = (0, h_w[1])
	y = ((lat_lon[0] ) * (newy[1] - newy[0])) + newy[0]

	newx = (0, h_w[0])
	x = ((lat_lon[1]) * (newx[1] - newx[0]) ) + newx[0]

	# y must be reversed because the orientation of the image in the matplotlib.
	# image - (0, 0) in upper left corner; coordinate system - (0, 0) in lower left corner
	res = int(x), h_w[1] - int(y)
	logger.info(f'[s] l:{lat_lon} hw:{h_w} nx:{newx} x:{x} ny:{newy} y:{y} res:{res}')
	return res

def scale_to_img(lat_lon, h_w, points):
	# points: Upper-left, and lower-right GPS points of the map (lat1, lon1, lat2, lon2)
	# y (self.points[2], self.points[0])
	# x old = (self.points[1], self.points[3])
	# points = (43.5371, -2.2172, 43.1240, -1.1391)
	# points = (43.4110, -2.1248, 43.2044, -1.6716)
	# points = (43.4110, -2.1248, 43.2044, -1.5858)
	oldy = (points[2], points[0]) #  (lat_lon[1], lat_lon[0])
	newy = (0, h_w[1])
	try:
		y = ((lat_lon[0] - oldy[0]) * (newy[1] - newy[0]) / (oldy[1] - oldy[0])) + newy[0]
	except ZeroDivisionError as e:
		#print(f'err {e} p:{points} l:{lat_lon} hw:{h_w} oldy:{oldy} newy:{newy}')
		y = 0


	oldx = (points[1], points[3]) # (lat_lon[1], lat_lon[0])
	newx = (0, h_w[0])
	try:
		x = ((lat_lon[1] - oldx[0]) * (newx[1] - newx[0]) / (oldx[1] - oldx[0])) + newx[0]
	except ZeroDivisionError as e:
		#print(f'err {e} p:{points} l:{lat_lon} hw:{h_w} oldx:{oldx} newx:{newx}')
		x = 0

	res = int(x), h_w[1] - int(y)
	# logger.info(f'[s] l:{lat_lon} hw:{h_w} ox:{oldx} nx:{newx} x:{x} oy:{oldy} ny:{newy} y:{y} res:{res}')
	return res


if __name__ == '__main__':
	param_dic = {
		'dialect': 'mysql',
		'driver': 'pymysql',
		'host' : 'elitedesk',
		'database' : 'torq9',
		'user' : 'torq',
		'password' : '',
		'pool_size': 200,
		'max_overflow':0,
		'port': 3306
	}

	connect = "%s+%s://%s:%s@%s:%s/%s" % (
	param_dic['dialect'],
	param_dic['driver'],
	param_dic['user'],
	param_dic['password'],
	param_dic['host'],
	param_dic['port'],
	param_dic['database'])
	engine = create_engine(connect)
	tasks = []
	if engine.driver == 'mysql':
		conn = engine.connect()
	Session = sessionmaker(bind=engine)
	session = Session()
	sql = f"SELECT tripid FROM torqtrips order by tripdate desc limit 30"
	# sql = f"SELECT tripid FROM torqtrips"
	mycursor = session.execute(sql)
	trips = mycursor.fetchall()
	tripidlist = [k[0] for k in trips]
	image = Image.open('map3w.png', 'r')  # Load map image.
	pointlist_q = []

	pointlist_q.append(f"select min(Latitude), min(Longitude), max(Latitude), max(Longitude) from torqlogs where tripid=")
	pointlist_q.append(f"select max(Latitude), min(Longitude), min(Latitude), max(Longitude) from torqlogs where tripid=")
	pointlist_q.append(f"select max(Latitude), max(Longitude), min(Latitude), min(Longitude) from torqlogs where tripid=")
	points_old = (43.4110, -2.1248, 43.2044, -1.5858)

	for tidx, tripid in enumerate(tripidlist):
		pointlist = []
		sql = f"SELECT Latitude,Longitude FROM torqlogs where tripid={tripid}"
		gps_data = session.execute(sql).fetchall()
		sql = f"SELECT GPSSpeedkmh FROM torqlogs where tripid={tripid}"
		GPSSpeedkmh_ = session.execute(sql).fetchall()
		GPSSpeedkmh = [k[0] for k in GPSSpeedkmh_]
		#pointlist.append(f"select max(Latitude), min(Longitude) , min(Latitude), max(Longitude) from torqlogs where tripid={tripid}")
		# sql = f"select max(Latitude) as maxlat, max(Longitude) as maxlon, min(Latitude) as minlat, min(Longitude) as minlon from torqlogs where  tripid={tripid}"
		# sql = f"select max(Latitude), min(Longitude) , min(Latitude), max(Longitude) from torqlogs where tripid={tripid}"
		# sql = f"select max(Latitude), max(Longitude) , min(Latitude), min(Longitude)from torqlogs where tripid={tripid}"
		# sql = f"select max(Latitude), min(Longitude) , min(Latitude), max(Longitude) from torqlogs where tripid={tripid}"
		# sql = f"select min(Latitude), min(Longitude), max(Latitude), max(Longitude) from torqlogs where tripid={tripid}"
		for sql in pointlist_q:
			try:
				pointlist.append(session.execute(f'{sql}{tripid}').fetchall()[0])
			except OperationalError as e:
				print(f'err {e.code} {e.args[0]}')
				points = [(0,0,0,0)]
		# pointlist.append(points_old)
		img_points = []
		img_pointsold = []
		gps_speed_points = []
		draw = ImageDraw.Draw(image)
		print(f'tidx:{tidx} tripid:{tripid} p:{len(pointlist)} po:{points_old}')

		for idx,d in enumerate(gps_data):
			img_points = []
			#x1, y1 = scale_to_img(d, (image.size[0], image.size[1]), points_old)
			#draw.point((x1,y1), fill=(255,0,0))
			for idxd, points in enumerate(pointlist):
				x1, y1 = scale_to_img(d, (image.size[0], image.size[1]), points)
				# gpsfill = (int(GPSSpeedkmh[idx])+30,tidx+20,int(GPSSpeedkmh[idx])*idxd+1)
				gpsfill = (30,tidx,123)
				if x1 >= 0 and y1 >=0:
					draw.point((x1,y1), fill=gpsfill)
					img_points.append((x1,y1))
				else:
					pass
		image.save(f'resultMap9f-{tripid}.png')
			# draw.line(img_points, fill=(55+tidx, tripid, tidx), width=1)
					# print(f'{x1} {y1}')
			# print(f'[d] idx:{idx} g:{gpsfill} p:{len(img_points)} x1:{x1} y1:{y1}')
			#draw.line(img_points, fill=(55, 50+(idx*2), 0), width=1)

			#x1old, y1old = scale_to_img(d, (image.size[0], image.size[1]), points_old)
			#img_pointsold.append((x1old, y1old))
			# gpsfill = (int(GPSSpeedkmh[idx])+30,90,90)
			# print(gpsfill)
			#draw.point((x1,y1), fill=gpsfill)
			# draw.rounded_rectangle([(x1,y1), (x1+1,y1+1)], radius=int(GPSSpeedkmh[idx]), fill=(int(GPSSpeedkmh[idx]),idx,int(GPSSpeedkmh[idx])))
			# gps_speed_points.append((x1,y1, GPSSpeedkmh[idx]))

		#draw.line(img_pointsold, fill=(155, 0, 0), width=1)  # Draw converted records to the map image.
		# draw.point(img_points, f)

	# image.save(f'resultMap9f.png')
	# vis = GPSVis(data_path='data.csv',
	# 			map_path='map.png',  # Path to map downloaded from the OSM.
	# 			points=(45.8357, 15.9645, 45.6806, 16.1557)) # Two coordinates of the map (upper left, lower right)

	# vis.create_image(color=(0, 0, 255), width=3)  # Set the color and the width of the GNSS tracks.
	# vis.plot_map(output='save')