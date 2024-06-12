import argparse
import sys
import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from loguru import logger
from datamodels import Torqlogs, TorqFile
import matplotlib.pyplot as plt

from plotutils import MAP_CACHE, PLOT_DIR
from plotutils import plot_trip, combine_map_plot, download_maps


def cli_main(args):
	dburl = args.dburl #'sqlite:///torqfiskur.db'
	engine = create_engine(dburl, echo=False, connect_args={'check_same_thread': False})

	Session = sessionmaker(bind=engine)
	session = Session()
	print(args)
	if args.plotid: # plot a single trip
		print(f'plotting {args.plotid}')
		plot_trip(args.plotid, session)
		sys.exit(0)
	elif args.combine:
		print(f'combiner {args.combine}')
		combine_map_plot(args.combine[0], args.combine[1], args.combine[2])
		#combine_map_plot(f'{MAP_CACHE}/tripmap-0001.png', f'{PLOT_DIR}/testplot1.png')
	elif args.dlmaps: # download all maps from mapbox
		download_maps(args, session)
		sys.exit(0)
	elif args.plotall: # make a plot of all trips - no maps
		trips = [k.tripid for k in session.query(TorqFile.tripid).all()]
		for idx,trip in enumerate(trips):
			logger.debug(f'[{idx}/{len(trips)}] plotting {trip}')
			pltfilename = f'{PLOT_DIR}/tripmap-{trip:04d}-plotly.png' # padding
			#tripid = str(trips.iloc[0].values[0])
			df = pd.DataFrame([k for k in session.query(Torqlogs.latitude, Torqlogs.longitude).filter(Torqlogs.fileid==trip).all()])
			px = 1/plt.rcParams['figure.dpi']  # pixel in inches
			fig,ax1 = plt.subplots(figsize=(800*px,600*px))
			plt.axis('off')
			df.plot(x="longitude", y="latitude", kind="scatter",   ax=ax1, marker='.')
			plt.savefig(pltfilename, transparent=True, dpi=100)
			# plt.gcf()
			# plt.show()
			plt.close()
			# fig = make_plt(df)
			#plt.show()
			# plt.savefig(pltfilename, transparent=True)
			# plt.close()
			logger.debug(f'[{idx}/{len(trips)}] saved {pltfilename}')
			# fig = make_plotly(df)
			# fig.write_image(pltfilename)
			# fig.canvas.draw()
			# buf = fig.canvas.buffer_rgba()
			# width, height = fig.canvas.get_width_height()
			# pil_image = Image.frombytes("RGB", (width, height), buf)

			# Save PIL Image as PNG
			# pil_image.save(pltfilename)
			# cv2.imwrite(pltfilename, plt)
			#
			# plt.show(hold=False)
			#plt.close(fig)


if __name__ == '__main__':
	parser = argparse.ArgumentParser(description="plotmap")

	parser.add_argument('-pi', '--plot-id', help="tripid to plot", action="store", dest='plotid')
	parser.add_argument('-c', '--combine', help="combiner mapfile plotfile outfile", action="store", dest='combine', default="", nargs=3)
	parser.add_argument('-pa', '--plot-all', help="plot all", action="store_true", dest='plotall', default=False)
	parser.add_argument('-d', '--download-maps', help="download all maps from mapbox", action="store_true", default=False, dest='dlmaps')
	parser.add_argument('-db', '--dburl', help="database url", action="store", default='sqlite:///torqfiskur.db', dest='dburl')
	args = parser.parse_args()
	cli_main(args)

