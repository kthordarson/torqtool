# torq web listener
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib import parse
from urllib.parse import urlparse, parse_qs, parse_qsl
from io import IOBase
import re
import time
import pandas as pd
from csv import writer
from functools import partial


class TorqServer():
	def __init__(self, host_address='127.0.0.1', host_port=8888, filename=None, *args, **kwargs):
		self.filename = open(filename, 'a', newline='')
		self.writer = writer(self.filename)
		self.handler = TorqWebHandler  # (filename=self.filename, writer=self.writer, *args, **kwargs)
		self.handler.filename = self.filename
		self.handler.writer = self.writer
		# self.handler.set_filename(self, filename=self.filename)
		# self.handler.set_writer(self, writer=self.writer)
		self.server = HTTPServer((host_address, host_port), self.handler)
		print(f'[TorqServer] init f:{self.filename} w:{self.writer} h:{self.handler} s:{self.server}')
	def startserver(self):
		print(f'[TorqServer] startserver ... ')
		try:
			self.server.serve_forever()
		except KeyboardInterrupt as e:
			print(f'[TorqServer] KeyboardInterrupt {e}')
		finally:
			self.server.server_close()
	def handle_errors(self, request, client_address):
		print(f'[tweb] server err from {client_address} request: {request}')

class TorqWebHandler(BaseHTTPRequestHandler):
	def __init__(self,request, client_address, server, *args, **kwargs):
		#self.filename = filename
		try:
			super().__init__(request, client_address, server, *args, **kwargs)
		except ConnectionResetError as e:
			print(f'[thandler] ConnectionResetError {e}')
			time.sleep(3)
			super().__init__(request, client_address, server, *args, **kwargs)
		self.filename = None
		self.writer = None

	@property
	def filename(self):
		return self.__filename
	@filename.setter
	def filename(self, logfilename):
		self.__filename = logfilename
	@property
	def writer(self):
		return self.__writer
	@writer.setter
	def writer(self, writerobject):
		self.__writer = writerobject
	def handle(self):
		try:
			BaseHTTPRequestHandler.handle(self)
		except ConnectionResetError as e:
			print(f'[tweb] handler err {e}')
			# pass
	def handle_errors(self, request, client_address):
		print(f'[tweb] handler err from {client_address} request: {request}')
	def set_filename(self, filename):
		self.filename = filename
		print(f'[twh] set_filname to {filename} {self.filename}')

	def set_writer(self, writer):
		self.writer = writer
		print(f'[twh] set_writer to {writer} {self.writer}')

	def log_request(self, code=None):  # surpress console logging
		raw = None
		data = None
		try:
			raw = self.path[1:]
		except AttributeError as e:
			print(f'[qhandler] AttributeError {e} r:{raw}')
		try:
			data = parse_qsl(qs=raw, keep_blank_values=True, strict_parsing=True)
		except ValueError as e:
			print(f'[qhandler] ValueError {e} r:{raw}')
			data = None
			raw = None
		try:
			# print(f'[{len(data)}] {data[:4]}')
			if data is not None:
				self.writer.writerow(data)
		except AttributeError as ae:
			print(f'[log_request ERR] {ae} {self.filename} {self.writer}')
		#print(f'[GET] {type(data)} {len(data)} ')

	def do_HEAD(self):
		self.send_response(200)
		self.send_header('Content-type', 'text/plain')
		self.end_headers()

	def do_GET(self):
		self.do_HEAD()
		self.wfile.write(f'{self.filename} {self.writer}\n'.encode('utf8'))

def start_web(engine):
	hostname = '192.168.1.222'
	host_port = 8888
	torqlogfile = 'torqincoming.csv'
	server = TorqServer(host_address=hostname, host_port=host_port, filename=torqlogfile)
	print(f'[tweb] init host: {hostname} port: {host_port} lf:{torqlogfile} s:{server}')
	# server.RequestHandlerClass.set_file(torqlogfile)
	print(f'[tweb] {time.asctime()} starting')
	server.startserver()