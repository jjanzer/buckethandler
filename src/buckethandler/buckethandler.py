import re
import math
import datetime
import time
from enum import Enum
from typing import Union, List

from .handler.b2 import BackblazeB2Handler
from .handler.s3 import S3Handler

class BucketHandlerType(Enum):
	B2 = 1
	S3 = 2
	DROPBOX = 3
	UNKNOWN = 99

class BucketHandlerHandlerTypeException(Exception):
	pass

class BucketHandler:
	def __init__(self, config=None, handler_type:BucketHandlerType=BucketHandlerType.UNKNOWN, path:str = ''):
		self.config = config
		self.handler_type = handler_type
		self.path = path

		if handler_type == BucketHandlerType.UNKNOWN and path is not None:
			self.handler_type = self._guess_protocol(path)

		if self.handler_type == BucketHandlerType.B2:
			from .handler.b2 import BackblazeB2Handler
			self.handler = BackblazeB2Handler(config)
		elif self.handler_type == BucketHandlerType.S3:
			from .handler.s3 import S3Handler
			self.handler = S3Handler(config)
		elif self.handler_type == BucketHandlerType.DROPBOX:
			from .handler.dropbox import DropboxHandler
			self.handler = DropboxHandler(config)
		else:
			raise BucketHandlerHandlerTypeException(f"Unknown handler type for path: {path}")

	def _guess_protocol(self,path):
		if path is None:
			return None

		if isinstance(path, list):
			for p in path:
				# use the first one, yes this will break if you have something like b2://... and then s3://...
				return self._guess_protocol(p)
			return None

		if path.startswith("b2://"):
			return BucketHandlerType.B2
		elif path.startswith("s3://"):
			return BucketHandlerType.S3
		elif path.startswith("db://"):
			return BucketHandlerType.DROPBOX
		else:
			return BucketHandlerType.UNKNOWN

	def __bool__(self):
		return self.handler is not None

	def search(self, prefix, include=None, min_size=None, max_size=None, include_dirs=True, include_files=True, recurse=True, limit=0):
		if min_size is not None and isinstance(min_size, str):
			min_size = from_pretty_file_size(min_size)
		if max_size is not None and isinstance(max_size, str):
			max_size = from_pretty_file_size(max_size)
		results = self.handler.search(prefix=prefix, include=include, min_size=min_size, max_size=max_size, include_dirs=include_dirs, include_files=include_files, recurse=recurse, limit=limit)
		if limit > 0 and len(results.get('files', [])) > limit:
			results['files'] = results['files'][:limit]
		return results

	def upload(self, path_root: Union[str, List[str]], destination_root:str):
		return self.handler.upload(path_root=path_root, destination_root=destination_root)

	def download(self, prefix: Union[str, List[str]], destination_root=None, include=None, min_size=None, max_size=None, recurse=True, preserve_dir_prefix=False):
		if min_size is not None and isinstance(min_size, str):
			min_size = from_pretty_file_size(min_size)
		if max_size is not None and isinstance(max_size, str):
			max_size = from_pretty_file_size(max_size)
		return self.handler.download(prefix=prefix, destination_root=destination_root, include=include, min_size=min_size, max_size=max_size, recurse=recurse, preserve_dir_prefix=preserve_dir_prefix)

	def set_max_download_threads(self,max_threads):
		self.handler.set_max_download_threads(max_threads)

	def set_max_upload_threads(self,max_threads):
		self.handler.set_max_upload_threads(max_threads)

	def set_max_upload_single_threads(self,max_threads):
		self.handler.set_max_upload_single_threads(max_threads)

	def set_failsafe_copy(self,path):
		self.handler.set_failsafe_copy(path)

	def strip_protocol_from_path(self, path):
		return self.handler.strip_protocol_from_path(path)

	def get_download_url(self,path:Union[str,List[str]],expiration_seconds=60*60, inline=False, content_type=None):
		return self.handler.get_download_url(path=path, expiration_seconds=expiration_seconds, inline=inline, content_type=content_type)

# Helpers

def pretty_file_size(bytes:int) -> str:
	'''
	Converts a number like 10 * 1024 * 1024 to 10MB
	'''
	if bytes is None or bytes == 0:
		return "0B"
	bytes_f = float(bytes)
	for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
		if bytes_f < 1024:
			return f"{bytes_f:.2f}{unit}"
		bytes_f /= 1024
	return f"{bytes_f:.2f}PB"

def from_pretty_file_size(raw_str:str) -> int:
	'''
	Converts something like 10MB to 10*1024*1024 or 5*1024 to 5KB or 46 to 46B
	'''
	raw_str = raw_str.strip().upper()
	match = re.match(r'([0-9\.]+)([KMGTP]?B)', raw_str)
	if not match:
		raise ValueError(f"Invalid file size format: {raw_str}")

	size = float(match.group(1))
	unit = match.group(2)

	if unit == 'KB':
		bytes = size * 1024
	elif unit == 'MB':
		bytes = size * 1024 * 1024
	elif unit == 'GB':
		bytes = size * 1024 * 1024 * 1024
	elif unit == 'TB':
		bytes = size * 1024 * 1024 * 1024 * 1024
	else:
		bytes = size

	return math.ceil(bytes)

def pretty_print_files(files):
	minTime = 0
	maxTime = 0

	max_filename_str_len = 0
	max_filesize_str_len = 0
	max_content_type_str_len = 0
	last_line = ''

	for file in files.get('files', []):
		max_filename_str_len = max(max_filename_str_len, len(file['fileName']))
		max_filesize_str_len = max(max_filesize_str_len, len(pretty_file_size(file['contentLength'])))
		content_type_str = ""
		if file['action'] == 'upload':
			content_type_str = "" if file['contentType'] is None else file['contentType'].ljust(max_content_type_str_len)
		elif file['action'] == 'folder':
			content_type_str = ""
		elif file['action'] == 'hide':
			content_type_str = ""
		elif file['action'] == 'list':
			content_type_str = ""

		max_content_type_str_len = max(max_content_type_str_len, len(content_type_str))

	for file in files.get('files', []):
		upload_timestamp = file['uploadTimestamp']
		if upload_timestamp > 0 and (minTime == 0 or upload_timestamp < minTime):
			minTime = upload_timestamp
		maxTime = max(maxTime, upload_timestamp)

		pretty_file_size_str = pretty_file_size(file['contentLength'])

		file_name_str = file['fileName'].ljust(max_filename_str_len)
		content_type_str = ""

		time_str = ""

		# there are 4 actions: start, upload, hide, folder, see: https://www.backblaze.com/apidocs/b2-list-file-names
		if file['action'] == 'upload':
			content_type_str = "" if file['contentType'] is None else file['contentType'].ljust(max_content_type_str_len)
			time_str = datetime.datetime.fromtimestamp(upload_timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
		elif file['action'] == 'folder':
			content_type_str = "".ljust(max_content_type_str_len)
			pretty_file_size_str = ""
		elif file['action'] == 'hide':
			content_type_str = "".ljust(max_content_type_str_len)
			pretty_file_size_str = ""
		elif file['action'] == 'list':
			content_type_str = "".ljust(max_content_type_str_len)
			pretty_file_size_str = ""

		file_size_str = pretty_file_size_str.ljust(max_filesize_str_len)
		line = f"{file_name_str}\t{content_type_str}\t{file_size_str}\t{time_str}"
		last_line = line
		print(line)

	min_time_str = datetime.datetime.fromtimestamp(minTime / 1000).strftime('%Y-%m-%d %H:%M:%S')
	max_time_str = datetime.datetime.fromtimestamp(maxTime / 1000).strftime('%Y-%m-%d %H:%M:%S')

	min_max_time_delta_ms = maxTime - minTime
	min_max_time_delta = min_max_time_delta_ms / 1000
	# convert the seconds to a time range like 01:23:45 for 1 day 23 hours 45 seconds
	days = int(min_max_time_delta // 86400)
	hours = int((min_max_time_delta % 86400) // 3600)
	minutes = int((min_max_time_delta % 3600) // 60)
	seconds = int(min_max_time_delta % 60)

	line_sep = "=" * len(last_line.expandtabs())
	print(line_sep)
	print(f"Files: {len(files.get('files', []))}, minTime: {min_time_str} maxTime: {max_time_str} time delta: {days}:{hours:02d}:{minutes:02d}:{seconds:02d}")
