'''
Main BackBlaze b2 handler, you don't directly interface with this, see bh.py for usage.
'''

import os
import glob
import requests
import base64
import mimetypes
import urllib.parse
import math
import time
import shutil
import re
import math
import traceback
import threading
import json
from typing import Union, List

from concurrent.futures import ProcessPoolExecutor,ThreadPoolExecutor,wait,as_completed,Future

from enum import Enum

#stop_event = threading.Event()

'''
This does nothing useful in uploads...
'''
'''
from http.client import HTTPConnection

HTTPConnection.__init__.__defaults__ = tuple(
	x if x != 8192 else 64 * 1024
	for x in HTTPConnection.__init__.__defaults__
)
'''

def pretty_file_size(bytes):
	'''
	Converts a number like 10 * 1024 * 1024 to 10MB
	'''
	if bytes is None:
		return "0B"
	for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
		if bytes < 1024:
			return f"{bytes:.2f}{unit}"
		bytes /= 1024
	return f"{bytes:.2f}PB"

def from_pretty_file_size(raw_str):
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

class BackblazeB2Handler:

	class RequestMethod(Enum):
		GET = 1
		POST = 2
		PUT = 3
		DELETE = 4

	def __init__(self, config):

		# if they passed in a string, assume it's a file path to a config that is a json
		if type(config) == str:
			config = self._load_config_file(config)

		self.config = config
		self.base_url = "https://api.backblazeb2.com/b2api/v4"
		self.download_url = None
		self.token = None
		self.accountId = None
		self.thread_local = threading.local()

		# How many bytes we can stuff per upload_part
		self.max_bytes_per_chunk = 100 * 1024 * 1024  # 100 MB
		# B2 only supports files up to 5GB and then they need to use a separate API for uploading
		self.large_file_upload_limit = 1024 * 1024 * 1024 # 1GB limit
		# how many times do we allow a retry?
		self.max_retries = 5
		# set to a path if you want to use a failsafe copy for when upload fails
		self.failsafe_copy = None # example: "./staging_upload/"

		# when uploading a large file how many threads should we use per file
		self.max_upload_single_threads = 4

		# how many threads will be used if we're uploading many files (not single file uploads)
		self.max_upload_threads = 4

		# how many threads will be used if we're downloading many download files (not single file downloads)
		self.max_download_threads = 8

		# if set it will attempt to set the modified time to remote file's time
		self.download_sync_mtime = True


	def _strip_protocol_from_path(self,path:str) -> str:
		if path[:5].lower().startswith('b2://'):
			return path[5:]
		return path

	def _remove_prefix(self, text:str, prefix:str) -> str:
		# python 3.9 has "removeprefix" but we might not be on that version
		if text.startswith(prefix):
			return text[len(prefix):]
		return text

	def _load_config_file(self, path='config.json'):
		'''
		Load the configuration from a file.

		Args:
			path (str): The path to the configuration file.

		Returns:
			dict: The loaded configuration.
		'''
		with open(path, 'r') as file:
			return json.load(file)

	def set_max_download_threads(self,max_threads):
		self.max_download_threads = max_threads

	def set_max_upload_threads(self,max_threads):
		self.max_upload_threads = max_threads

	def set_max_upload_single_threads(self,max_threads):
		self.max_upload_single_threads = max_threads

	def _encode_credentials(self,account_key, application_key):
		credentials = f"{account_key}:{application_key}"
		bytes = credentials.encode('utf-8')
		encoded = base64.b64encode(bytes)
		return encoded.decode('utf-8')

	def _is_authenticated(self):
		return self.token is not None

	def _authenticate(self):
		url = f"{self.base_url}/b2_authorize_account"
		credentials = self._encode_credentials(self.config['account_key'], self.config['application_key'])
		headers = {
			'Authorization': f"Basic{credentials}"
		}
		response = requests.get(url, headers=headers)
		if response.status_code == 200:
			data = response.json()
			self.token = data['authorizationToken']
			self.accountId = data['accountId']
			self.root_url = f"{data['apiInfo']['storageApi']['apiUrl']}"
			self.base_url = f"{data['apiInfo']['storageApi']['apiUrl']}/b2api/v4"
			self.download_url = f"{data['apiInfo']['storageApi']['downloadUrl']}"
			return data
		else:
			raise Exception("Authentication failed: " + response.text)

	def _auto_authenticate(self):
		if not self._is_authenticated():
			return self._authenticate()
		return None

	def _quote(self,path):
		'''
		Handles percent encoding for paths as B2 requires this
		'''
		# Note we DO allow "/" symbol
		return urllib.parse.quote(path)

	def _make_request(self,url,headers=None,data=None,json=None,method=RequestMethod.GET,authenticate=False):
		'''
		Attempts to make a network request, if max_retries > 0 will reattempt on non 200, 206, 400 response
		'''

		response = None
		response_status_code = None

		if authenticate:
			self._auto_authenticate()
			if headers is None:
				headers = {}
			headers['Authorization'] = self.token

		for attempt in range(self.max_retries):
			if method == self.RequestMethod.GET:
				response = requests.get(url, headers=headers)
			elif method == self.RequestMethod.POST:
				response = requests.post(url, headers=headers,data=data,json=json)
			elif method == self.RequestMethod.PUT:
				response = requests.put(url, headers=headers,data=data,json=json)
			elif method == self.RequestMethod.DELETE:
				response = requests.delete(url, headers=headers,data=data,json=json)
			else:
				raise Exception("Unsupported request method")

			response_status_code = response.status_code if response else None

			if response.status_code == 200 or response.status_code == 206 or response.status_code == 404:
				return response

			if response.status_code == 400:
				print(f"Bad request (attempt {attempt+1}/{self.max_retries}): {response.status_code} => {url}")
				return response

			print(f"Request failed (attempt {attempt+1}/{self.max_retries}): {response.status_code} => {url}")
			time.sleep(2 ** attempt)

		raise Exception(f"Failed to make request after retries: {response_status_code}")

	def _get_upload_key(self):
		url = f"{self.base_url}/b2_get_upload_url?bucketId={self.config['bucket_id']}"

		response = self._make_request(url, method=self.RequestMethod.GET, authenticate=True)

		if response.status_code == 200:
			# You will need both the uploadUrl and the authorizationToken to upload files
			j = response.json()
			result = {
				"bucketId": j['bucketId'],
				"uploadUrl": j['uploadUrl'],
				"authorizationToken": j['authorizationToken'],
			}
			return result
		else:
			raise Exception("Failed to get upload key: " + response.text)

	def _get_upload_part_key(self,fileId):

		url = f"{self.base_url}/b2_get_upload_part_url?fileId={fileId}"

		response = self._make_request(url, method=self.RequestMethod.GET, authenticate=True)

		if response.status_code == 200:
			# You will need both the uploadUrl and the authorizationToken to upload files
			j = response.json()
			result = {
				"fileId": j['fileId'],
				"uploadUrl": j['uploadUrl'],
				"authorizationToken": j['authorizationToken'],
			}
			return result
		else:
			raise Exception("Failed to get upload key: " + response.text)

	def _finish_large_file(self, file_id, shas):
		url = f"{self.base_url}/b2_finish_large_file"
		data = {
			'fileId': file_id,
			'partSha1Array': shas,
		}
		response = self._make_request(url, json=data, method=self.RequestMethod.POST, authenticate=True)
		if response.status_code == 200:
			return response.json()
		else:
			raise Exception("Failed to mark large file complete: " + response.text)

	def _calculate_sha1(self, content):
		'''
		Calculate the SHA1 hash of the content.

		Args:
			content (bytes): The content to hash.

		Returns:
			str: The SHA1 hash in hexadecimal format.
		'''
		import hashlib
		hasher = hashlib.sha1()
		hasher.update(content)
		return hasher.hexdigest()

	def _clean_destination_root(self,destination_root):
		if destination_root.startswith('/'):
			# Do not allow a preceeding slash or it creates a weird folder structure
			destination_root = destination_root.lstrip('/')
		if destination_root.endswith('/'):
			destination_root = destination_root.rstrip('/')
		return destination_root

	def _get_remote_path_from_local_path(self,path,destination_root=None,preserve_local_dir=False):
		'''
		Tells us the remote location we're uploading the file to
		'''
		file_name = os.path.basename(path)

		destination_root = self._clean_destination_root(destination_root)

		if preserve_local_dir == False:
			path = os.path.basename(path)

		if path.startswith('/'):
			path = path.lstrip('/')
		elif path.startswith('./'):
			path = path.lstrip('./')
		elif path.startswith('.\\'):
			path = path.lstrip('.\\')


		upload_path = f"{destination_root}/{path}" if destination_root else file_name

		upload_path = upload_path.replace('\\', '/')  # Ensure the path is in Unix format

		# Handle things like "a b" => "a%20b"
		upload_path = self._quote(upload_path)

		return upload_path


	def _upload_file(self,path_src,path_dst, upload_key=None):
		'''
		Upload a file to the bucket.

		Args:
			path_src (str): The path to the file to upload.
			path_dst (str): The destination path in the bucket where the file will be uploaded.

		Returns:
			dict: The result of the upload operation.
		'''
		self._auto_authenticate()

		content_type = mimetypes.guess_type(path_src)[0] or 'application/octet-stream'

		with open(path_src, 'rb') as file:
			content = file.read()

		#if upload_key is None:
		#	#upload_key = self.GetUploadKey(file_name, content_type)
		#	upload_key = self._get_upload_key()

		# upload key if not set will be generated once per thread
		# they will be unset when the thread finishes
		if not hasattr(self.thread_local,'upload_key'):
			self.thread_local.upload_key = self._get_upload_key()

		upload_key = self.thread_local.upload_key

		#url = f"{self.base_url}/b2_upload_file"
		url = upload_key['uploadUrl']

		#print(f"Uploading file: {path} to {upload_path} with content type: {content_type}")

		print(f"Uploaded {path_src} => {path_dst}")

		headers = {
			'Authorization': upload_key['authorizationToken'],
			'Content-Type': content_type,
			'Content-Length': str(len(content)),
			'X-Bz-File-Name': path_dst,
			'X-Bz-Content-Sha1': self._calculate_sha1(content),
		}
		response = self._make_request(url, headers=headers, data=content, method=self.RequestMethod.POST, authenticate=False)
		if response.status_code == 200:
			return response.json()
		else:
			raise Exception("Failed to upload file: " + response.text)


	def _start_large_file_upload(self, path, remote_path):
		'''
		Initiate the large file upload process.
		'''
		self._auto_authenticate()
		url = f"{self.base_url}/b2_start_large_file"
		headers = {
			'Authorization': self.token,
			'Content-Type': 'application/json',
		}
		data = {
			'fileName': remote_path,
			'contentType': mimetypes.guess_type(path)[0] or 'application/octet-stream',
			'bucketId': self.config['bucket_id'],
		}
		response = self._make_request(url, headers=headers, json=data, method=self.RequestMethod.POST, authenticate=False)
		if response.status_code == 200:
			# What we REALLY care about is the resulting "fileId"
			return response.json()
		else:
			raise Exception("Failed to start large file upload: " + response.text)


	def _get_chunk_count_for_file(self,path):
		'''
		How many chunks will we need to split a file, eg:
		file is 105 bytes and max_bytes_per_chunk is 20
		we will need 105/20 = 6
		'''
		file_size = os.path.getsize(path)
		return math.ceil(file_size / self.max_bytes_per_chunk)

	def _upload_chunk(self,chunk,chunks,path_src,url,upload_part_data):
		buffer_len = 1024 * 1024 * 100 # 10mB
		content = None
		with open(path_src, 'rb', buffering=buffer_len) as file:
			file.seek(chunk * self.max_bytes_per_chunk)
			content = file.read(self.max_bytes_per_chunk)

		part_sha = self._calculate_sha1(content)
		#shas.append(part_sha)

		print(f"Uploading chunk {chunk+1}/{chunks} size: {len(content)} bytes sha1: {part_sha}")

		headers = {
			'Authorization': upload_part_data['authorizationToken'],
			#'Content-Type': 'application/octet-stream',
			'Content-Length': str(len(content)),
			#'X-Bz-File-Id': file_id,
			'X-Bz-Part-Number': str(chunk + 1),
			'X-Bz-Content-Sha1': part_sha,
		}
		response = self._make_request(url, headers=headers, data=content, method=self.RequestMethod.POST, authenticate=False)

		result = None
		if response.status_code == 200:
			#results.append(response.json())
			result = response.json()
		else:
			#raise Exception("Failed to upload file: " + response.text)
			#result = None
			print(f"Failed to upload chunk {chunk+1}/{chunks}: {response.text}")
			pass

		return(chunk,part_sha,result)


	def _upload_large_file(self,path_src, path_dst):
		'''
		To upload a large file in B2 it's done in stages:
		b2_start_large_file
		b2_get_upload_part_url
		b2_upload_part
		b2_finish_large_file
		'''

		self._authenticate()

		upload_data = self._start_large_file_upload(path_src, path_dst)
		file_id = upload_data['fileId']

		'''
		Now that we're here we need to split the discrete files into chunks
		We won't physically split the file, but we have to read in X bytes
		and submit them in a loop
		'''

		buffer_len = 1024 * 1024 * 100 # 10mB

		chunks = self._get_chunk_count_for_file(path_src)

		upload_part_data = None
		url = None

		# if we're uploading in parallel we need to request a custom upload url for each chunk
		#if self.max_upload_single_threads <= 1:
		#	upload_part_data = self.GetUploadPartKey(file_id)
		#	url = upload_part_data['uploadUrl']

		print(f"Uploading large file at  {path_src} => {path_dst} chunks: {chunks} fileId: {file_id}")

		shas = []
		results = []

		# Now we know how many iterations we need to take
		with ThreadPoolExecutor(max_workers=self.max_upload_single_threads) as executor:
			futures = []
			for chunk in range(chunks):
				'''
				content = None
				with open(path, 'rb', buffering=buffer_len) as file:
					file.seek(chunk * self.max_bytes_per_chunk)
					content = file.read(self.max_bytes_per_chunk)

				part_sha = self.CalculateSha1(content)
				shas.append(part_sha)

				print(f"Uploading chunk {chunk+1}/{chunks} size: {len(content)} bytes sha1: {part_sha}")

				headers = {
					'Authorization': upload_part_data['authorizationToken'],
					#'Content-Type': 'application/octet-stream',
					'Content-Length': str(len(content)),
					#'X-Bz-File-Id': file_id,
					'X-Bz-Part-Number': str(chunk + 1),
					'X-Bz-Content-Sha1': part_sha,
				}
				response = self.MakeRequest(url, headers=headers, data=content, method=self.RequestMethod.POST, authenticate=False)
				if response.status_code == 200:
					results.append(response.json())
				else:
					raise Exception("Failed to upload file: " + response.text)
				'''
				upload_part_data = self._get_upload_part_key(file_id)
				url = upload_part_data['uploadUrl']
				futures.append(executor.submit(self._upload_chunk,chunk,chunks,path_src,url,upload_part_data))
			#wait(futures)

			results = [None] * chunks
			shas = [None] * chunks
			for future in as_completed(futures):
				chunk,part_sha,result = future.result()
				if result is not None:
					results[chunk] = result
					shas[chunk] = part_sha
				else:
					print(f"Failed to upload chunk {chunk+1}/{chunks}")
					raise Exception("Failed to upload chunk")

		# Now that we're done we need to trip the finish call
		print(f"Tripping finish for file: {file_id} with {shas}")
		self._finish_large_file(file_id, shas)

		# If we got here we're done

	def upload(self, path_root: Union[str, List[str]], destination_root:str):
		'''
		Uploads a file or folder to the bucket

		Args:
			path_root (str or List[str]): The local file or folder to upload
			destination_root (str): The root folder in the bucket to upload to

		Returns:
			bool: True if the upload was successful, False otherwise.

		Remarks:
			Let's say we have a folder called "logs" and inside it we have "2026/log1.txt" and "2026/log2.txt"

			if path_root = "./logs" or "./logs/" and destination_root = "backups"
				We will create the files:
					backups/logs/2026/log1.txt
					backups/logs/2026/log2.txt
			if path_root = "./logs/2026" and destination_root = "backups"
				We will create the files:
					backups/2026/log1.txt
					backups/2026/log2.txt
			if path_root = "./logs/2026/log1.txt" and destination_root = "backups"
				We will create the files:
					backups/log1.txt

			Notice that if path_root is a directory we will include that directory name in the upload path


		'''
		self._auto_authenticate()

		# if something like b2://foo/bar is passed in we want to just get "foo/bar"
		destination_root = self._strip_protocol_from_path(destination_root)
		destination_root = destination_root.rstrip('/').replace('\\', '/')

		# we support both str and list of strings, convert these to list of strings
		path_roots = []
		if isinstance(path_root, str):
			path_roots = [path_root]
		else:
			path_roots = path_root

		# convert all the paths into tuples of (path_src,path_dst)

		uploads = []

		result = []

		for path_root in path_roots:
			# Find all the files
			recursive = False
			path_root_original = path_root
			path_root_original_abs = os.path.abspath(path_root_original)
			last_original_dir = ""
			paths =  []

			if os.path.isdir(path_root):

				# convert this to a recursive lookup
				recursive = True
				last_original_dir = os.path.basename(path_root_original_abs)

				# If it's a directory, we need to find all files in it
				path_root = os.path.join(path_root, '**', '*')
				paths = glob.glob(path_root, recursive=True)
			else:
				# If it's a file, we just use it directly
				paths = [path_root]

			path_root_abs = os.path.abspath(path_root_original)

			for path in paths:
				if os.path.isdir(path):
					continue
				#print(f"{path} => {destination_root}/{path}")

				path_abs = os.path.abspath(path)

				path_src = path
				path_dst = ""

				# if our og path is a directory include that directory in the upload path
				if recursive == True:
					path_dst = destination_root + '/' + last_original_dir + '/' + self._remove_prefix(path_abs, path_root_abs).replace('\\', '/').lstrip('/')
				else:
					if path_root_abs == path_root_original_abs:
						path_dst = destination_root + '/' + os.path.basename(path)
					else:
						path_dst = destination_root + '/' + self._remove_prefix(path_abs, path_root_abs).replace('\\', '/').lstrip('/')

				uploads.append((path_src,path_dst))


		with ThreadPoolExecutor(max_workers=self.max_upload_threads) as executor:
			futures = []

			for path_src, path_dst in uploads:

				# if a single file is larger than our limit we have to use the large file upload instead
				future:Future
				if os.path.getsize(path_src) > self.large_file_upload_limit:
					# Use the large file upload API
					#file = self._upload_large_file(path_src, path_dst)
					future = executor.submit(self._upload_large_file, path_src, path_dst)
				else:
					#file = self._upload_file(path_src, path_dst, upload_key=upload_key)
					future = executor.submit(self._upload_file, path_src, path_dst)

				futures.append((future,(path_src,path_dst)))
			for future, args in futures:
				try:
					file = future.result()
					result.append(file)
				except Exception as e:
					path_src, path_dst = args
					print(f"Error uploading {path_src}: {e}")
					if self.failsafe_copy:
						print(f"Using failsafe copy for {path_src} => {self.failsafe_copy}")
						try:
							shutil.copy2(path_src,self.failsafe_copy)
						except Exception as e:
							print(f"Error copying {path_src} to {self.failsafe_copy}: {e}")
							return False
					else:
						return False
		return result

	def _write_file_to(self, destination_root, result, file_path=None):
		'''
		Write the downloaded file to the specified destination root
		'''
		if file_path == None:
			if destination_root != None:
				file_path = os.path.join(destination_root, result['fileName'])

		buffer_len = 1024 * 1024 * 10 # 10mB

		if file_path != None:
			os.makedirs(os.path.dirname(file_path), exist_ok=True)
			with open(file_path, 'wb', buffering=buffer_len) as file:
				file.write(result['content'])
				#print(f"File downloaded to: {file_path}")

				if self.download_sync_mtime:
					# we have to manually close it if we're going to sync the mtime
					file.close()
					# If set, attempt to sync the modified time to the remote file's time
					remote_mtime_ms = int(result.get('uploadTimestamp'))
					remote_mtime = remote_mtime_ms / 1000
					if remote_mtime > 0:
						os.utime(file_path, (remote_mtime,remote_mtime))
				return True

		return False

	def download_by_name(self, name, destination_root=None,destination_path=None,with_txt=False, start=None, end=None, write_to_disk=True):
		self._auto_authenticate()
		url = f"{self.root_url}/file/{self.config['bucket_name']}/{name}"

		headers = {}

		if start != None and end != None:
			headers['Range'] = f"bytes={start}-{end}"

		response = self._make_request(url, method=self.RequestMethod.GET, authenticate=True, headers=headers)

		headers = response.headers
		text = None
		if with_txt:
			text = response.text

		result = {
			'contentType': headers.get('Content-Type', 'application/octet-stream'),
			'fileName': headers.get('X-Bz-File-Name', name),
			'contentLength': headers.get('Content-Length', '0'),
			'raw': text,
			'content': response.content if 'Content-Length' in headers else None,
		}

		if write_to_disk:
			self._write_file_to(destination_root,result,file_path=destination_path)

		return result

	def download_by_key(self, key, path_dst=None, with_txt=False, start=None, end=None, write_to_disk=True):
		'''
		If you already have the file ID, you can download it directly instead of using the file name
		avoid with_txt as it is VERY slow

		if start and end are set it will use Range and get a partial download
		'''
		self._auto_authenticate()
		url = f"{self.base_url}/b2_download_file_by_id?fileId={key}"

		headers = {}

		if start != None and end != None:
			headers['Range'] = f"bytes={start}-{end}"

		response = self._make_request(url, method=self.RequestMethod.GET, authenticate=True, headers=headers)

		headers = response.headers
		text = None
		if with_txt:
			text = response.text

		result = {
			'contentType': headers.get('Content-Type', 'application/octet-stream'),
			'fileName': headers.get('X-Bz-File-Name', key),
			'fileId': key,
			'contentLength': headers.get('Content-Length', '0'),
			'raw': text,
			'content': response.content if 'Content-Length' in headers else None,
			'uploadTimestamp': headers.get('X-Bz-Upload-Timestamp', '0'),
		}

		if write_to_disk:
			self._write_file_to(None,result,file_path=path_dst)

		return result

	def _get_file_versions(self, path):
		'''
		Get all versions of a file with the given path

		Args:
			path (str): The path to the file to get versions for.

		Returns:
			dict: The result of the search operation, including all versions of the file.
		'''
		self._auto_authenticate()
		url = f"{self.base_url}/b2_list_file_versions?bucketId={self.config['bucket_id']}&prefix={self._quote(path)}&maxFileCount=10000"
		response = self._make_request(url, method=self.RequestMethod.GET, authenticate=True)
		if response.status_code == 200:
			return response.json()
		else:
			raise Exception("Failed to get file versions: " + response.text)

	def delete(self, path: Union[str, List[str]], recurse=False, all_versions=True):
		'''
		Delete file(s) and folder(s) matching the prefix path

		Args:
			path (str or List[str]): The file or folder to delete at this prefix
			recurse (bool): If true, will delete all files in the directory if path is a directory, for safety this is not on by default
			all_versions (bool): If true, will delete all versions of the file(s), not just the latest version.

		Returns:
			int: The number of files deleted
		'''

		paths = [path] if isinstance(path, str) else path

		self._auto_authenticate()

		to_delete = []

		url = f"{self.base_url}/b2_delete_file_version"

		count = 0

		for path in paths:
			path = self._strip_protocol_from_path(path)

			# when delete a file on b2, we need the fileid, which we can only get by querying for it

			files = self.search(prefix=path, recurse=recurse)
			if len(files['files']) <= 0:
				return count

			for file in files['files']:
				if all_versions == True:
					# query for all file versions and add them to the delete list
					versions = self._get_file_versions(file['fileName'])
					for version in versions['files']:
						to_delete.append({
							"fileName": file['fileName'], "fileId": version['fileId']
						})
				else:
					to_delete.append({
						"fileName": file['fileName'], "fileId": file['fileId']
					})

		unique_files_deleted = set()
		for data in to_delete:
			if data['fileId'] is None:
				continue

			response = self._make_request(url, method=self.RequestMethod.POST, authenticate=True, json=data)
			response_data = response.json() if response else None

			if response_data != None and response_data['fileId'] == data['fileId']:
				if data['fileName'] not in unique_files_deleted:
					# if we have multiple versions, just print one delete statement per filename
					print(f"Deleted {data['fileName']}")
					unique_files_deleted.add(data['fileName'])
					count += 1
			else:
				print(f"Failed to delete file: {data['fileName']} with id: {data['fileId']} => {response_data}")

		return count


	def download(self, prefix: Union[str, List[str]], destination_root=None, include=None, min_size=None, max_size=None, recurse=True, preserve_dir_prefix=False):
		'''
		Downloads files matching the prefix, if prefix is a directory it will download all files in that directory, if it's a file it will just download that file

		Args:
			prefix (str or List[str]): The prefix or list of prefixes to search for.
			include (str or List[str]): If set, only files that include this string will be downloaded.
			min_size (int): If set, only files larger than this size (in bytes) will be downloaded.
			max_size (int): If set, only files smaller than this size (in bytes) will be downloaded.
			recurse (bool): If true, will search for files in subdirectories as well.
			destination_root (str): The local directory to download the files to. If not set, files will be downloaded to the current directory.
			preserve_dir_prefix (bool): If true, will preserve the relative path after the 'prefix' for the local files:
				eg:
					preserve_dir_prefix = True, prefix = "2026/logs/123.txt", destination_root="./downloads" will create "./downloads/2026/logs/123.txt"
					preserve_dir_prefix = True, prefix = "2026/logs", destination_root="./downloads" will create "./downloads/2026/logs/123.txt"
					preserve_dir_prefix = True, prefix = "2026", destination_root="./downloads" will create "./downloads/2026/logs/123.txt"
					preserve_dir_prefix = False, prefix = "2026/logs/123.txt", destination_root="./downloads" will create "./downloads/123.txt"
					preserve_dir_prefix = False, prefix = "2026/logs", destination_root="./downloads" will create "./downloads/logs/123.txt"
					preserve_dir_prefix = False, prefix = "2026", destination_root="./downloads" will create "./downloads/2026/logs/123.txt"

		Returns:
			list: A list of local file paths that were downloaded.
		'''
		self._auto_authenticate()

		destination_root = destination_root.rstrip('/').replace('\\', '/') if destination_root else destination_root

		result = []

		prefixes = []
		if isinstance(prefix, str):
			prefixes = [prefix]
		elif isinstance(prefix, list):
			prefixes = prefix

		global_add_back_last_prefix_folder = True
		#if destination_root != None and not os.path.isdir(destination_root):
			# if the output folder doesn't exist, treat it like cp in the sense that we will not nest a new folder inside of it
			#global_add_back_last_prefix_folder = False

		files = {"files": []}

		for prefix in prefixes:

			prefix = self._strip_protocol_from_path(prefix)

			add_back_last_prefix_folder = global_add_back_last_prefix_folder

			if prefix.endswith('*'):
				# if the prefix ends with a * we want to search for the prefix without the *
				prefix = prefix[:-1]
				# treat it like cp in the sense that we would glob and not create the last folder of the prefix
				add_back_last_prefix_folder = False

			'''
			This works for both a file or directory
			'''
			include_dirs = True
			include_files = True
			tmp_files = self.search(prefix=prefix, include=include, min_size=min_size, max_size=max_size,include_dirs=include_dirs, include_files=include_files, recurse=recurse)

			for idx,file in enumerate(tmp_files['files']):
				path_dst = None
				if preserve_dir_prefix:
					# we want to preserve the relative path after the prefix, so we need to calculate that and store it for later when we do the download
					path_dst = destination_root + '/' + file['fileName']
				else:
					# if we have something like prefix = "2026" and the file is "2026/logs/123.txt" we want destination_root + "/2026/logs/123.txt" since "logs" is a dir
					if file['fileName'] == prefix:
						# we've requested the exact file
						path_dst = destination_root + '/' + os.path.basename(file['fileName'])
					else:
						relative_path = file['fileName'][len(prefix):].lstrip('/')
						# now we'll have something like logs/123.txt

						# add back the "last" folder of the prefix
						last_prefix_folder = os.path.basename(prefix.rstrip('/'))
						if add_back_last_prefix_folder and last_prefix_folder != '' and relative_path != '':
							relative_path = last_prefix_folder + '/' + relative_path

						path_dst = destination_root + '/' + relative_path

				#print(f"download: {file['fileName']} => {path_dst}")


				tmp_files['files'][idx]['path_dst'] = path_dst

			files['files'].extend(tmp_files['files'])

		jobs_total = len(files['files'])
		jobs_completed = 0

		if len(files['files']) <= 0:
			print(f"No files found for path: {prefix}")

		result = []

		with ThreadPoolExecutor(max_workers=self.max_download_threads) as executor:

			futures_to_args = {}

			futures = []

			for file in files['files']:

				# .bzEmpty files are special holders to keep a folder "open" without files
				if file['fileName'].endswith('.bzEmpty'):
					continue

				# where we do want to download this to?
				path_dst = file['path_dst']

				future = executor.submit(self.download_by_key, file['fileId'], path_dst=path_dst, write_to_disk=True)

				futures_to_args[future] = (file,path_dst)

				futures.append(future)


				#fetched = self.DownloadByKey(file['fileId'],destination_root=destination_root)
				#print(f"Downloaded file: {fetched['fileName']}, Content Type: {fetched['contentType']}, Size: {fetched['contentLength']} bytes")
				#print(f"{destination_root}{fetched['fileName']}")
			try:
				for future in as_completed(futures):
					jobs_completed += 1
					try:
						fetched = future.result()
						result.append(fetched)
						file, path_dst = futures_to_args[future]
						print(f"Downloaded [{jobs_completed}]/[{jobs_total}] => {path_dst}")
						result.append(path_dst)
					except Exception as e:
						print(f"Error downloading file")
						print(traceback.format_exc())
			except KeyboardInterrupt:
				print(f"User canceled downloads")
				executor.shutdown(wait=False,cancel_futures=True)
				#stop_event.set()
			wait(futures)

		return result


	def _search(self,prefix:str='',next_file_name=None,max_file_count=1000, recurse=True):
		'''
		Requests a list of files, can be paginated using next_file_name
		'''
		url = f"{self.base_url}/b2_list_file_names?bucketId={self.config['bucket_id']}"
		if prefix != None and len(prefix) > 0:
			url += f"&prefix={self._quote(prefix)}"
		if recurse == False:
			url += f"&delimiter={self._quote('/')}"

		url += f"&maxFileCount={max_file_count}"
		if next_file_name != None:
			url += f"&startFileName={next_file_name}"

		response = self._make_request(url, method=self.RequestMethod.GET, authenticate=True)
		if response.status_code == 200:
			return response.json()
		else:
			raise Exception("Failed to list files: " + response.text)
		return None

	def search(self,prefix:Union[str,List[str]]='',include=None,min_size=None,max_size=None,include_dirs=True,include_files=True, recurse=True):
		self._auto_authenticate()

		prefixes = []
		if isinstance(prefix, str):
			prefixes = [prefix]
		elif isinstance(prefix, list):
			prefixes = prefix

		results = {'files': []}
		for prefix in prefixes:
			prefix = self._strip_protocol_from_path(prefix)
			'''
			Records look like:
			{
				'accountId': 'b65cdab40c0e',
				'action': 'upload',
				'bucketId': 'db86658c7dba8ba4908c001e',
				'contentLength': 1590771,
				'contentMd5': '3c31591b1105a955fc4fc8156b4a541e',
				'contentSha1': 'f45fd9f6aa60def60bf2facef3685ff7c8ad18fe',
				'contentType': 'application/x-zip-compressed',
				'fileId': '4_zdb86658c7dba8ba4908c001e_f1153a53c31fdc903_d20250828_m185809_c002_v0203007_t0055_u01756407489603',
				'fileInfo': {},
				'fileName': 'PRERUN2_DONE/fig_base_real_m_historical_tightfull_032329.zip',
				'fileRetention': {'isClientAuthorizedToRead': False, 'value': None},
				'legalHold': {'isClientAuthorizedToRead': False, 'value': None},
				'serverSideEncryption': {'algorithm': None, 'mode': None},
				'uploadTimestamp': 1756407489603
			}
			'''
			nextFileName = None
			result = {'files': [], 'nextFileName': nextFileName}
			attempt = 0
			considered = 0
			while True:
				attempt += 1
				if attempt > 100:
					print(f"Safety break on file list, exceeded max attempts of 100")
					break

				response = self._search(prefix=prefix, next_file_name=nextFileName, recurse=recurse)
				if response is not None:

					considered += len(response['files'])
					for file in response['files']:

						if include != None:
							if not re.search(include, file['fileName']):
								continue

						if min_size != None:
							if file['contentLength'] <= min_size:
								continue

						if max_size != None:
							if file['contentLength'] >= max_size:
								continue


						if not include_dirs:
							if file['action'] == 'folder':
								continue
						if not include_files:
							if file['action'] == 'upload':
								continue

						result['files'].append(file)

					if nextFileName == response.get('nextFileName'):
						# we have reached the end of the list
						break

					nextFileName = response.get('nextFileName')
					result['nextFileName'] = nextFileName

					if not result['nextFileName'] or len(response['files']) <= 0:
						# if we had no files in the list terminate our loop
						break

			#print(f"Considered: {considered} final count: {len(result['files'])}")
			results['files'].extend(result['files'])

		return results

	def list_buckets(self):
		self._auto_authenticate()
		url = f"{self.base_url}/b2_list_buckets"
		payload = {
			'accountId': self.accountId,
			'bucketId': self.config['bucket_id'], # we need to specify a bucket with a bucket restricted key
		}
		response = self._make_request(url, json=payload, method=self.RequestMethod.POST, authenticate=True)
		if response.status_code == 200:
			return response.json()
		else:
			raise Exception("Failed to list buckets: " + response.text)
		return None

	def set_failsafe_copy(self,path):
		# set to a path if you want to use a failsafe copy for when upload fails
		self.failsafe_copy = path

	def get_download_url(self,path:Union[str,List[str]],expiration_seconds=60*60):
		'''
		Generates a URL that is accessible for the specified duration, this file can then be downloaded with a simple GET request
		This is useful to expose a file from a private bucket to a client, such as in a browser
		Note: it's possible to generate a url for a file that doesn't exist
		'''
		self._auto_authenticate()
		paths = [path] if isinstance(path, str) else path

		# these are limits from B2's api, see: https://www.backblaze.com/apidocs/b2-get-download-authorization
		if expiration_seconds > 604800:
			raise Exception("Expiration seconds cannot be greater than 7 days (604800 seconds)")
		if expiration_seconds < 1:
			raise Exception("Expiration seconds must be >= 1")

		url = f"{self.base_url}/b2_get_download_authorization"

		results = []

		for path in paths:
			path = self._strip_protocol_from_path(path)

			payload = {
				"bucketId": self.config['bucket_id'],
				"fileNamePrefix": path,
				"validDurationInSeconds": expiration_seconds,
			}
			response = self._make_request(url,json=payload, method=self.RequestMethod.POST, authenticate=True)
			if response.status_code == 200:
				temp = response.json()

				download_url = f"{self.download_url}/file/{self.config['bucket_name']}/{path}?Authorization={temp['authorizationToken']}"
				results.append(download_url)

			else:
				raise Exception("Failed to get download URL: " + response.text)

		return results
