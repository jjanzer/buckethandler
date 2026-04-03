import os
import glob
import re
import shutil
import json
import hashlib
import hmac
import datetime
import time
import requests
try:
	import httpx
except ImportError:
	httpx = None
import io
import urllib.parse
import xml.etree.ElementTree as ET
from enum import Enum
from typing import List, Union
from importlib_metadata import files
from concurrent.futures import ProcessPoolExecutor,ThreadPoolExecutor,wait,as_completed,Future

'''
# These were tried but had little to no effect
import http.client
http.client.HTTPConnection.__init__.__defaults__ = tuple(
	x if x != 8192 else 10 * 1024 * 1024
	for x in http.client.HTTPConnection.__init__.__defaults__
)
import socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024 * 10) # 10MB send buffer
'''

from .base import BaseHandler

class S3Handler(BaseHandler):
	'''
	Handles bucket requests for S3 and S3 compatible services

	Official API calls are here: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations_Amazon_Simple_Storage_Service.html
	'''

	class RequestMethod(Enum):
		GET = 1
		POST = 2
		PUT = 3
		DELETE = 4

	def __init__(self, config):
		super().__init__(config)
		self.large_file_upload_limit = 5 * 1024 * 1024 * 1024 # 5GB limit
		self.large_file_upload_limit = 5 * 1024 * 1024 # testing
		self.large_file_upload_limit_min= 5 * 1024 * 1024 # 5MB, this is a hard requirement from s3

	def _strip_protocol_from_path(self,path:str) -> str:
		if path[:5].lower().startswith('s3://'):
			return path[5:]
		return path

	def _is_authenticated(self):
		#return self.token is not None
		pass
	def _authenticate(self):
		pass
	def _auto_authenticate(self):
		if not self._is_authenticated():
			return self._authenticate()
		return None

	def _sign(self, key, msg):
		return hmac.new(key, msg.encode(), hashlib.sha256).digest()
	def _get_signature(self, key, date_stamp,region,service='s3'):
		# V4 signature
		k_date = self._sign(("AWS4" + key).encode(), date_stamp)
		k_region = self._sign(k_date, region)
		k_service = self._sign(k_region, service)
		k_signing = self._sign(k_service, "aws4_request")
		return k_signing


	def _get_amz_datetime(self):
		t = datetime.datetime.utcnow()
		return t.strftime('%Y%m%dT%H%M%SZ')
	def _get_amz_date(self):
		t = datetime.datetime.utcnow()
		return t.strftime('%Y%m%d')

	def _make_request(self, params:dict={}, path:str="/", headers={}, data=None, json=None, method=RequestMethod.GET, payload_hash:str=''):
		# for the path we want to only use the path of the url not query params
		service = 's3'

		bucket = None
		if 'bucket_name' in self.config:
			bucket = self.config['bucket_name']
		else:
			raise Exception("No bucket_name found in config")

		key = None
		if 'secret_key' in self.config:
			# s3 style
			key = self.config['secret_key']
		elif 'application_key' in self.config:
			# b2 style
			key = self.config['application_key']
		else:
			raise Exception("No secret_key or application_key found in config")

		access_key = None
		if 'access_key' in self.config:
			access_key = self.config['access_key']
		elif 'account_key' in self.config:
			access_key = self.config['account_key']
		else:
			raise Exception("No access_key or account_key found in config")

		region = None
		if 'region' in self.config:
			region = self.config['region']
		else:
			raise Exception("No region found in config")

		method_str = method.name if isinstance(method, self.RequestMethod) else str(method)
		host = f"{bucket}.s3.{region}.backblazeb2.com"

		amz_date = self._get_amz_datetime()
		date_stamp = self._get_amz_date()

		# This is mostly lifted from the boto3 implementation

		canonical_querystring = "&".join(
			f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(str(v), safe='')}"
			for k, v in sorted(params.items())
		)

		if len(payload_hash) == 0:
			# if we didn't provide a hash calculate one
			if data:
				payload_hash = hashlib.sha256(data if isinstance(data, bytes) else data.encode()).hexdigest()
			elif json:
				payload_hash = hashlib.sha256(json.dumps(json).encode()).hexdigest()
			else:
				payload_hash = hashlib.sha256(b"").hexdigest()

		canonical_headers = f"host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n"
		signed_headers = "host;x-amz-content-sha256;x-amz-date"

		canonical_uri = path
		canonical_request = f"{method_str}\n{canonical_uri}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"

		algorithm = "AWS4-HMAC-SHA256"
		credential_scope = f"{date_stamp}/{region}/{service}/aws4_request"
		string_to_sign = f"{algorithm}\n{amz_date}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode()).hexdigest()}"

		signing_key = self._get_signature(key, date_stamp, region=region, service=service)
		signature = hmac.new(signing_key, string_to_sign.encode(), hashlib.sha256).hexdigest()

		auth_header =  f"{algorithm} Credential={access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"

		extra_headers = {
			'host': host,
			'x-amz-content-sha256': payload_hash,
			'x-amz-date': amz_date,
			'Authorization': auth_header,
		}

		size = 0

		if 'Content-Length' not in headers and data is not None:
			size = len(data) if isinstance(data, bytes) else len(data.encode())
			extra_headers['Content-Length'] = str(size)

		response = None
		response_status_code = None

		base_url = f"https://{host}"

		url = f"{base_url}{path}?{canonical_querystring}"

		headers_combined = headers.copy() if headers else {}
		headers_combined.update(extra_headers)

		#buf = io.BytesIO(data) if isinstance(data, bytes) else data

		# Used to optimize the send chunk size
		send_buffer_size = self.ideal_send_size
		class FastBytesIO(io.BytesIO):
			def read(self, n=-1):
				# Force X writes
				if n < 0 or n > send_buffer_size:
						n = send_buffer_size
				return super().read(n)

		for attempt in range(self.max_retries):
			if method == self.RequestMethod.GET:
				response = requests.get(url, headers=headers_combined)
			elif method == self.RequestMethod.POST:
				#response = requests.post(url, headers=headers_combined,data=data,json=json)
				if httpx:
					buf = FastBytesIO(data) if isinstance(data, bytes) else data
					timeout = httpx.Timeout(60.0, connect=60.0, read=60.0, write=60.0)
					client = httpx.Client(timeout=timeout)
					response = client.post(url, headers=headers_combined,content=buf,json=json)
					#response = client.put(url, headers=headers_combined,content=data,json=json)
				else:
					response = requests.post(url, headers=headers_combined,data=data,json=json)
			elif method == self.RequestMethod.PUT:
				if httpx:
					buf = FastBytesIO(data) if isinstance(data, bytes) else data
					timeout = httpx.Timeout(60.0, connect=60.0, read=60.0, write=60.0)
					client = httpx.Client(timeout=timeout)
					response = client.put(url, headers=headers_combined,content=buf,json=json)
					#response = client.put(url, headers=headers_combined,content=data,json=json)
				else:
					response = requests.put(url, headers=headers_combined,data=data,json=json)
			elif method == self.RequestMethod.DELETE:
				response = requests.delete(url, headers=headers_combined,data=data,json=json)
			else:
				raise Exception("Unsupported request method")

			response_status_code = response.status_code if response else None

			if response.status_code == 200 or response.status_code == 206 or response.status_code == 404:
				return response

			if response.status_code == 400:
				print(f"Bad request (attempt {attempt+1}/{self.max_retries}): {response.status_code} => {url}")
				print(response.text)
				return response

			if response.status_code == 403:
				print(f"Forbidden (attempt {attempt+1}/{self.max_retries}): {response.status_code} => {url}")
				print(response.text)
				return response

			print(f"Request failed (attempt {attempt+1}/{self.max_retries}): {response.status_code} => {url}")
			time.sleep(2 ** attempt)

		raise Exception(f"Failed to make request after retries: {response_status_code}")


	def _search(self,prefix:str='',continuation_token=None,max_file_count=1000,recurse=True) -> dict:
		'''
		Internal lookup for listing items in a bucket path

		Args:
			prefix: the path to list, e.g. "foo/bar/"
			continuation_token: if the listing is truncated, this token can be used to get the next page of results
			max_file_count: the maximum number of files to return
			recurse: whether to list files recursively or only one level deep (if false, it will only return files directly under the prefix, and return "folders" for subdirectories)

		Returns:
			A dict with a "files" key

		Remarks:
			https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
		'''

		params = {
			'list-type': '2',
			'prefix': prefix,
			'max-keys': str(max_file_count),
			'delimiter': '' if recurse else '/',
		}

		if continuation_token:
			params['continuation-token'] = continuation_token
		result = self._make_request(params,method=self.RequestMethod.GET)

		result_dict = {
			'files': []
		}

		if result.status_code == 200:

			root = ET.fromstring(result.text)

			for common_prefix in root.findall('.//{http://s3.amazonaws.com/doc/2006-03-01/}CommonPrefixes'):
				p = common_prefix.find('{http://s3.amazonaws.com/doc/2006-03-01/}Prefix').text

				result_dict['files'].append({
					'fileName': p,
					'contentLength': 0,
					'contentType': None,
					'ContentMD5': None,
					'etag': None,
					'uploadTimestamp': 0,
					'action': 'folder',
				})

			for contents in root.findall('.//{http://s3.amazonaws.com/doc/2006-03-01/}Contents'):
				key = contents.find('{http://s3.amazonaws.com/doc/2006-03-01/}Key').text
				size = int(contents.find('{http://s3.amazonaws.com/doc/2006-03-01/}Size').text)
				last_modified = contents.find('{http://s3.amazonaws.com/doc/2006-03-01/}LastModified').text
				etag = contents.find('{http://s3.amazonaws.com/doc/2006-03-01/}ETag').text

				# Convert last_modified to a timestamp in milliseconds
				last_modified_dt = datetime.datetime.strptime(last_modified, '%Y-%m-%dT%H:%M:%S.%fZ')
				last_modified_ts = int(last_modified_dt.timestamp() * 1000)

				result_dict['files'].append({
					'fileName': key,
					'contentLength': size,
					'contentType': None, # You have to make a separate request to get the content type, it's not included in the listing response
					'ContentMD5': None,
					'etag': etag, # this is sometimes the md5, but not always
					'uploadTimestamp': last_modified_ts,
					'action': 'upload',
				})


			if recurse:
				is_truncated = root.find('.//{http://s3.amazonaws.com/doc/2006-03-01/}IsTruncated').text == 'true'
				if is_truncated:
					next_continuation_token = root.find('.//{http://s3.amazonaws.com/doc/2006-03-01/}NextContinuationToken').text
					next_result = self._search(prefix=prefix,continuation_token=next_continuation_token,max_file_count=max_file_count,recurse=recurse)
					result_dict['files'].extend(next_result['files'])

		return result_dict


	def search(self,prefix:Union[str,List[str]]='',include=None,min_size=None,max_size=None,include_dirs=True,include_files=True, recurse=True) -> dict:
		#self._auto_authenticate()
		if isinstance(prefix, str):
			prefix = [prefix]


		result = {'files': []}

		for p in prefix:
			p = self._strip_protocol_from_path(p)

			cur_result = self._search(prefix=p, max_file_count=4, recurse=recurse)
			if cur_result == None:
				print(f"Search failed for prefix {p}")
				continue

			for file in cur_result['files']:
				if file['action'] == 'folder' and not include_dirs:
					continue
				if file['action'] == 'upload' and not include_files:
					continue
				if min_size is not None and file['contentLength'] < min_size:
					continue
				if max_size is not None and file['contentLength'] > max_size:
					continue
				if include != None:
					if not re.search(include, file['fileName']):
						continue

				result['files'].append(file)

		return result

	def _write_file_to(self, path_dst, result):
		'''
		Write the downloaded file to the specified destination root
		'''

		# use a 10MB buffer for performance
		buffer_len = 1024 * 1024 * 10 # 10mB

		if path_dst != None:
			os.makedirs(os.path.dirname(path_dst), exist_ok=True)
			with open(path_dst, 'wb', buffering=buffer_len) as file:
				file.write(result['content'])
				#print(f"File downloaded to: {path_dst}")

				if self.download_sync_mtime:
					# we have to manually close it if we're going to sync the mtime
					file.close()
					# If set, attempt to sync the modified time to the remote file's time
					remote_mtime_ms = int(result.get('uploadTimestamp'))
					remote_mtime = remote_mtime_ms / 1000
					if remote_mtime > 0:
						os.utime(path_dst, (remote_mtime,remote_mtime))
				return True

		return False

	def _download_by_path(self, path_src:str, path_dst:Union[str,None]=None, with_txt = False, start=None, end=None, write_to_disk=True):
		# this is a single file download, not to be confused with the "download" method which can download multiple files based on a prefix

		params = {
		}

		if not path_src.startswith('/'):
			path_src = '/' + path_src

		headers = {}
		if start != None and end != None:
			headers['Range'] = f"bytes={start}-{end}"

		response = self._make_request(params=params,path=path_src,method=self.RequestMethod.GET, headers=headers)
		headers = response.headers

		text = None
		if with_txt:
			text = response.text

		if response.status_code != 200:
			print(f"Failed to download {path_src}: {response.status_code}")

		mtime = headers.get('Last-Modified', None)
		# convert mtime to timestamp in milliseconds
		mtime_ts = 0
		if mtime:
			mtime_dt = datetime.datetime.strptime(mtime, '%a, %d %b %Y %H:%M:%S %Z')
			mtime_ts = int(mtime_dt.timestamp() * 1000)

		result = {
			'contentType': headers.get('Content-Type', 'application/octet-stream'),
			'fileName': os.path.basename(path_src),
			'fileId': None,
			'contentLength': headers.get('Content-Length', '0'),
			'raw': text,
			'content': response.content if 'Content-Length' in headers else None,
			'uploadTimestamp': mtime_ts,
		}

		if write_to_disk and path_dst is not None and len(path_dst) > 0 and response.status_code == 200:
			self._write_file_to(path_dst, result)

		return result

	def download(self, prefix: Union[str, List[str]], destination_root=None, include=None, min_size=None, max_size=None, recurse=True, preserve_dir_prefix=False):

		files = self._download_paths(prefix=prefix,destination_root=destination_root, include=include, min_size=min_size, max_size=max_size, recurse=recurse, preserve_dir_prefix=preserve_dir_prefix)
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

				future = executor.submit(self._download_by_path, path_src=file['fileName'], path_dst=path_dst)

				futures_to_args[future] = (file,path_dst)

				futures.append(future)
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

		destination_root = destination_root.rstrip('/').replace('\\', '/') if destination_root else destination_root

		result = []

		prefixes = []
		if isinstance(prefix, str):
			prefixes = [prefix]
		elif isinstance(prefix, list):
			prefixes = prefix

		files = {"files": []}

		# Gather the list of files we want to download and determine where they should be downloaded to
		for prefix in prefixes:
			prefix = self._strip_protocol_from_path(prefix)
			res = self._download_by_path(prefix, path_dst=destination_root)

		#with ThreadPoolExecutor(max_workers=self.max_download_threads) as executor:


	def _start_large_file_upload(self, path_src:str, path_dst:str) -> dict:
		# this is a single file upload, not to be confused with the "upload" method which can upload multiple files based on a prefix
		params = {
			'uploads': '1'
		}
		if not path_dst.startswith('/'):
			path_dst = '/' + path_dst
		url = path_dst

		response = self._make_request(params=params,path=url,method=self.RequestMethod.POST)
		if response.status_code == 200:
			root = ET.fromstring(response.text)
			upload_id = root.find('.//{http://s3.amazonaws.com/doc/2006-03-01/}UploadId').text
			return {
				'upload_id': upload_id,
			}
		else:
			print(f"Failed to start large file upload for {path_src} to {path_dst}: {response.status_code}")
			return {
				'upload_id': None,
			}

	def _finish_large_file(self, path_dst:str, file_id, shas):
		params = {
			'uploadId': file_id,
		}

		data = '<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'

		# sort the shas by chunk number
		shas.sort(key=lambda x: x['chunk'])

		for sha in shas:
			part_number = sha['chunk'] + 1
			etag = sha['sha']
			data += f'<Part><PartNumber>{part_number}</PartNumber><ETag>{etag}</ETag></Part>'
		data += '</CompleteMultipartUpload>'

		headers = {
			'Content-Type': 'application/xml',
		}
		response = self._make_request(params=params,path=path_dst,method=self.RequestMethod.POST,data=data,headers=headers)
		# The response contains a series of Part elements with ETag and PartNumber, we don't care about this data however
		if response.status_code == 200:
			return True
		else:
			print(f"Failed to finish large file upload for {path_dst}: {response.status_code}")
			return False

	def _upload_chunk(self,chunk,chunks,path_src,url,upload_part_data):
		buffer_len = 1024 * 1024 * 100 # 10mB
		content = None
		with open(path_src, 'rb', buffering=buffer_len) as file:
			file.seek(chunk * self.max_bytes_per_chunk)
			content = file.read(self.max_bytes_per_chunk)

		params = {
			'partNumber': chunk + 1,
			'uploadId': upload_part_data['upload_id'],
		}

		print(f"Uploading chunk {chunk+1}/{chunks} size: {len(content)} bytes")

		self.max_retries = 1
		response = self._make_request(params=params,path=url,method=self.RequestMethod.PUT,data=content)

		if response.status_code == 200:
			etag = response.headers.get('ETag', '').strip('"')
			# compare this etag with the md5 of the content we uploaded, if they don't match we should retry the upload of this chunk
			return {
				'success': True,
				'chunk': chunk,
				'sha': etag,
			}
		else:
			print(f"Failed to upload chunk {chunk} for {path_src}: {response.status_code}")
			return {
				'success': False,
				'chunk': chunk,
				'sha': None,
			}






	def _upload_large_file(self, path_src:str, path_dst:str):
		# this is a single file upload, not to be confused with the "upload" method which can upload multiple files based on a prefix
		upload_data = self._start_large_file_upload(path_src, path_dst)
		# we're using file_id as that is what we call it in the rest of our library
		file_id = upload_data['upload_id']

		buffer_len = 1024 * 1024 * 100 # 10mB

		chunks = self._get_chunk_count_for_file(path_src)
		if not path_dst.startswith('/'):
			path_dst = '/' + path_dst
		url = path_dst

		# if we're uploading in parallel we need to request a custom upload url for each chunk
		#if self.max_upload_single_threads <= 1:
		#	upload_part_data = self.GetUploadPartKey(file_id)
		#	url = upload_part_data['uploadUrl']

		print(f"Uploading large file at  {path_src} => {path_dst} chunks: {chunks} fileId: {file_id}")

		shas = []
		results = []

		'''
		for chunk in range(chunks):
			upload_part_data = upload_data
			result = self._upload_chunk(chunk, chunks, path_src, url, upload_part_data)
			print("OVER")
			return False
		'''

		with ThreadPoolExecutor(max_workers=self.max_upload_single_threads) as executor:
			futures = []
			for chunk in range(chunks):
				upload_part_data = upload_data
				future = executor.submit(self._upload_chunk, chunk, chunks, path_src, url, upload_part_data)
				futures.append(future)

			for future in as_completed(futures):
				result = future.result()
				results.append(result)
				if result['success']:
					shas.append(result)
				else:
					print(f"Chunk upload failed for {path_src} => {path_dst}")
					return False

		self._finish_large_file(path_dst, file_id, shas=shas)



	def _upload_file(self, path_src:str, path_dst:str, upload_key=None):
		# this is a single file upload, not to be confused with the "upload" method which can upload multiple files based on a prefix

		# uploads need the sha256 hash of the file content
		sha256_hash = ''
		with open(path_src, 'rb') as f:
			data = f.read()
			sha256_hash = hashlib.sha256(data).hexdigest()

		params = {
		}

		if not path_dst.startswith('/'):
			path_dst = '/' + path_dst

		with open(path_src, 'rb') as f:
			data = f.read()

		response = self._make_request(params=params,path=path_dst,method=self.RequestMethod.PUT,data=data,payload_hash=sha256_hash)

		if response.status_code == 200:
			return True
		else:
			print(f"Failed to upload {path_src} to {path_dst}: {response.status_code}")
			return False

	def upload(self, path_root: Union[str, List[str]], destination_root:str):
		return super().upload(path_root, destination_root)
