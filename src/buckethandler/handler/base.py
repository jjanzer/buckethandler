import json
import os
import shutil
import glob
import math
import threading
import traceback
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Union, List

class BaseHandler():
	def __init__(self, config):
		if isinstance(config, str):
			config = self._load_config_file(config)

		self.config = config
		self.thread_local = threading.local()

		self.max_retries = 5
		self.max_download_threads = 8
		self.max_upload_threads = 4
		self.ideal_send_size = 10 * 1024 * 1024 # 4MB, optionally set when uploading large buffers, this is the value we set for buffers
		self.max_bytes_per_chunk = 100 * 1024 * 1024  # 100 MB
		# this is likely to be redefined by each handler, eg: s3 has a limit of 5GB but b2 has a limit of 1GB for non-part uploads
		self.large_file_upload_limit = 1024 * 1024 * 1024 # 1GB limit
		# set to a path if you want to use a failsafe copy for when upload fails
		self.failsafe_copy = None # example: "./staging_upload/"
		# when uploading a large file how many threads should we use per file
		self.max_upload_single_threads = 32

		# if set it will attempt to set the modified time to remote file's time
		self.download_sync_mtime = True

	def _remove_prefix(self, text:str, prefix:str) -> str:
		# python 3.9 has "removeprefix" but we might not be on that version
		if text.startswith(prefix):
			return text[len(prefix):]
		return text

	def _load_config_file(self, path):
		'''
		Load the configuration from a file.

		Args:
			path (str): The path to the configuration file.

		Returns:
			dict: The loaded configuration.
		'''
		with open(path, "r") as f:
			return json.load(f)

	def _strip_protocol_from_path(self,path):
		raise NotImplementedError("This method should be implemented by the specific handler")

	def _get_chunk_count_for_file(self,path):
		'''
		How many chunks will we need to split a file, eg:
		file is 105 bytes and max_bytes_per_chunk is 20
		we will need 105/20 = 6
		'''
		file_size = os.path.getsize(path)
		return math.ceil(file_size / self.max_bytes_per_chunk)

	def _quote(self,path):
		'''
		Handles percent encoding for paths as B2 requires this
		'''
		# Note we DO allow "/" symbol
		return urllib.parse.quote(path)

	def set_max_download_threads(self,max_threads):
		self.max_download_threads = max_threads

	def set_max_upload_threads(self,max_threads):
		self.max_upload_threads = max_threads

	def set_max_upload_single_threads(self,max_threads):
		self.max_upload_single_threads = max_threads

	def set_failsafe_copy(self,path):
		# set to a path if you want to use a failsafe copy for when upload fails
		self.failsafe_copy = path

	def search(self,prefix:Union[str,List[str]]='',include=None,min_size=None,max_size=None,include_dirs=True,include_files=True, recurse=True):
		raise NotImplementedError("Search method not implemented for this handler")

	# individual handlers should implement these methods
	def _upload_file(self,path_src:str,path_dst:str, upload_key=None):
		raise NotImplementedError("Upload method not implemented for this handler")
	def _start_large_file_upload(self, path_src:str, path_dst:str):
		raise NotImplementedError("Start large file upload method not implemented for this handler")
	def _finish_large_file(self, path_dst:str, file_id, shas):
		raise NotImplementedError("Finish large file upload method not implemented for this handler")
	def _upload_chunk(self,chunk,chunks,path_src:str,url,upload_part_data):
		raise NotImplementedError("Chunk upload method not implemented for this handler")
	def _upload_large_file(self,path_src:str, path_dst:str):
		raise NotImplementedError("Large file upload method not implemented for this handler")

	def _download_paths(self, prefix: Union[str, List[str]], destination_root=None, include=None, min_size=None, max_size=None, recurse=True, preserve_dir_prefix=False):
		'''
		Builds a list of src/dst paths for downloading direct files into
		'''
		destination_root = destination_root.rstrip('/').replace('\\', '/') if destination_root else destination_root

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
		return files

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
		raise NotImplementedError("Download method not implemented for this handler")

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

				# if we have no destination root don't upload a file to "/log.txt" but instead "log.txt"
				path_dst = path_dst.lstrip('/')

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
					traceback.print_exc()
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

