import os
import sys
import re
import traceback
import hashlib
from datetime import datetime, timedelta, timezone

try:
	import webbrowser
except ImportError:
	webbrowser = None

from typing import List, Union
from concurrent.futures import ThreadPoolExecutor,wait,as_completed

import requests

import dropbox
from dropbox.sharing import RequestedVisibility

from .base import BaseHandler


class DropboxHandler(BaseHandler):
	def __init__(self,config):
		super().__init__(config)
		self.authorized = False

	def _authorize(self):
		'''
		Handle authorizing with Dropbox, this is called automatically

		For specific internal API see: https://dropbox-sdk-python.readthedocs.io/en/latest/

		There are two ways to get authenticated with Dropbox,
		The first is through a user access token which is tied to the user's account
		The other is through a team access token which is tied to a Dropbox Business account
			The issue with the team access token is that it still needs "masquerade" as a user
			This library only provides the default user access.
		'''

		try:
			self.dbx = dropbox.Dropbox(
				app_key = self.config["BH_PUBLIC_KEY"],
				app_secret = self.config["BH_SECRET_KEY"],
				oauth2_refresh_token = self.config["BH_REFRESH_TOKEN"]
			)

			root_namespace_id = self.dbx.users_get_current_account().root_info.root_namespace_id

			self.dbx_user = self.dbx.with_path_root(
				dropbox.common.PathRoot.root(root_namespace_id)
			)
			self.authorized = True
		except Exception as e:
			print(f"Error initializing Dropbox handler: {str(e)}")
			traceback.print_exc()
			raise e

	def _auto_authenticate(self):
		'''
		A memoized internal function that handles getting a new dropbox context with the refresh token
		'''
		if self.authorized:
			return
		self._authorize()

	def _get_team_namespace_id(self, dbx_team, namespace):
		'''
		Get the namespace id for a given team namespace name
		Args:
			dbx_team: the dropbox team object
			namespace: the name of the namespace to find, this is usually something like "Widget Co"
		Returns:
			the namespace id for the given namespace name
		'''
		namespaces = dbx_team.team_namespaces_list()
		for ns in namespaces.namespaces:
			if ns.name == namespace:
				return ns.namespace_id
		raise Exception("Team namespace not found")

	def _get_team_members(self, dbx_team, team_member_name, select=True):
		'''
		Find a team member and masquerade as them

		Args:
			dbx_team: the dropbox team object
			team_member_name: the display name of the user to find, this is usually something like "John Doe"
			select: if true, return the dropbox client masquerading as this user, otherwise just return the member info
		Returns:
			if select is true, returns a dropbox client masquerading as the user, otherwise returns the member info
		'''
		members = dbx_team.team_members_list()
		for member in members.members:
			if member.profile.name.display_name == team_member_name:
				if select:
					# masquerade as this user
					dbx_user = dbx_team.as_user(member.profile.team_member_id)

					return dbx_user

				return  member
		raise Exception("Team member not found")

	def _oauth_login(self):
		auth_flow = dropbox.DropboxOAuth2FlowNoRedirect(self.config["BH_PUBLIC_KEY"], self.config["BH_SECRET_KEY"])
		authorize_url = auth_flow.start()
		print("1. Go to: " + authorize_url)
		print("2. Click 'Allow' (you might have to log in first)")
		print("3. Copy the authorization code.")
		if webbrowser is not None:
			webbrowser.open(authorize_url)
		auth_code = input("Enter the authorization code here: ").strip()
		try:
			oauth_result = auth_flow.finish(auth_code)
			self.dbx = dropbox.Dropbox(
				oauth2_access_token=oauth_result.access_token,
				oauth2_refresh_token=oauth_result.refresh_token,
				app_key=self.config["BH_PUBLIC_KEY"],
				app_secret=self.config["BH_SECRET_KEY"]
			)
			print("Successfully authenticated with Dropbox!")
		except Exception as e:
			print("Error during Dropbox authentication: " + str(e))
			sys.exit(1)

	def _prep_config(self, config) -> dict:
		keys_required = ['BH_PUBLIC_KEY', 'BH_SECRET_KEY']
		keys_optional = ['BH_ACCESS_TOKEN', 'BH_REFRESH_TOKEN']
		config = self._extend_config_from_env(config, keys_required, keys_optional)
		return config

	def _strip_protocol_from_path(self,path:str) -> str:
		'''
		Remove the protocol from paths that start with the protocol
		'''
		if path[:5].lower().startswith('db://'):
			return path[5:]
		return path

	def _find_file_by_path(self, path:str) -> Union[dict, None]:
		'''
		Queries dropbox for the specific file and mimics the reuslts from search

		Args:
			path (str): the path to the file in dropbox, should start with a /
		Returns:
			dict: mimics the file dict from search results, or None if not found
		'''
		try:
			metadata = self.dbx_user.files_get_metadata(path)
			if metadata is None:
				return None
			mimetype = self._get_mime_type(metadata.path_display, try_magic=False)
			return {
				"fileName": metadata.path_display,
				"contentLength": metadata.size if isinstance(metadata, dropbox.files.FileMetadata) else None,
				"contentType": mimetype,
				"contentMD5": None, # we would have to fetch the file to calculate this, and since dropbox doesn't provide it, we'll just leave it as None
				"etag": metadata.content_hash if isinstance(metadata, dropbox.files.FileMetadata) else None, #not technically a true etag but it's a hash of the content so it serves a similar purpose, it uses sha256
				"uploadTimestamp": int(metadata.client_modified.timestamp() * 1000) if isinstance(metadata, dropbox.files.FileMetadata) else None,
				"action": "upload" if isinstance(metadata, dropbox.files.FileMetadata) else "folder",
			}
		except dropbox.exceptions.ApiError as e:
			if e.error.is_path() and e.error.get_path().is_not_found():
				print(f"Path not found: {path}")
			else:
				print(f"Error getting file info for {path}: {str(e)}")
			return None

	def _search_append_results(self, results, tmp_result, include, min_size, max_size, include_dirs, include_files, recurse, seen, safety_max_file_count, limit):
		for entry in tmp_result.entries:
			if (include_dirs and isinstance(entry, dropbox.files.FolderMetadata)) or (include_files and isinstance(entry, dropbox.files.FileMetadata)):
				if (min_size is None or (isinstance(entry, dropbox.files.FileMetadata) and entry.size >= min_size)) and (max_size is None or (isinstance(entry, dropbox.files.FileMetadata) and entry.size <= max_size)):
					seen += 1

					# note this is a local client side search, it's not very efficient
					# TODO: add support for caching using the cursor to tell us updates
					if include != None:
						if not re.search(include, entry.path_display):
							continue

					#get the last mtime of the file
					mtime = 0
					contentHash = None
					if isinstance(entry, dropbox.files.FileMetadata):
						# convert the mtime to a unix timestamp in ms
						mtime = int(entry.client_modified.timestamp() * 1000)
						contentHash = entry.content_hash

					# guess the mimetype from the name
					mimetype = self._get_mime_type(entry.path_display, try_magic=False)


					results.append({
						"fileName": entry.path_display,
						"contentLength": entry.size if isinstance(entry, dropbox.files.FileMetadata) else None,
						"contentType": mimetype,
						"contentMD5": None, # we would have to fetch the file to calculate this, and since dropbox doesn't provide it, we'll just leave it as None
						"etag": contentHash, #not technically a true etag but it's a hash of the content so it serves a similar purpose, it uses sha256
						"uploadTimestamp": mtime,
						"action": "upload" if isinstance(entry, dropbox.files.FileMetadata) else "folder",
					})
					if limit > 0 and len(results) >= limit:
						break
	def search(self,prefix:Union[str,List[str]]='',include=None,min_size=None,max_size=None,include_dirs=True,include_files=True, recurse=True, limit=0):
		self._auto_authenticate()

		if isinstance(prefix, str):
			prefix = [prefix]

		# starting from the prefix root, list all files in the this path recursively if set

		safety_max_file_count = 100000

		results = []
		# some dropbox folders can contain a huge number of files, we'll have a safety check to prevent querying forever
		seen = 0
		for p in prefix:
			p = self._strip_protocol_from_path(p)

			path_root = "/" + p.strip('/')

			tmp_result = None
			try:
				tmp_result = self.dbx_user.files_list_folder(path_root, recursive=recurse)
				self._search_append_results(results, tmp_result, include, min_size, max_size, include_dirs, include_files, recurse, seen, safety_max_file_count, limit)
			except dropbox.exceptions.ApiError as e:
				if e.error.is_path():
					lookup_error = e.error.get_path()
					if lookup_error.is_not_folder():
						# If we got here it means the requested path is a file
						# if we can get the info just append this and continue on
						file_info = self._find_file_by_path(path_root)
						if file_info is not None:
							results.append(file_info)
							continue
					elif lookup_error.is_not_found():
						print(f"ERROR: Path not found, skipping: {path_root}")
					else:
						print(f"ERROR: Unknown path error for: {path_root} error: {str(e)}")
					break
				else:
					raise e
			while True:
				tmp_result = self.dbx_user.files_list_folder_continue(tmp_result.cursor)
				self._search_append_results(results, tmp_result, include, min_size, max_size, include_dirs, include_files, recurse, seen, safety_max_file_count, limit)
				if not tmp_result.has_more:
					break
				if seen >= safety_max_file_count:
					print(f"Reached max file count of {safety_max_file_count}, aborting search")
					break
				if limit > 0 and len(results) >= limit:
					break

		for idx in range(len(results)):
			# standardize this so it's just like b2/s3
			results[idx]['fileName'] = results[idx]['fileName'].lstrip('/')

		result = {"files": results}
		self._sort_search_results(result)
		return result

	def _download_by_path(self, path_src:str, path_dst:Union[str,None]=None, with_txt = False, start=None, end=None, write_to_disk=True):
		self._auto_authenticate()
		if not path_src.startswith('/'):
			path_src = '/' + path_src # Dropbox needs a leading slash for the path
		try:
			if path_dst != None:
				os.makedirs(os.path.dirname(path_dst), exist_ok=True)
			self.dbx_user.files_download_to_file(path_dst, path_src)
			return True
		except Exception as e:
			print(f"Error downloading file {path_src} to {path_dst}: {str(e)}")
		return False


	def download(self, prefix: Union[str, List[str]], destination_root=None, include=None, min_size=None, max_size=None, recurse=True, preserve_dir_prefix=False):
		self._auto_authenticate()
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
						print(f"Error downloading file: {str(e)}")
						print(traceback.format_exc())
			except KeyboardInterrupt:
				print(f"User canceled downloads")
				executor.shutdown(wait=False,cancel_futures=True)
				#stop_event.set()
			wait(futures)

		return result

	def _calculate_db_hash(self, path_src:str):
		'''
		Calculates the hash that dropbox uses, this is not currently used but is here for reference if you need it
		See Also:
			https://www.dropbox.com/developers/reference/content-hash
		'''

		block_size = 4 * 1024 * 1024 # 4MB
		hasher = hashlib.sha256()

		with open(path_src, 'rb') as f:
			while True:
				block = f.read(block_size)
				if not block:
					break
				block_hash = hashlib.sha256(block).digest()
				hasher.update(block_hash)

		return hasher.hexdigest()

	def _upload_file(self, path_src:str, path_dst:str, upload_key=None):
		self._auto_authenticate()
		# this is a single file upload, not to be confused with the "upload" method which can upload multiple files based on a prefix

		if not path_dst.startswith('/'):
			path_dst = '/' + path_dst

		data:bytes
		buffer_size = 10 * 1024 * 1024 # 10MB
		with open(path_src, 'rb',buffering=buffer_size) as f:
			data = f.read()

		try:
			self.dbx_user.files_upload(data,path=path_dst, mode=dropbox.files.WriteMode.overwrite)
		except Exception as e:
			print(f"Error uploading file {path_src} to {path_dst}: {str(e)}")
			return False
		return True


	def get_download_url(self,path:Union[str,List[str]],expiration_seconds=60*60, inline=False, content_type=None):
		self._auto_authenticate()
		# Dropbox doesn't have a way to generate a download url without sharing the file, so we'll create a shared link with an expiration and return that

		#print(f"Generating download URL for {path} with expiration of {expiration_seconds} seconds, inline={inline}, content_type={content_type}")

		if isinstance(path, str):
			path = [path]

		expiration_timestamp = None
		if expiration_seconds is not None and expiration_seconds > 0:
			expiration_timestamp = datetime.now(timezone.utc) + timedelta(seconds=expiration_seconds)

		results = []
		for p in path:
			p = self._strip_protocol_from_path(p)
			path_root = "/" + p.strip('/')

			settings = dropbox.sharing.SharedLinkSettings(requested_visibility=RequestedVisibility.public, expires=expiration_timestamp)
			try:
				# allow anyone with the link to view the file
				link_metadata = self.dbx_user.sharing_create_shared_link_with_settings(path_root, settings)
				results.append(link_metadata.url)
			except Exception as e:
				#does the link already exist?
				if (isinstance(e, dropbox.exceptions.ApiError) and e.error.is_shared_link_already_exists()) or "shared_link_already_exists" in str(e):
					try:
						links = self.dbx_user.sharing_list_shared_links(path=path_root, direct_only=True).links

						# does this link have public access?
						if links:
							for link in links:
								# see if we can find a public link
								if link.link_permissions.resolved_visibility.is_public():
									results.append(link.url)
									break
								else:
									#  we couldn't find a public link, so we have to modify an existing one
									print(f"Existing link found for {path_root} but it's not public, making it public for url: {link.url}")
									self.dbx_user.sharing_modify_shared_link_settings(link.url, settings)
									results.append(link.url)
									break
						else:
							print(f"No existing links found for {path_root}, nor can we create one")
							results.append(None)

					except Exception as e2:
						print(f"Error fetching existing shared link for {path_root}: {str(e2)}")
						results.append(None)
				else:
					print(f"Error creating shared link for {path_root}: {str(e)}")
					results.append(None)

		# dropbox has three modes, true inline (raw=1) a semi-embedded page (dl=0) and a forced download (dl=1)
		for idx in range(len(results)):
			url = results[idx]
			if inline and (url.endswith('?dl=0') or url.endswith('&dl=0')):
				url = url[:-4] + 'raw=1'
				results[idx] = url
			elif not inline and (url.endswith('?raw=1') or url.endswith('&raw=1')):
				url = url[:-6] + 'dl=0'
				results[idx] = url
			elif not inline and (url.endswith('?dl=0') or url.endswith('&dl=0')):
				url = url[:-4] + 'dl=1'
				results[idx] = url

		return results


	def get_token_from_code(self, code):
		# This comes from:
		#  https://www.dropbox.com/oauth2/authorize?client_id=yuel97nok44tb1l&response_type=code&token_access_type=offline
		# which has to be in a browser, you then copy it and run this...

		url = "https://api.dropboxapi.com/oauth2/token"
		data = {
			"code": code,
			"grant_type": "authorization_code",
			"client_id": self.config["BH_PUBLIC_KEY"],
			"client_secret": self.config["BH_SECRET_KEY"],
		}
		response = requests.post(url, data=data)

		result =  {
		}

		if response.status_code == 200:
			result['access_token'] = response.json().get("access_token")
			result['refresh_token'] = response.json().get("refresh_token")
			result['expires_in'] = response.json().get("expires_in")
			result['token_type'] = response.json().get("token_type")
		else:
			raise Exception(f"Failed to get token: {response.status_code} - {response.text}")

		return result

	def get_temp_access_token_from_refresh_token(self, refresh_token):
		url = "https://api.dropboxapi.com/oauth2/token"
		data = {
			"refresh_token": refresh_token,
			"grant_type": "refresh_token",
			"client_id": self.config["BH_PUBLIC_KEY"],
			"client_secret": self.config["BH_SECRET_KEY"],
		}
		response = requests.post(url, data=data)

		result =  {
		}

		if response.status_code == 200:
			result['access_token'] = response.json().get("access_token")
			result['expires_in'] = response.json().get("expires_in")
			result['token_type'] = response.json().get("token_type")
		else:
			raise Exception(f"Failed to get token: {response.status_code} - {response.text}")

		return result


	def initiate_auth(self):
		'''
		Starts the process for generating the access and refresh token.
		This is currently performed by the user triggering this and is NOT automated

		This generates a access_token and refresh_token which you must copy into your config json file

		You can trigger this automatically from the cli with the command:
		bh authorize --dropbox --config=path/to/config.json
		'''
		app_key = self.config.get("dropbox_app_key")
		auth_flow = dropbox.DropboxOAuth2FlowNoRedirect(app_key, use_pkce=True, token_access_type='offline')
		auth_url = auth_flow.start()
		print(f"Visit: {auth_url} and get the auth code and return here")
		if webbrowser is not None:
			webbrowser.open(auth_url)
		else:
			pass
		auth_code = input("Enter code: ").strip()

		result = auth_flow.finish(auth_code)
		access_token = result.access_token
		refresh_token = result.refresh_token

		print(f"Access token: {access_token} refresh token: {refresh_token} you can use the refresh token to get new access tokens when they expire expiration: {result.expires_at}")
