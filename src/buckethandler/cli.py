'''
Main entry point for the buckethandler, make sure you setup a config.json file alongside this with your keys.

Basic usage:
python bh.py fetch [key]
python bh.py list
python bh.py delete [key]
python bh.py push [key]

Where key will be a unique identifier such as the recipe id or similar.

SEE ALSO: README.md
'''

from ast import arg
import os
import sys
import getopt
import datetime
import re
import time
import json
import argparse
import textwrap

import buckethandler.b2 as b2

def _guess_protocol(path):
	if path is None:
		return None

	if isinstance(path, list):
		for p in path:
			# use the first one, yes this will break if you have something like b2://... and then s3://...
			return _guess_protocol(p)
		return None

	if path.startswith("b2://"):
		return 'b2'
	elif path.startswith("s3://"):
		return 's3'
	else:
		return None

def pretty_print_files(files):
	minTime = 0
	maxTime = 0

	max_filename_str_len = 0
	max_filesize_str_len = 0
	max_content_type_str_len = 0
	last_line = ''

	for file in files.get('files', []):
		max_filename_str_len = max(max_filename_str_len, len(file['fileName']))
		max_filesize_str_len = max(max_filesize_str_len, len(b2.pretty_file_size(file['contentLength'])))
		content_type_str = ""
		if file['action'] == 'upload':
			content_type_str = file['contentType'].ljust(max_content_type_str_len)
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

		pretty_file_size = b2.pretty_file_size(file['contentLength'])

		file_name_str = file['fileName'].ljust(max_filename_str_len)
		content_type_str = ""

		time_str = ""

		# there are 4 actions: start, upload, hide, folder, see: https://www.backblaze.com/apidocs/b2-list-file-names
		if file['action'] == 'upload':
			content_type_str = file['contentType'].ljust(max_content_type_str_len)
			time_str = datetime.datetime.fromtimestamp(upload_timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
		elif file['action'] == 'folder':
			content_type_str = "".ljust(max_content_type_str_len)
			pretty_file_size = ""
		elif file['action'] == 'hide':
			content_type_str = "".ljust(max_content_type_str_len)
			pretty_file_size = ""
		elif file['action'] == 'list':
			content_type_str = "".ljust(max_content_type_str_len)
			pretty_file_size = ""

		file_size_str = pretty_file_size.ljust(max_filesize_str_len)
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

def main():


	parser_global = argparse.ArgumentParser(add_help=False)

	parser_global.add_argument('-c', '--config', help='Path to the configuration file (default: config.json)', default='config.json')
	parser_global.add_argument('--norecurse', help='Do not recurse into subdirectories, defaults to recurse', action='store_true')
	parser_global.add_argument('--nodirs', help='Exclude directories from the file list, defaults to include directories', action='store_true')
	parser_global.add_argument('--nofiles', help='Exclude files from the file list, defaults to include files', action='store_true')
	parser_global.add_argument('--threads', type=int, help='Set the maximum number of concurrent download or upload threads', default=None)
	parser_global.add_argument('--preservedir', help='When downloading, preserve the prefix folder structure in the destination path, defaults to False', action='store_true')
	parser_global.add_argument('--include', help='Filter file list by a regular expression')
	parser_global.add_argument('--exclude', help='Exclude files from the list by a regular expression')
	parser_global.add_argument('--minsize', help='Minimum filesize to consider for listing or downloading, in bytes or with suffixes like 10KB, 5MB, 2GB')
	parser_global.add_argument('--maxsize', help='Maximum filesize to consider for listing or downloading, in bytes or with suffixes like 10KB, 5MB, 2GB')

	parser = argparse.ArgumentParser(description='A command-line tool for managing files in remote buckets.',formatter_class=argparse.RawTextHelpFormatter, parents=[parser_global])

	subparsers = parser.add_subparsers(dest='cmd', required=True, help='The command to execute')

	parser_ls = subparsers.add_parser('ls', help='List files in a remote path. Usage: bh ls [remote_path]', parents=[parser_global])
	parser_ls.add_argument('src', nargs="+", help='The remote path to list, such as b2://bucket/prefix/')

	parser_cp = subparsers.add_parser('cp', help='Copy files between local and remote paths. Usage: bh cp [src] [dst]', parents=[parser_global])
	parser_cp.add_argument('src', nargs='+', help='The source path may be local or remote, context depends on the command')
	parser_cp.add_argument('dst', help='Only used in cp command, if a local path this will be an download, if a remote path this will be an upload')
	parser_cp.add_argument('--failsafe', help='Specify a failsafe copy path to use if the upload fails, only used for uploads, useful if you delete locally after upload')

	parser_rm = subparsers.add_parser('rm', help='Remove a file from a remote path. Usage: bh rm [remote_path]', parents=[parser_global])
	parser_rm.add_argument('src', nargs='+', help='The remote path to delete, such as b2://bucket/path/file.txt')
	parser_rm.add_argument('--recursive', help='Recurse through the files to delete, this is off by default for safety', action='store_true')
	parser_rm.add_argument('--latestonly', help='If set, only delete the latest file version not all versions', action='store_true')

	parser_url = subparsers.add_parser('url', help='Generate a temporary URL for a file in a remote path. Usage: bh url [remote_path]', parents=[parser_global])
	parser_url.add_argument('src', nargs='+', help='The remote path to generate a URL for, such as b2://bucket/path/file.txt')

	parser_ls_buckets = subparsers.add_parser('ls-buckets', help='List all buckets in the account', parents=[parser_global])

	'''
	parser.add_argument('src', nargs='+', help='The source path may be local or remote, context depends on the command')
	parser.add_argument('dst', nargs='?', help='Only used in cp command, if a local path this will be an download, if a remote path this will be an upload')
	'''

	args = parser.parse_args()

	protocol_src = None
	protocol_dst = None

	if 'src' in args:
		protocol_src = _guess_protocol(args.src)
	if 'dst' in args:
		protocol_dst = _guess_protocol(args.dst)

	include_dirs = not args.nodirs
	include_files = not args.nofiles
	recurse = not args.norecurse

	if args.cmd == 'ls':
		# make sure src is provided and is a remote path
		if args.src is None:
			print("Please provide a remote path to list, such as b2://bucket/prefix/")
			sys.exit(1)
		if protocol_src is None:
			print("Please provide a remote path to list, such as b2://bucket/prefix/")
			sys.exit(1)

		handler = b2.BackblazeB2Handler(args.config)

		files = handler.search(prefix=args.src, include=args.include, min_size=args.minsize, max_size=args.maxsize, include_dirs=include_dirs, include_files=include_files, recurse=recurse)
		pretty_print_files(files)


	elif args.cmd == 'cp':
		# make sure src and dst are provided and one is a remote path and the other is a local path
		if args.src is None or args.dst is None:
			print("Please provide both a source and destination path for copy, such as bh.py cp /local/path/file.txt b2://bucket/path/file.txt for upload or bh.py cp b2://bucket/path/file.txt /local/path/file.txt for download")
			sys.exit(1)
		if protocol_src is not None and protocol_dst is not None:
			print("Please provide one local path and one remote path for copy, such as bh.py cp /local/path/file.txt b2://bucket/path/file.txt for upload or bh.py cp b2://bucket/path/file.txt /local/path/file.txt for download")
			sys.exit(1)
		if protocol_src is None and protocol_dst is None:
			print("Please provide one local path and one remote path for copy, such as bh.py cp /local/path/file.txt b2://bucket/path/file.txt for upload or bh.py cp b2://bucket/path/file.txt /local/path/file.txt for download")
			sys.exit(1)

		handler = b2.BackblazeB2Handler(args.config)

		# which direction are we going?
		if protocol_src is None and protocol_dst is not None:
			# upload
			if args.threads is not None:
				handler.set_max_upload_single_threads(args.threads)
				handler.set_max_upload_threads(args.threads)
			if args.failsafe is not None:
				handler.set_failsafe_copy(args.failsafe)
			result = handler.upload(args.src, destination_root=args.dst)
			if result == False:
				print(f"Upload failed")
				sys.exit(1)
			else:
				print(f"Uploaded: {len(result)} files")

		else:
			# download
			if args.threads != None:
				handler.set_max_download_threads(args.threads)
			result = handler.download(prefix=args.src, include=args.include, min_size=args.minsize, max_size=args.maxsize, destination_root=args.dst, preserve_dir_prefix=args.preservedir)



	elif args.cmd == 'rm':

		if args.src is None:
			print("Please provide a remote path to delete, such as b2://bucket/path/file.txt")
			sys.exit(1)
		if protocol_src is None:
			print("Please provide a remote path to delete, such as b2://bucket/path/file.txt")
			sys.exit(1)

		handler = b2.BackblazeB2Handler(args.config)

		if args.recursive:
			for src in args.src:
				src = handler._strip_protocol_from_path(src)
				if len(src) == 0 or src == '/':
					# sanity check, they're asking to purge all files in their bucket
					print(f"You are requesting to delete the entire source bucket, type YES to confirm")
					confirm = input()
					if confirm != 'YES':
						print("Aborting delete")
						sys.exit(1)

		result = handler.delete(args.src,args.recursive, all_versions=not args.latestonly)
		print(f"Deleted {result} files")



	elif args.cmd == 'url':
		if args.src is None:
			print("Please provide a remote path to generate a pre-signed URL for, such as b2://bucket/path/file.txt")
			sys.exit(1)
		if protocol_src is None:
			print("Please provide a remote path to generate a pre-signed URL for, such as b2://bucket/path/file.txt")
			sys.exit(1)

		handler = b2.BackblazeB2Handler(args.config)
		urls = handler.get_download_url(args.src)
		for url in urls:
			print(url)

	elif args.cmd == 'ls-buckets':
		handler = b2.BackblazeB2Handler(args.config)
		buckets = handler.list_buckets()
		for bucket in buckets.get('buckets', []):
			print(bucket['bucketName'])




	'''


	opts, args = getopt.getopt(sys.argv[1:], "h c l u d r rp", ["help","config=", "list=","upload=", "download=", "remove=", "path=", "failsafe=", "maxsize=", "minsize=", "filter=", "threads=", "norecurse", "nodirs", "nofiles"])

	configuration = None
	handler = None
	destination_path = None
	min_size = None
	max_size = None
	filter = None
	recurse = True
	include_dirs = True
	include_files = True
	threads = None

	# handle defaults like configurations first, then handle params
	for opt, arg in opts:
		if opt in ("-p", "--path"):
			destination_path = arg
			#print(f"Remote path set to: {destination_path}")
		if opt in ("--maxsize"):
			max_size = b2.FromPrettyFileSize(arg)
		if opt in ("--minsize"):
			min_size = b2.FromPrettyFileSize(arg)
		if opt in ("--filter"):
			filter = arg
		if opt in ("--nodirs"):
			include_dirs = False
		if opt in ("--nofiles"):
			include_files = False
		if opt in ("--norecurse"):
			recurse = False
		if opt in ("--threads"):
			threads = int(arg)

	if configuration is None:
		configuration = 'config.json'

	handler = b2.BackblazeB2Handler(configuration)


	for opt, arg in opts:
		if opt in ("-f", "--failsafe"):
			handler.SetFailsafeCopy(arg)
			print(f"Set failsafe copy path to: {arg}")

	for opt, arg in opts:
		if opt in ("-c", "--config"):
			# already handled above, so we can skip this
			pass
		if opt in ("-h", "--help"):
			usage()
			sys.exit(0)
		elif opt in ("-l", "--list"):
			buckets = handler.ListBuckets()
			files = handler.List(prefix=arg, filter=filter, min_size=min_size, max_size=max_size, include_dirs=include_dirs, include_files=include_files, recurse=recurse)
			minTime = 0
			maxTime = 0

			max_filename_str_len = 0
			max_filesize_str_len = 0
			max_content_type_str_len = 0

			for file in files.get('files', []):
				max_filename_str_len = max(max_filename_str_len, len(file['fileName']))
				max_filesize_str_len = max(max_filesize_str_len, len(b2.PrettyFileSize(file['contentLength'])))
				content_type_str = ""
				if file['action'] == 'upload':
					content_type_str = file['contentType'].ljust(max_content_type_str_len)
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

				pretty_file_size = b2.PrettyFileSize(file['contentLength'])

				file_name_str = file['fileName'].ljust(max_filename_str_len)
				content_type_str = ""

				time_str = ""

				# there are 4 actions: start, upload, hide, folder, see: https://www.backblaze.com/apidocs/b2-list-file-names
				if file['action'] == 'upload':
					content_type_str = file['contentType'].ljust(max_content_type_str_len)
					time_str = datetime.datetime.fromtimestamp(upload_timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
				elif file['action'] == 'folder':
					content_type_str = "".ljust(max_content_type_str_len)
					pretty_file_size = ""
				elif file['action'] == 'hide':
					content_type_str = "".ljust(max_content_type_str_len)
					pretty_file_size = ""
				elif file['action'] == 'list':
					content_type_str = "".ljust(max_content_type_str_len)
					pretty_file_size = ""

				file_size_str = pretty_file_size.ljust(max_filesize_str_len)

				print(f"{file_name_str}\t{content_type_str}\t{file_size_str}\t{time_str}")

			min_time_str = datetime.datetime.fromtimestamp(minTime / 1000).strftime('%Y-%m-%d %H:%M:%S')
			max_time_str = datetime.datetime.fromtimestamp(maxTime / 1000).strftime('%Y-%m-%d %H:%M:%S')

			min_max_time_delta_ms = maxTime - minTime
			min_max_time_delta = min_max_time_delta_ms / 1000
			# convert the seconds to a time range like 01:23:45 for 1 day 23 hours 45 seconds
			days = int(min_max_time_delta // 86400)
			hours = int((min_max_time_delta % 86400) // 3600)
			minutes = int((min_max_time_delta % 3600) // 60)
			seconds = int(min_max_time_delta % 60)

			print(f"Total files: {len(files.get('files', []))} examined, minTime: {min_time_str} maxTime: {max_time_str} time delta: {days}:{hours:02d}:{minutes:02d}:{seconds:02d}")

		elif opt in ("-u", "--upload"):
			if not arg:
				print("Please provide a key to upload.")
				usage()
				sys.exit(1)
			else:
				print(f"Uploading file with key: {arg}")
				# Here you would implement the upload logic
				if destination_path is None:
					destination_path = '/uploads/'
				if threads != None:
					handler.SetMaxUploadSingleThreads(threads)
				result = handler.Upload(arg, destination_root=destination_path)
				if result != True:
					print(f"Upload failed")
					sys.exit(1)
		elif opt in ("-d", "--download"):
			if not arg:
				print("Please provide a key to download.")
				usage()
				sys.exit(1)
			else:
				#print(f"Downloading files at: {arg}")
				if destination_path is None:
					destination_path = './downloads/'
				if threads != None:
					handler.SetMaxDownloadThreads(threads)
				result = handler.DownloadDirectory(prefix=arg, filter=filter, min_size=min_size, max_size=max_size, destination_root=destination_path)
				#if result:
				#	print(f"Downloaded file: {result['fileName']}, Content Type: {result['contentType']}, Size: {result['contentLength']} bytes")
		elif opt in ("-r", "--remove"):
			if not arg:
				print("Please provide a key to delete.")
				usage()
				sys.exit(1)
			else:
				print(f"Deleting file with key: {arg}")
				result = handler.Delete(arg)
				if result:
					print(f"Deleted file: {arg}, Result: {result}")
				else:
					print(f"Failed to delete file: {arg}")

	'''



if __name__ == "__main__":
	main()
