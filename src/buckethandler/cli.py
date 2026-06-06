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
import re
import json
import argparse
import textwrap
import traceback
try:
	import webbrowser
except ImportError:
	webbrowser = None

from .buckethandler import BucketHandler, BucketHandlerType, BucketHandlerHandlerTypeException, pretty_print_files




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
	parser_ls.add_argument('--limit', help='Limit the number of files to return in a search')

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
	parser_url.add_argument('--expires', help='The number of seconds the URL should be valid for, default is 3600 (1 hour)', type=int, default=3600)
	parser_url.add_argument('--inline', help='Used to disable the download prompt for the url', action='store_true')
	parser_url.add_argument('--contenttype', help='Forces the content type of the url')

	parser_authorize = subparsers.add_parser('authorize', help='Command for generating tokens and access', parents=[parser_global])
	parser_authorize.add_argument('--dropbox', help='Authorize the application against dropbox, this will trigger a browser page', action='store_true')

	parser_ls_buckets = subparsers.add_parser('ls-buckets', help='List all buckets in the account', parents=[parser_global])

	'''
	parser.add_argument('src', nargs='+', help='The source path may be local or remote, context depends on the command')
	parser.add_argument('dst', nargs='?', help='Only used in cp command, if a local path this will be an download, if a remote path this will be an upload')
	'''

	args = parser.parse_args()

	bh_src = None
	bh_dst = None

	protocol_src = None
	protocol_dst = None

	if 'src' in args:
		try:
			bh_src = BucketHandler(args.config, path=args.src)
			protocol_src = bh_src.handler_type
		except BucketHandlerHandlerTypeException as e:
			if 'dst' not in args:
				print(f"Error initializing source handler: {e}")
				sys.exit(1)
		except Exception as e:
			print(f"Error initializing source handler: {e}")
			sys.exit(1)
			#traceback.print_exc()
	if 'dst' in args:
		try:
			bh_dst = BucketHandler(args.config, path=args.dst)
			protocol_dst = bh_dst.handler_type
		except BucketHandlerHandlerTypeException as e:
			pass
		except Exception as e:
			print(f"Error initializing destination handler: {e}")
			sys.exit(1)

	include_dirs = not args.nodirs
	include_files = not args.nofiles
	recurse = not args.norecurse


	if args.cmd == 'authorize':
		config = json.load(open(args.config))
		if args.dropbox:
			handler = BucketHandler(args.config, path="db://", handler_type=BucketHandlerType.DROPBOX).handler
			handler.initiate_auth()

	if args.cmd == 'ls':
		limit = 0
		if args.limit is not None:
			try:
				limit = int(args.limit)
				if limit < 0:
					print("Limit must be a non-negative integer")
					sys.exit(1)
			except ValueError:
				print("Invalid limit value, must be an integer")
				sys.exit(1)
		if bh_src is not None:
			files = bh_src.search(prefix=args.src, include=args.include, min_size=args.minsize, max_size=args.maxsize, include_dirs=include_dirs, include_files=include_files, recurse=recurse, limit=limit)
			if files != None and 'files' in files:
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

		handler = None
		if bh_src is not None:
			handler = bh_src
		elif bh_dst is not None:
			handler = bh_dst

		if handler is None:
			print("Error initializing handler for copy command")
			sys.exit(1)


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

		if args.recursive:
			for src in args.src:
				src = bh_src._strip_protocol_from_path(src)
				if len(src) == 0 or src == '/':
					# sanity check, they're asking to purge all files in their bucket
					print(f"You are requesting to delete the entire source bucket, type YES to confirm")
					confirm = input()
					if confirm != 'YES':
						print("Aborting delete")
						sys.exit(1)

		result = bh_src.delete(args.src,args.recursive, all_versions=not args.latestonly)
		print(f"Deleted {result} files")



	elif args.cmd == 'url':
		if args.src is None:
			print("Please provide a remote path to generate a pre-signed URL for, such as b2://bucket/path/file.txt")
			sys.exit(1)
		if protocol_src is None:
			print("Please provide a remote path to generate a pre-signed URL for, such as b2://bucket/path/file.txt")
			sys.exit(1)

		handler = bh_src
		if handler is None:
			print("Error initializing handler for url command")
			sys.exit(1)
		urls = handler.get_download_url(args.src, expiration_seconds=args.expires, inline=args.inline, content_type=args.contenttype)
		for url in urls:
			print(url)

	elif args.cmd == 'ls-buckets':
		handler = bh_src
		buckets = handler.list_buckets()
		for bucket in buckets.get('buckets', []):
			print(bucket['bucketName'])


if __name__ == "__main__":
	main()
