#!/usr/bin/env python3

# piexif is faster than Hachoir, and so has prececence
# Even if it cannot treat as many files, the speed gain is worthy
#

import piexif
import os
import hashlib
from tqdm import tqdm

import magic
import re

from hachoir.metadata import extractMetadata
from hachoir.parser import createParser
from hachoir.core import config as HachoirConfig
#HachoirConfig.quiet = True

import glob
import time
import pandas as pd

import argparse

from colorama import init, Fore
init(autoreset=True)

CODE_OK = 0
CODE_ERROR = 1
CODE_INVALIDIMAGEDATA = 2
CODE_WEIRD = 3

CHUNK_SIZE = 2**20 # 1 MB

IGNORED_FOLDERS = ['.AppleDouble', '.git']
IGNORED_FILES = ['.DS_Store', 'ZbThumbnail.info', '.gitignore']
IGNORED_PATTERNS = ['.*\.json']

patterns = []
for pat in IGNORED_PATTERNS:
	patterns.append(re.compile(pat))
IGNORED_PATTERNS = patterns

count_fixed = 0
def identify_file(path, name):
	global count_fixed
	datetime = None
	model = None
	digest = None
	mime = None
	code = CODE_OK
	try:
		digester = hashlib.sha1()
		with open(path, "rb") as f:
			for chunk in iter(lambda: f.read(CHUNK_SIZE), b''):
				digester.update(chunk)
		digest = digester.hexdigest()
		mime = magic.from_file(path, mime=True)

		pic_exif = piexif.load(path)
		if args.verbose > 4 : print(pic_exif)
		try:
			#36867 - taken
			#36868 - digitized

			# datetime
			if (36867 in pic_exif["Exif"]):
				#and ( pic_exif["Exif"][36867] == pic_exif["Exif"][36868]):
				datetime = pic_exif["Exif"][36867].decode('utf-8')
				if args.verbose > 1 : print("{}EXIF - {}".format(Fore.BLUE, name))
			elif 306 in pic_exif['0th']:
				datetime = pic_exif['0th'][306].decode('utf-8')
				if args.verbose > 1 : print("{}0th - {}".format(Fore.WHITE, name))
			elif 'GPS' in pic_exif and 29 in pic_exif['GPS']:
				datetime = pic_exif['GPS'][29].decode('utf-8')
				if args.verbose > 1 : print("{}GPS - {}".format(Fore.GREEN, name))
			else:
				code = CODE_WEIRD
				if args.verbose: print("{}ABSENT - {}".format(Fore.YELLOW, name))
				if args.verbose > 2 : print(pic_exif)			
			# model
			if 42036 in pic_exif["Exif"]:
				model = pic_exif["Exif"][42036].decode('utf-8')
			elif 272 in pic_exif['0th']:
				model = pic_exif["0th"][272].decode('utf-8')

		except KeyError:
			code = CODE_ERROR
			print("{}KEY ERROR - {}".format(Fore.RED, name))
			if args.verbose > 2: print(pic_exif)
	except piexif._exceptions.InvalidImageDataError:
		code = CODE_INVALIDIMAGEDATA
		if args.verbose: print("{}NOT an EXIF picture - {}".format(Fore.RED, name))

		parser = createParser(path)
		#print(path)
		if parser:
			try:
				metadata = extractMetadata(parser)
				if metadata:
					metadata = metadata.exportDictionary(human=False)
					#	print("{} {}".format(count_fixed, 
					#		metadata['Metadata']['creation_date']))

					if 'Metadata' in metadata:
						datetime = metadata['Metadata']['creation_date'].replace('-', ':')
					elif 'Common' in metadata:
						datetime = metadata['Common']['creation_date'].replace('-', ':')

					code = CODE_OK
					count_fixed += 1
					if args.verbose: print("     {}NOW! - {}".format(Fore.GREEN, name))
			except:
				print("{} - {}".format(name, metadata))
		else:
			if args.verbose: print("     {}NOT even NOW - {}".format(Fore.RED, name))

	return (datetime, model, digest, mime, code)

def filter_out(n, f):
	for ign in IGNORED_FOLDERS:
		if ign in n: n.remove(ign)
	for ign in IGNORED_FILES:
		if ign in f: f.remove(ign)
	for ign in IGNORED_PATTERNS:
		f = list(filter(lambda x: ign.match(x) is None,f))
	return n, f

def explore(space):
	""" files can be either a file, a folder or a pattern
		It can also be a list of files, folders or patterns.
	"""
	data = []

	if type(space) is not list:
		space = [space]

	for source in space:
		for path in glob.iglob(source):
			if os.path.isfile(path):
				datetime, model, digest, mime, code = identify_file(path, os.path.split(path)[1])
				data.append([*os.path.split(path), datetime, model, digest, code])
			else:
				# Pre-calculation of data size to process
				total_size = 0
				for p,n,f in os.walk(path):
					n, f = filter_out(n,f)

					for file in f:
						total_size += os.stat(os.path.join(p, file)).st_size
			
				# Gigabytes instead of Gibibytes
				with tqdm(total=total_size, unit='B', unit_scale=True, unit_divisor=100) as pbar:
					for p,n,f in os.walk(path):
						n, f = filter_out(n,f)
						for file in tqdm(f):
							pbar.update(os.stat(os.path.join(p, file)).st_size)
							datetime, model, digest, mime, code = identify_file(os.path.join(p,file), file)
							if code is None:
								continue
			
							data.append([p, file, datetime, model, digest, mime, code])
							# file sizes # pbar.update()

	return data


				
if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Photon")
	parser.add_argument('-d', '--data',
					    help='data file',
	    				dest='datafile')
	parser.add_argument('-v', '--verbose',
						help='verbose output',
						action='count',
						default=0)
	parser.add_argument('-s', '--space',
						nargs='*',
		                help='files, folders or pattern space to explore',
		                dest='space')
	args = parser.parse_args()
	
	data = None
	if args.space:
		print('no data file, so just explore')
		data = explore(args.space)	
		if len(data) > 0:
			print("{} entries".format(len(data)))

			ph = pd.DataFrame(data, columns=['folder', 'name', 'datetime', 'model', 'digest', 'mime', 'code'])

			# split into OK and ERROR files
			ph_ok, ph_error = ph[ph.code == CODE_OK].copy(), ph[ph.code != CODE_OK].copy()
			print(ph_ok.head(2))

			dates_with_no_time = ~ph_ok.datetime.str.match("^\d{4}:\d{2}:\d{2} ")	
			full_datetimes = ph_ok.loc[dates_with_no_time].datetime + " 08:00:00"
			ph_ok.loc[dates_with_no_time, 'datetime'] = full_datetimes
			ph_ok.loc[:, 'datetime'] = pd.to_datetime(ph_ok['datetime'], format="%Y:%m:%d %H:%M:%S")
			print(ph_ok.head(2))

			ph_ok.to_hdf('pik.h5', key='ok', format="table")
			ph_error.to_hdf('pik.h5', key='error', format="table")
			#(ph_ok, ph_error).to_pickle('pik.pk')

			#print(ph_ok.datetime)
			print("{} ok / {} error".format(len(ph_ok), len(ph_error)))
			print(ph_error)
			print("fixed {}".format(count_fixed))
	else:
		print('no input folders/files')


	if args.datafile:
		print('if it exists....')
		if data is not None:
			ph_ok.to_hdf("{}.pho".format(args.datafile), key='ok', format="table")
			ph_error.to_hdf("{}.pho".format(args.datafile), key='error', format="table")

	else:
		print('default file')

