#!/usr/bin/env python3

import piexif
import os
import hashlib
from tqdm import tqdm

from hachoir.metadata import extractMetadata
from hachoir.parser import createParser

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

count_fixed = 0
def identify_file(path, name):
	global count_fixed
	datetime = None
	model = None
	digest = None
	code = CODE_OK
	try:
		digester = hashlib.sha1()
		with open(path, "rb") as f:
			for chunk in iter(lambda: f.read(CHUNK_SIZE), b''):
				digester.update(chunk)
		digest = digester.hexdigest()

		pic_exif = piexif.load(path)
		if args.verbose > 4 : print(pic_exif)
		try:
			#36867 - taken
			#36868 - digitized

			parser = createParser(path)
			if parser:
				try:
					metadata = extractMetadata(parser)
					if metadata:
						metadata = metadata.exportDictionary(human=False)
						print("{} {}".format(count_fixed, 
							metadata['Metadata']['creation_date']))
	
						datetime = metadata['Metadata']['creation_date']
						model = metadata['Metadata']['camera_model']
						#print("     {}  {}".format(Fore.GREEN, datetime))
				except:
					pass

			# datetime
			if not datetime:		
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
			if not model:
				if 42036 in pic_exif["Exif"]:
					model = pic_exif["Exif"][42036].decode('utf-8')
				elif 272 in pic_exif['0th']:
					model = pic_exif["0th"][272].decode('utf-8')

			#print("  {} - {} ".format(name, datetime) )
#			datetimed_photos.append([dir, name, datetime])
		except KeyError:
			code = CODE_ERROR
			print("{}KEY ERROR - {}".format(Fore.RED, name))
			if args.verbose > 2: print(pic_exif)
	except piexif._exceptions.InvalidImageDataError:
		code = CODE_INVALIDIMAGEDATA
		if args.verbose: print("{}NOT an EXIF picture - {}".format(Fore.RED, name))

		parser = createParser(path)
		if parser:
			try:
				metadata = extractMetadata(parser)
				if metadata:
					metadata = metadata.exportDictionary(human=False)
					print("{} {}".format(count_fixed, 
						metadata['Metadata']['creation_date']))

					datetime = metadata['Metadata']['creation_date']
					code = CODE_OK
					count_fixed += 1
					if args.verbose: print("     {}NOW! - {}".format(Fore.GREEN, name))
			except:
				print(metadata)
		else:
			if args.verbose: print("     {}NOT even NOW - {}".format(Fore.RED, name))

	return (datetime, model, digest, code)


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
				datetime, model, digest, code = identify_file(path, os.path.split(path)[1])
				data.append([*os.path.split(path), datetime, model, digest, code])
			else:
				for p,n,f in os.walk(path):
					print("{} {} {}".format(p, n, len(f)))
				print("--")
			
				for p,n,f in os.walk(path):
					for file in tqdm(f):
					#if dir.startswith('data-download-'):
					#	print("{}--- {} ---".format(Fore.GREEN, dir))
				#		for file in os.scandir(os.path.join(folders, dir)):
						datetime, model, digest, code = identify_file(os.path.join(p,file), file)
						if code is None:
							continue
		
						data.append([p, file, datetime, model, digest, code])

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
		
	if args.space:
		print('no data file, so just explore')
		data = explore(args.space)	
		if len(data) > 0:
			print("{} entries".format(len(data)))

			ph = pd.DataFrame(data, columns=['folder', 'name', 'datetime', 'model', 'digest', 'code'])

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
	else:
		print('default file')

