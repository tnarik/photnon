#!/usr/bin/env python3


import piexif
import os
import pandas as pd

from colorama import init, Fore
init(autoreset=True)

root = "/Volumes/frø/flickr_backup/"
dated_photos = []
for dir in os.listdir(root):
	if dir.startswith('data-download-'):
		print("{}--- {} ---".format(Fore.GREEN, dir))
		for file in os.scandir(os.path.join(root, dir)):
			try:
				pic_exif = piexif.load(file.path)
				try:
					if (36867 in pic_exif["Exif"]) and ( pic_exif["Exif"][36867] == pic_exif["Exif"][36868]):
						date = pic_exif["Exif"][36867].decode('utf-8')
					elif 306 in pic_exif['0th']:
							date = pic_exif['0th'][306].decode('utf-8')
					else:
						print("  {} {} is weird".format(Fore.YELLOW, file.name))
						continue
						
					#print("  {} - {} ".format(file.name, date) )
					dated_photos.append([dir, file.name, date])
				except KeyError:
					print("{} BIG ERROR with {}".format(Fore.RED, file.name))
			except piexif._exceptions.InvalidImageDataError:
				print("{}  {} was not a picture".format(Fore.RED, file.name))

#print(dated_photos)
ph = pd.DataFrame(dated_photos, columns=['folder', 'name', 'date'])

print(ph)

input("{}Press Enter to save the data...{}".format(Fore.GREEN, Fore.RESET))

ph.to_pickle('daphotos.pk')