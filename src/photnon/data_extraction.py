# piexif is faster than Hachoir, and so has prececence
# Even if it cannot treat as many files, the speed gain is worthy
#

import piexif
import os
import sys
import hashlib
from tqdm import tqdm

import magic
import re

from hachoir.metadata import extractMetadata
from hachoir.parser import createParser
from hachoir.core import config as HachoirConfig
HachoirConfig.quiet = True

from prompt_toolkit.shortcuts import confirm

import glob
import time
import pandas as pd
from datetime import datetime as dt

from photnon import storage

from colorama import init, Fore
init(autoreset=True)

CODE_OK = 0
CODE_ERROR = 1
CODE_INVALIDIMAGEDATA = 2
CODE_WEIRD = 3
CODE_SIZE_ZERO = 4

CHUNK_SIZE = 2**20 # 1 MB

IGNORED_FOLDERS = ['.AppleDouble', '.git']
IGNORED_FILES = ['.DS_Store', 'ZbThumbnail.info', '.gitignore']
IGNORED_PATTERNS = ['.*\.json']

patterns = []
for pat in IGNORED_PATTERNS:
  patterns.append(re.compile(pat))
IGNORED_PATTERNS = patterns

count_hachoir = 0
def identify_file(path, name, verbose=0):
  global count_hachoir
  datetime = None
  make = None
  model = None
  digest = None
  mime = None
  code = CODE_OK
  has_json = False

  stats = os.stat(path)
  atime = dt.fromtimestamp(stats.st_atime)
  mtime = dt.fromtimestamp(stats.st_mtime)
  ctime = dt.fromtimestamp(stats.st_ctime)

  digester = hashlib.sha1()
  with open(path, "rb") as f:
    for chunk in iter(lambda: f.read(CHUNK_SIZE), b''):
      digester.update(chunk)
  digest = digester.hexdigest()
  mime = magic.from_file(path, mime=True)

  if stats.st_size == 0:
    code = CODE_SIZE_ZERO
  else:
    # Are there metadata as .json?
    has_json = os.path.exists(os.path.splitext(path)[0]+'.json')

    try:
      pic_exif = piexif.load(path)
      if verbose > 4 : print(pic_exif)
      try:
        #36867 - taken
        #36868 - digitized
  
        # datetime
        if (36867 in pic_exif["Exif"]):
          #and ( pic_exif["Exif"][36867] == pic_exif["Exif"][36868]):
          datetime = pic_exif["Exif"][36867].decode('utf-8')
          if verbose > 1 : print("{}EXIF - {}".format(Fore.BLUE, name))
        elif 306 in pic_exif['0th']:
          datetime = pic_exif['0th'][306].decode('utf-8')
          if verbose > 1 : print("{}0th - {}".format(Fore.WHITE, name))
        elif 'GPS' in pic_exif and 29 in pic_exif['GPS']:
          datetime = pic_exif['GPS'][29].decode('utf-8')
          if verbose > 1 : print("{}GPS - {}".format(Fore.GREEN, name))
        else:
          code = CODE_WEIRD
          if verbose: print("{}ABSENT - {}".format(Fore.YELLOW, name))
          if verbose > 2 : print(pic_exif)       
        # make
        if 42035 in pic_exif["Exif"]:
          make = pic_exif["Exif"][42035].decode('utf-8')
        elif 271 in pic_exif['0th']:
          make = pic_exif["0th"][271].decode('utf-8')
  
        # model
        if 42036 in pic_exif["Exif"]:
          model = pic_exif["Exif"][42036].decode('utf-8')
        elif 272 in pic_exif['0th']:
          model = pic_exif["0th"][272].decode('utf-8')
  
      except KeyError:
        code = CODE_ERROR
        print("{}KEY ERROR - {}".format(Fore.RED, name))
        if verbose > 2: print(pic_exif)
    except piexif._exceptions.InvalidImageDataError:
      code = CODE_INVALIDIMAGEDATA
      if verbose: print("{}NOT an EXIF picture - {}".format(Fore.RED, name))
  
      parser = createParser(path)
      #print(path)
      if parser:
        try:
          metadata = extractMetadata(parser)
          if metadata:
            metadata = metadata.exportDictionary(human=False)
  
            if 'Metadata' in metadata:
              datetime = metadata['Metadata']['creation_date'].replace('-', ':')
            elif 'Common' in metadata:
              datetime = metadata['Common']['creation_date'].replace('-', ':')
  
            code = CODE_OK
            count_hachoir += 1
            if verbose: print("   {}NOW! - {}".format(Fore.GREEN, name))
        except:
          if verbose > 1: print("{} - {}".format(name, metadata))
      else:
        if verbose: print("   {}NOT even NOW - {}".format(Fore.RED, name))

  return (datetime, make, model, digest, mime, code, stats.st_size, atime, mtime, ctime, has_json)

def filter_out(n, f):
  for ign in IGNORED_FOLDERS:
    if ign in n: n.remove(ign)
  for ign in IGNORED_FILES:
    if ign in f: f.remove(ign)
  for ign in IGNORED_PATTERNS:
    f = list(filter(lambda x: ign.match(x) is None,f))
  return n, f

def explore(space, working_info):
  """ files can be either a file, a folder or a pattern
    It can also be a list of files, folders or patterns.
  """
  data = []
  working_info['sources'] = []

  if type(space) is not list:
    space = [space]

  for source in space:
    for path in glob.iglob(source):
      if os.path.isfile(path):
        datetime, make, model, digest, mime, code, size, atime, mtime, ctime, has_json = identify_file(path, os.path.split(path)[1])
        data.append([*os.path.split(path), datetime, make, model, digest, code, size, atime, mtime, ctime, has_json ])
      else:
        working_info['sources'].append(path)
        # Pre-calculation of data size to process
        total_size = 0
        for p,n,f in os.walk(path):
          n, f = filter_out(n,f)

          for file in f:
            total_size += os.stat(os.path.join(p, file)).st_size
      
        # Gigabytes instead of Gibibytes
        with tqdm(total=total_size, unit='B', unit_scale=True, unit_divisor=1000) as pbar:
          for p,n,f in os.walk(path):
            n, f = filter_out(n,f)
            for file in tqdm(f):
              pbar.update(os.stat(os.path.join(p, file)).st_size)
              datetime, make, model, digest, mime, code, size, atime, mtime, ctime, has_json = identify_file(os.path.join(p,file), file)
              if code is None:
                continue
      
              data.append([p, file, datetime, make, model, digest, mime, code, size, atime, mtime, ctime, has_json ])
              # file sizes # pbar.update()

  return data

def extract_data(space, datafile = None, working_info=None, verbose=0, force=False):
  if verbose >= 1: print("As a list of spaces has been specified, analysis will take place\n")

  if not working_info:
    working_info = { 'wd': [os.getcwd()],
                     'hostname': [os.uname()[1]] }
  data = explore(space, working_info)

  if len(data) > 0:
    print("\n{} entries ({} parsed by Hachoir)".format(len(data), count_hachoir))

    ph = pd.DataFrame(data, columns=['folder', 'name', 'datetime', 'make', 'model', 'digest', 'mime', 'code', 'size', 'atime', 'mtime', 'ctime', 'has_json' ])
    # split into OK and ERROR files
    ph_ok, ph_error = ph[ph.code == CODE_OK].copy(), ph[ph.code != CODE_OK].copy()
    print("{} ok / {} error".format(len(ph_ok), len(ph_error)))

    # Add dummy time to 'timeless' timestamps. Tag those entries as well.
    dates_with_no_time = ~ph_ok.datetime.str.match("^\d{4}:\d{2}:\d{2} ")   
    full_datetimes = ph_ok.loc[dates_with_no_time].datetime + " 08:00:00"
    ph_ok['timeless'] = False
    ph_ok.loc[dates_with_no_time, 'timeless'] = True
    ph_ok.loc[dates_with_no_time, 'datetime'] = full_datetimes
    ph_ok.loc[:, 'datetime'] = pd.to_datetime(ph_ok['datetime'], format="%Y:%m:%d %H:%M:%S")

    # Save data
    if datafile:
      datafilename = storage.normalize(datafile)
      create_file = True
      if os.path.isfile(datafilename):  
        create_file = force or confirm(
          suffix="(y/N)",
          message="Do you want to overwrite existing datafile '{}'?".format(datafilename))
        if not create_file:
          print("NOT overwritting '{}{}{}'".format(Fore.YELLOW, datafilename, Fore.RESET))
        else:
          print("overwritting '{}{}{}'".format(Fore.GREEN, datafilename, Fore.RESET))
      if create_file:
        ph_ok.to_hdf(datafilename, key='ok', format="table")
        ph_error.to_hdf(datafilename, key='error', format="table")
        pd.DataFrame(working_info).to_hdf(datafilename, key='info', format="table")
