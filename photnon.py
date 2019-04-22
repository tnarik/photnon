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
HachoirConfig.quiet = True

from prompt_toolkit.shortcuts import confirm
from prompt_toolkit import prompt
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.validation import Validator, ValidationError

import glob
import time
import pandas as pd
import numpy as np
from datetime import datetime as dt

import argparse

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
def identify_file(path, name):
  global count_hachoir
  datetime = None
  make = None
  model = None
  digest = None
  mime = None
  code = CODE_OK

  stats = os.stat(path)
  atime = dt.fromtimestamp(stats.st_atime)
  mtime = dt.fromtimestamp(stats.st_mtime)
  ctime = dt.fromtimestamp(stats.st_ctime)
  if stats.st_size == 0:
    code = CODE_SIZE_ZERO
  else:
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
  
            if 'Metadata' in metadata:
              datetime = metadata['Metadata']['creation_date'].replace('-', ':')
            elif 'Common' in metadata:
              datetime = metadata['Common']['creation_date'].replace('-', ':')
  
            code = CODE_OK
            count_hachoir += 1
            if args.verbose: print("   {}NOW! - {}".format(Fore.GREEN, name))
        except:
          if args.verbose > 1: print("{} - {}".format(name, metadata))
      else:
        if args.verbose: print("   {}NOT even NOW - {}".format(Fore.RED, name))

  return (datetime, make, model, digest, mime, code, atime, mtime, ctime)

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
        datetime, make, model, digest, mime, code, atime, mtime, ctime = identify_file(path, os.path.split(path)[1])
        data.append([*os.path.split(path), datetime, make, model, digest, code, atime, mtime, ctime ])
      else:
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
              datetime, make, model, digest, mime, code, atime, mtime, ctime = identify_file(os.path.join(p,file), file)
              if code is None:
                continue
      
              data.append([p, file, datetime, make, model, digest, mime, code, atime, mtime, ctime ])
              # file sizes # pbar.update()

  return data


def all_folders(folders, result = set()):
  if isinstance(folders, np.ndarray):
    folders = folders.tolist()
  if not isinstance(folders, list):
    folders = [folders]
      
  for folder in folders:
    result.add(folder)
    head, tail = os.path.split(folder)
    if head:
      all_folders(head, result)
  return result

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
  if args.datafile:
    print("datafile '{}{}{}' has been specified\n".format(Fore.GREEN, args.datafile, Fore.RESET))
  else:
    print('NO datafile specified')


  if args.space:
    print("As a list of spaces has been specified, analysis will take place\n")
    data = explore(args.space)  
    if len(data) > 0:
      print("{} entries".format(len(data)))

      ph = pd.DataFrame(data, columns=['folder', 'name', 'datetime', 'make', 'model', 'digest', 'mime', 'code', 'atime', 'mtime', 'ctime' ])

      # split into OK and ERROR files
      ph_ok, ph_error = ph[ph.code == CODE_OK].copy(), ph[ph.code != CODE_OK].copy()

      # Add dummy time to 'timeless' timestamps. Tag those entries as well.
      dates_with_no_time = ~ph_ok.datetime.str.match("^\d{4}:\d{2}:\d{2} ")   
      full_datetimes = ph_ok.loc[dates_with_no_time].datetime + " 08:00:00"
      ph_ok['timeless'] = False
      ph_ok.loc[dates_with_no_time, 'timeless'] = True
      ph_ok.loc[dates_with_no_time, 'datetime'] = full_datetimes
      ph_ok.loc[:, 'datetime'] = pd.to_datetime(ph_ok['datetime'], format="%Y:%m:%d %H:%M:%S")

      # Save data
      if args.datafile:
        datafilename = "{}.pho".format(args.datafile)
        create_file = True
        if os.path.isfile(datafilename):  
          create_file = confirm(
            suffix="(y/N)",
            message="Do you want to overwrite existing datafile '{}'?".format(datafilename))
          if not create_file:
            print("{}NOT overwritting".format(Fore.YELLOW))
          else:
            print("{}overwritting".format(Fore.GREEN))
        if create_file:
          ph_ok.to_hdf(datafilename, key='ok', format="table")
          ph_error.to_hdf(datafilename, key='error', format="table")

      #print(ph_ok.datetime)
      print("{} ok / {} error".format(len(ph_ok), len(ph_error)))
      #print(ph_error)
      print("parsed by Hachoir: {}".format(count_hachoir))
  else:
    if not args.datafile:
      parser.print_help()
    else:
      datafilename = "{}.pho".format(args.datafile)

      ph_ok = pd.read_hdf(datafilename, key='ok')
      ph_error = pd.read_hdf(datafilename, key='error')

      num_ok = len(ph_ok)
      num_error = len(ph_error)
      num_total = num_ok + num_error
      print("total: {} (ok: {}/ error: {})".format(num_total, num_ok, num_error))
      print("{}================================".format(Fore.YELLOW))
      print("{}percentage of errors{}: {} %".format(Fore.GREEN, Fore.RESET, 100* num_error/ num_total))

      ph_ok['mtime_date'] = ph_ok.mtime.apply(lambda x: x.date())
      ph_ok['datetime_date'] = ph_ok.datetime.apply(lambda x: x.date())
      ph_ok['second_discrepancy'] = (ph_ok.datetime - ph_ok.mtime).apply(lambda x: x.total_seconds())

      print("{}matching times (%):\n{}{}".format(Fore.GREEN,Fore.RESET,
                ((ph_ok.mtime == ph_ok.datetime).value_counts(normalize=True, sort=False) *100).to_string()))

      print("{}matching dates (%):\n{}{}".format(Fore.GREEN,Fore.RESET,
                ((ph_ok.mtime_date == ph_ok.datetime_date).value_counts(normalize=True, sort=False) *100).to_string()))

      print("{}less than hour discrepancy (%):\n{}{}".format(Fore.GREEN,Fore.RESET,
                ((ph_ok.second_discrepancy <= 3600).value_counts(normalize=True, sort=False) *100).to_string()))

      print("{}================================ OK set".format(Fore.YELLOW))
      dup_full = ph_ok.duplicated(keep=False, subset=ph_ok.columns[1:].drop(['atime', 'ctime']))
      dup_full_except_first = ph_ok.duplicated(keep='first', subset=ph_ok.columns[1:].drop(['atime', 'ctime']))
      dup_full_reduced = dup_full ^ dup_full_except_first
      print("{}full duplicates:{} {} -> {} (total: {} -> {})".format(Fore.GREEN,Fore.RESET,
         len(ph_ok[dup_full]),
         sum(dup_full_reduced),
         len(ph_ok),
         sum (~dup_full_except_first)
         ))
      if len(ph_ok[dup_full]) > 0:
        print("{}- in folders:\n{}{}".format(Fore.GREEN,Fore.RESET,
                  (ph_ok[dup_full].folder.value_counts()).to_string()))

      list_digest = ph_ok.digest.value_counts()
      dup_digest = list_digest[list_digest > 1].index.values
      print("{}digest duplicates:{} {} -> {} (total: {} -> {})".format(Fore.GREEN,Fore.RESET,
          len(ph_ok[ph_ok.digest.isin(dup_digest)]),
          len(dup_digest),
          len(ph_ok),
          len(list_digest)
          ))
      if len(ph_ok[ph_ok.digest.isin(dup_digest)]) > 0:
        print("{}- in folders:\n{}{}".format(Fore.GREEN,Fore.RESET,
                (ph_ok[ph_ok.digest.isin(dup_digest)].folder.value_counts()).to_string()))

      print("\n")
      result = confirm(
            suffix="(y/N)",
            message="Do you want to process OK duplicates?")

      if result:
        #ph_ok['path']=ph_ok[dup_full].apply(lambda x: os.path.join(x['folder'], x['name']), axis=1)

        if confirm(
              suffix="(y/N)",
              message="Generate simple HTML for inspection?"):
          print("HTML will be generated")
          str = "<html><body>"
          for i, p in ph_ok[dup_full].sort_values(by=['name', 'folder']).iterrows():#.path.values:
            src = os.path.join(p['folder'], p['name'])
            str+="<div>{}<img src='{}' width='30%'/></div>".format(src, src)
          str +="</body></html>"
          with open('inspection_OK.html', 'w') as f:
            f.write(str)

        if len(ph_ok[dup_full]) > 0:
          print("{}--- full duplicates (names, tags, etc)".format(Fore.YELLOW))
  
          folders = sorted(all_folders(ph_ok[dup_full].folder.unique()))
          folder_completer = WordCompleter(folders)
          #class NumberValidator(Validator):
          #  def validate(self, document):
          #    raise ValidationError(message='not a valid folder',
          #                          cursor_position=0)
          print(folders)
          validator = Validator.from_callable(
            lambda x: x in folders,
            error_message='not a valid folder',
            move_cursor_to_end=True)
          preferred_folder = prompt('Enter the preferred folder (use [TAB]): ', completer=folder_completer,
            validator=validator)
  
          # Keep everything by default
          ph_ok.loc[:, 'should_remove'] = False
  
          keep_list = ph_ok.loc[dup_full, 'folder'].str.match(preferred_folder)
          keepers = ph_ok.loc[dup_full][keep_list].index
          removable_digests = ph_ok.loc[keepers, "digest"].unique()
          removable = ph_ok.loc[dup_full][ph_ok.loc[dup_full, "digest"].isin(removable_digests)].index
          # Mark the duplicates with a digest matching those from the preferred folder
          ph_ok.loc[removable, 'should_remove'] = True
          # Keep the preferred folder verion
          ph_ok.loc[keepers, 'should_remove'] = False
          print("{}Should remove report (duplicated entries):\n{}{}".format(Fore.GREEN,Fore.RESET,
                    (ph_ok[dup_full].should_remove.value_counts(sort=False)).to_string()))
          print("{}Should remove report (total entries):\n{}{}".format(Fore.GREEN,Fore.RESET,
                    (ph_ok.should_remove.value_counts(sort=False)).to_string()))
  
          str = ""
          for i, p in ph_ok.loc[dup_full][ph_ok.loc[dup_full, 'should_remove']].iterrows():
            keeper = ph_ok[(ph_ok.should_remove == False) &
                           (ph_ok.digest == p.digest) & 
                           (ph_ok['name'] == p['name'])].iloc[0]
            str += "diff \"{}\" \"{}\"".format(os.path.join(keeper['folder'], keeper['name']), os.path.join(p['folder'], p['name']))
            str += " && echo rm \"{}\"\n".format(os.path.join(p['folder'], p['name']))
          with open('dupes.sh', 'w') as f:
              f.write(str)
    # print("{}================================ error set".format(Fore.YELLOW))
    # dup_full = ph_error.duplicated(keep=False, subset=ph_error.columns[1:].drop(['atime', 'ctime']))
    # print("{}full duplicates:{} {}".format(Fore.GREEN,Fore.RESET, len(ph_error[dup_full])))
    # print("{}- in folders:\n{}{}".format(Fore.GREEN,Fore.RESET,
    #           (ph_error[dup_full].folder.value_counts()).to_string()))


    # list_digest = ph_error.digest.value_counts()
    # dup_digest = list_digest[list_digest > 1].index.values
    # print("{}digest duplicates:{} {}".format(Fore.GREEN,Fore.RESET, len(ph_error[ph_error.digest.isin(dup_digest)])))
    # print("{}- in folders:\n{}{}".format(Fore.GREEN,Fore.RESET,
    #           (ph_error[ph_error.digest.isin(dup_digest)].folder.value_counts()).to_string()))
