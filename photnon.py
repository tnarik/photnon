#!/usr/bin/env python3

# piexif is faster than Hachoir, and so has prececence
# Even if it cannot treat as many files, the speed gain is worthy
#

import piexif
import os
import sys
import tables.scripts.ptrepack as ptrepack
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

import tempfile

import argparse

from colorama import init, Fore
init(autoreset=True)

CODE_OK = 0
CODE_ERROR = 1
CODE_INVALIDIMAGEDATA = 2
CODE_WEIRD = 3
CODE_SIZE_ZERO = 4

REMOVAL_CODE_IGNORE = 0
REMOVAL_CODE_POSIBLE = 1
REMOVAL_CODE_SCHEDULE = 2
REMOVAL_CODE_KEEP = 3

REMOVAL_CODE_LEGEND = { REMOVAL_CODE_IGNORE: "Ignore", 
                        REMOVAL_CODE_POSIBLE: "Considered",
                        REMOVAL_CODE_SCHEDULE: "Scheduled",
                        REMOVAL_CODE_KEEP: "Kept"}


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

  return (datetime, make, model, digest, mime, code, stats.st_size, atime, mtime, ctime)

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
        datetime, make, model, digest, mime, code, size, atime, mtime, ctime = identify_file(path, os.path.split(path)[1])
        data.append([*os.path.split(path), datetime, make, model, digest, code, size, atime, mtime, ctime ])
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
              datetime, make, model, digest, mime, code, size, atime, mtime, ctime = identify_file(os.path.join(p,file), file)
              if code is None:
                continue
      
              data.append([p, file, datetime, make, model, digest, mime, code, size, atime, mtime, ctime ])
              # file sizes # pbar.update()

  return data

def bsize_value(value):
  unit_divisor=1000
  units = {0: 'B',
           1: 'kB',
           2: 'MB',
           3: 'GB',
           4: 'TB'
    }

  level = 0
  while True:
    if value < unit_divisor: break
    value = value/unit_divisor
    level +=1
  return(value, units[level])

def all_folders(folders, result = set()):
  if isinstance(folders, np.ndarray):
    folders = folders.tolist()
  if not isinstance(folders, list):
    folders = [folders]
      
  for folder in folders:
    result.add(folder)
    head, tail = os.path.split(folder)
    if head and tail:
      all_folders(head, result)
  return result

def report_dupes(photos_df, dup_indexes, goal = None):
  print("{}Remove report:{}".format(Fore.GREEN,Fore.RESET))
  print("schedule removal: listed {} / total: {}".format(
          sum(photos_df[dup_indexes].should_remove == REMOVAL_CODE_SCHEDULE),
          sum(photos_df.should_remove == REMOVAL_CODE_SCHEDULE)
        ))
  if goal:
    if goal == sum(photos_df.should_remove == REMOVAL_CODE_SCHEDULE):
      print("  {} - REMOVAL GOAL ACHIEVED{}".format(Fore.GREEN,Fore.RESET))
    else:
      print("  {} - REMOVAL GOAL NOT YET ACHIEVED {} ({} instead of {})".format(Fore.YELLOW,Fore.RESET, sum(photos_df.should_remove == REMOVAL_CODE_SCHEDULE), goal))

  #print("{}Remove report (duplicated entries):\n{}{}".format(Fore.GREEN,Fore.RESET,
  #            (photos_df[dup_indexes].should_remove.value_counts(sort=False).rename(REMOVAL_CODE_LEGEND)).to_string()))
  #print("{}Remove report (total entries):\n{}{}".format(Fore.GREEN,Fore.RESET,
  #            (photos_df.should_remove.value_counts(sort=False).rename(REMOVAL_CODE_LEGEND)).to_string()))

def generate_dupes_info(photos_df, dup_indexes):
    if len(photos_df[dup_indexes]) == 0:
      return

    print("{}Remove pre-report{}".format(Fore.GREEN,Fore.RESET))
    # All that shouldn't be removed yet (and therefore susceptible of analysis)
    photos_df_dups = photos_df.loc[dup_indexes][photos_df.loc[dup_indexes].should_remove != REMOVAL_CODE_SCHEDULE]
    list_digest = photos_df_dups.digest.value_counts()
    print("entries in process: {} / possible removal: {}".format(
        len(photos_df_dups),
        len(photos_df_dups) - len(list_digest)
      ))
    # Which from the intended set can we really check now?
    # Let's reduce the intended set based on duplicates ocurring within it
    list_digest_dup = list_digest[list_digest > 1].index.values
    photos_df_dups = photos_df_dups[photos_df_dups.digest.isin(list_digest_dup)]
    print("filtered entries in process: {} / possible removal: {}".format(
        len(photos_df_dups),
        len(photos_df_dups) - len(list_digest_dup)
      ))

    ''' to speed up testing '''
    preferred_folder = '../flickr_backup/_whole'

    '''
        folders = sorted(all_folders(photos_df_dups.folder.unique()))
        folder_completer = WordCompleter(folders)
    
        validator = Validator.from_callable(
          lambda x: x in folders,
          error_message='not a valid folder',
          move_cursor_to_end=True)
        preferred_folder = prompt('Enter the preferred folder (use [TAB]): ', completer=folder_completer,
          validator=validator)
    '''  

    # Preserve 'preferred folder'. This doesn't work when duplicates are on the same one
    persist_candidates_list = photos_df_dups['folder'].str.match(preferred_folder)
    persist_candidates = photos_df_dups[persist_candidates_list].index
  
    photos_df.loc[photos_df_dups.index, 'should_remove'] = REMOVAL_CODE_POSIBLE

    decide_removal_entries = photos_df.loc[photos_df_dups.index]

    def select_best_alternative(alternatives, msg):
      '''
      This works like this:
      1. If a single alternative, choose that: This is the case for name based duplication (preferred folder wins)
      2. If several are equivalent, choose the first one: they have the same name
      3. Choose better name when times match (have different clauses depending on which times match, just in case) 
      4. If the only match is the digest (check it again), then choose the lower 'mtime' (older file)
      5. No alternative is good
      '''
      # Single alternative
      if len(alternatives) == 1:
        return alternatives.iloc[0]

      # All alternatives are equivalent
      if (alternatives['name'].nunique() == 1) and (alternatives['mtime'].nunique() == 1) and (alternatives['datetime'].nunique() == 1):
        # If names, mtime and datetime_date are the same, choose at random (the first one, for instance)
        #print(alternatives['name'].unique()[0])
        return alternatives.iloc[0]
      
      if (alternatives['mtime'].nunique() == 1) and (alternatives['datetime'].nunique() == 1):
        return alternatives.loc[alternatives['name'].str.split('.', expand=True)[0].sort_values().index[0]]
      elif (alternatives['datetime'].nunique() == 1):
        # Internal time is the same
        return alternatives.loc[alternatives['name'].str.split('.', expand=True)[0].sort_values().index[0]]
      elif (alternatives['digest'].nunique() == 1):
        return alternatives.sort_values('mtime').iloc[0]

      # Really, at least the datetime should be equivalent
      return None


    def decide_removal_action(x, photos_df, persist_candidates, decide_removal_entries):
      master_candidates = photos_df.loc[persist_candidates][photos_df.loc[persist_candidates].digest==x.digest]
      if len(master_candidates) == 1:
        # Single master candidate (we want to keep the entry if that's the master) 
        if master_candidates.index == x.name:
          return {'persist_version': -1, 'should_remove': REMOVAL_CODE_KEEP}
        else:
          return {'persist_version':master_candidates.index[0] , 'should_remove':REMOVAL_CODE_SCHEDULE}
      elif len(master_candidates) > 1:
        # Several master candidates
        best_alternative = select_best_alternative(master_candidates, 'preferred')
        if (best_alternative is not None) and (best_alternative.name != x.name):
          return {'persist_version': best_alternative.name, 'should_remove':REMOVAL_CODE_SCHEDULE}
        else:
          # Either there is no alternative or right now there is only one: this entry
          return {'persist_version': -1, 'should_remove':REMOVAL_CODE_POSIBLE}
      else:
        # No master candidates, let the algorithm decide
        best_alternative = select_best_alternative(decide_removal_entries[decide_removal_entries.digest==x.digest], 'digests')
        if (best_alternative is not None) and (best_alternative.name != x.name):
          return {'persist_version': best_alternative.name, 'should_remove':REMOVAL_CODE_SCHEDULE}
        else:
          # Either there is no alternative or right now there is only one: this entry
          return {'persist_version': -1, 'should_remove':REMOVAL_CODE_POSIBLE}


    photos_df.loc[photos_df_dups.index, ['persist_version', 'should_remove']] = decide_removal_entries.apply(
          lambda x: decide_removal_action(x, photos_df, persist_candidates, decide_removal_entries),
          axis=1, result_type='expand')


      
def produce_dupes_script(photos_df, dup_indexes, dupes_script="dupes.sh"):
    str = ""
    for i, p in photos_df.loc[dup_indexes][photos_df.loc[dup_indexes, 'should_remove'] == REMOVAL_CODE_SCHEDULE].sort_values('digest').iterrows():
      try:
        keeper = photos_df.loc[int(p.persist_version)]
      except:
        #print(p)
        keeper = {'folder':'', 'name':''}
      
      str += "diff \"{}\" \"{}\"".format(os.path.join(keeper['folder'], keeper['name']), os.path.join(p['folder'], p['name']))
      str += " && echo \'rm \"{}\"\'\n".format(os.path.join(p['folder'], p['name']))
    with open(dupes_script, 'w') as f:
        f.write(str)


if __name__ == "__main__":
  # working_info adds information about the data extraction process (as the files were last accessed in that context)
  working_info = { 'wd': [os.getcwd()],
                   'hostname': [os.uname()[1]] }
  parser = argparse.ArgumentParser(description="Photon")
  parser.add_argument('-d', '--data',
            nargs='+',
            help='data files. if -s is used, only the first datafile will be taken into account',
            dest='datafiles')
  parser.add_argument('-o', '--output',
            nargs='?',
            const='',
            help='generate output datafile. if no file is indicated, the input one will overwriten (if there is just one)')
  parser.add_argument('-v', '--verbose',
            help='verbose output',
            action='count',
            default=0)
  parser.add_argument('-f', '--force',
            help='force reprocessing of all entries of a datafile',
            action='store_true')
  parser.add_argument('-r', '--repack_off',
            help='disable repack output datafile after processing',
            action='store_false',
            dest='repack')
  parser.add_argument('-s', '--space',
            nargs='+',
            help='files, folders or pattern space to explore',
            dest='space')

  args = parser.parse_args()

  data = None
  if args.datafiles:
    print("datafiles '{}{}{}' have been specified\n".format(Fore.GREEN, ' / '.join(args.datafiles), Fore.RESET))
  else:
    print('NO datafiles specified')


  if args.space:
    print("As a list of spaces has been specified, analysis will take place\n")
    data = explore(args.space)  
    if len(data) > 0:
      print("{} entries".format(len(data)))

      ph = pd.DataFrame(data, columns=['folder', 'name', 'datetime', 'make', 'model', 'digest', 'mime', 'code', 'size', 'atime', 'mtime', 'ctime' ])

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
      if args.datafiles:
        datafilename = "{}.pho".format(args.datafiles[0])
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
          pd.DataFrame(working_info).to_hdf(datafilename, key='info', format="table")

      #print(ph_ok.datetime)
      print("{} ok / {} error".format(len(ph_ok), len(ph_error)))
      #print(ph_error)
      print("parsed by Hachoir: {}".format(count_hachoir))
  else:
    if not args.datafiles:
      parser.print_help()
    else:
      computed_columns = ['mtime_date', 'datetime_date'] # Values that cannot be stored as HDF and are computable
      divergent_columns = ['atime', 'ctime', 'should_remove', 'persist_version'] # Values which might differ without impacting file identity (some are computed)

      ph_ok_orig = pd.DataFrame()
      ph_error_orig = pd.DataFrame()
      ph_working_info = pd.DataFrame()
      for datafile in args.datafiles:
        store = pd.HDFStore("{}.pho".format(datafile))
        if '/info' in store:
          store.close()
          last_working_info = pd.read_hdf("{}.pho".format(datafile), key='info').iloc[0]
          if last_working_info['hostname'] != working_info['hostname'][0]:
            print("Data file was generated at {}, but analysis is running on {}".format(
                last_working_info['hostname'], working_info['hostname'][0]
              ))
          ph_working_info = pd.concat([ph_working_info, last_working_info])
          print(ph_working_info)
        else:
          store.close()
          print("{}Datafile '{}{}{}' doesn't contain 'info':{} be extra vigilant\n".format(Fore.RED, Fore.GREEN, datafile, Fore.RED, Fore.RESET))

        ph_ok_orig = pd.concat([ph_ok_orig, pd.read_hdf("{}.pho".format(datafile), key='ok')])
        ph_error_orig = pd.concat([ph_error_orig, pd.read_hdf("{}.pho".format(datafile), key='error')])

      if not args.force:
        if 'should_remove' in ph_ok_orig.columns:
          ph_ok = ph_ok_orig.loc[ph_ok_orig[ph_ok_orig.should_remove != REMOVAL_CODE_SCHEDULE].index]
        else:
         ph_ok = ph_ok_orig
        if 'should_remove' in ph_error_orig.columns:
          ph_error = ph_error_orig.loc[ph_error_orig[ph_error_orig.should_remove != REMOVAL_CODE_SCHEDULE].index]
        else:
          ph_error = ph_error_orig
      else:
        ph_ok = ph_ok_orig
        ph_error = ph_error_orig

      num_ok = len(ph_ok)
      num_error = len(ph_error)
      num_total = num_ok + num_error
      print("total: {} (ok: {}/ error: {})".format(num_total, num_ok, num_error))
      print("{}================================".format(Fore.YELLOW))
      print("{}% errors{}: {:.2%}".format(Fore.GREEN, Fore.RESET, num_error/ num_total))

      ph_ok['mtime_date'] = ph_ok.mtime.apply(lambda x: x.date())
      ph_ok['datetime_date'] = ph_ok.datetime.apply(lambda x: x.date())
      ph_ok['second_discrepancy'] = (ph_ok.datetime - ph_ok.mtime).apply(lambda x: abs(x.total_seconds()))

      print("{}% matching times{}: {:.2%}".format(Fore.GREEN,Fore.RESET,
                (ph_ok.mtime == ph_ok.datetime).value_counts(normalize=True, sort=False)[True]))

      print("{}% matching dates{}: {:.2%}".format(Fore.GREEN,Fore.RESET,
                (ph_ok.mtime_date == ph_ok.datetime_date).value_counts(normalize=True)[True]))

      print("{0}% with discrepancy{1}:{0} 1 minute:{1} {2:.2%} {0}/ 1 hour:{1} {3:.2%} {0}/ 1 day:{1} {4:.2%}".format(Fore.GREEN,Fore.RESET,
                sum(ph_ok.second_discrepancy <= 60)/len(ph_ok),
                sum(ph_ok.second_discrepancy <= 3600)/len(ph_ok),
                sum(ph_ok.second_discrepancy <= 24*3600)/len(ph_ok)))

      print("{}% timeless{}: {:.2%}".format(Fore.GREEN,Fore.RESET,
                len(ph_ok[ph_ok.timeless])/len(ph_ok)))
      if len(ph_ok[ph_ok.timeless]) > 0:
        print("{0}% with discrepancy (timeless) {1}:{0} 1 minute:{1} {2:.2%} {0}/ 1 hour:{1} {3:.2%} {0}/ 1 day:{1} {4:.2%}".format(Fore.GREEN,Fore.RESET,
                  sum(ph_ok[ph_ok.timeless].second_discrepancy <= 60)/len(ph_ok[ph_ok.timeless]),
                  sum(ph_ok[ph_ok.timeless].second_discrepancy <= 3600)/len(ph_ok[ph_ok.timeless]),
                  sum(ph_ok[ph_ok.timeless].second_discrepancy <= 24*3600)/len(ph_ok[ph_ok.timeless])))

      for label, photos_df in [("OK", ph_ok), ("ERROR", ph_error)]:
        print("{}================================ {} set".format(Fore.YELLOW, label))
        # All duplicates
        dup_full = photos_df.duplicated(keep=False, subset=photos_df.columns[1:].drop(divergent_columns, errors='ignore'))
        dup_full_except_first = photos_df.duplicated(keep='first', subset=photos_df.columns[1:].drop(divergent_columns, errors='ignore'))
        # Not used, but I was logging that before
        dup_full_reduced = dup_full ^ dup_full_except_first

        #if len(photos_df[dup_full]) > 0:
        #  print("{}- in folders:\n{}{}".format(Fore.GREEN,Fore.RESET,
        #            (photos_df[dup_full].folder.value_counts()).to_string()))
  
        # Digest duplicates, computed differently. Clearly slower and more complex (more lines) but it doesn't matter that much
        list_digest = photos_df.digest.value_counts()
        dup_digest_values = list_digest[list_digest > 1].index.values
        dup_digest = photos_df.digest.isin(dup_digest_values)
        dup_digest_except_first = photos_df.duplicated(keep='first', subset=['digest'])

        #if len(photos_df[dup_digest]) > 0:
        #  print("{}- in folders:\n{}{}".format(Fore.GREEN,Fore.RESET,
        #          (photos_df[dup_digest].folder.value_counts()).to_string()))
        print("photos {}, after reducing:".format(
          len(photos_df)
          ))
        print("   - full   -> {} (removing {} and processing {})".format(
          sum(~dup_full_except_first),
          sum(dup_full_except_first),
          sum(dup_full)
          ))
        print("   - digest -> {} (removing {} and processing {})".format(
          sum(~dup_digest_except_first),
          sum(dup_digest_except_first),
          sum(dup_digest)
          ))

        if 'size' in photos_df:
          print("size {:.3f} {}, after reducing:".format(
            *bsize_value(photos_df['size'].sum())
            ))
          print("   - full   -> {:.3f} {} (removing {:.3f} {} and processing {:.3f} {})".format(
            *bsize_value(photos_df[~dup_full_except_first]['size'].sum()),
            *bsize_value(photos_df[dup_full_except_first]['size'].sum()),
            *bsize_value(photos_df[dup_full]['size'].sum())
            ))
          print("   - digest -> {:.3f} {} (removing {:.3f} {} and processing {:.3f} {})".format(
            *bsize_value(photos_df[~dup_digest_except_first]['size'].sum()),
            *bsize_value(photos_df[dup_digest_except_first]['size'].sum()),
            *bsize_value(photos_df[dup_digest]['size'].sum())
            ))
        print()

        result = True
        '''confirm(
              suffix="(y/N)",
              message="Do you want to process OK duplicates?")'''
  
        if result:
          #if confirm(
          #      suffix="(y/N)",
          #      message="Generate simple HTML for inspection?"):
          #  print("HTML will be generated")
          #  str = "<html><body>"
          #  for i, p in photos_df[dup_full].sort_values(by=['name', 'folder']).iterrows():#.path.values:
          #    src = os.path.join(p['folder'], p['name'])
          #    str+="<div>{}<img src='{}' width='30%'/></div>".format(src, src)
          #  str +="</body></html>"
          #  with open('inspection_OK.html', 'w') as f:
          #    f.write(str)
  
          # Keep everything by default
          photos_df.loc[:, 'should_remove'] = REMOVAL_CODE_IGNORE
          photos_df.loc[:, 'persist_version'] = -1
  
          print("{}   - full -{}".format(Fore.GREEN,Fore.RESET))
          generate_dupes_info(photos_df, dup_full)
          report_dupes(photos_df, dup_full, sum(dup_digest_except_first))
  
          print("{}   - digest -{}".format(Fore.GREEN,Fore.RESET))
          generate_dupes_info(photos_df, dup_digest)
          report_dupes(photos_df, dup_digest, sum(dup_digest_except_first))
          produce_dupes_script(photos_df, dup_digest, "dup_actions_{}.sh".format(label))

      # ALL sets processed
      if args.output is not None:
        if len(args.output) > 0:
          print("use '{}'".format(args.output))
          datafilename = "{}.pho".format(args.output)
        elif len(args.datafiles) == 1:
          print("use '{}'".format(args.datafiles[0]))
          datafilename = "{}.pho".format(args.datafiles[0])
        else:
          print("There are several input datafiles and no single output")
          sys.exit(2)

        ph_ok.drop(computed_columns, axis=1).to_hdf(datafilename, key='ok', format="table")
        ph_error.to_hdf(datafilename, key='error', format="table")
        if ph_working_info:
          # This only make sense when ONE (1) datafile is used as input. If combining, ....
          pd.DataFrame(ph_working_info).to_hdf(datafilename, key='info', format="table")

        if args.repack:
          with tempfile.TemporaryDirectory() as tempdir:
            #tempdir = tempfile.mkdtemp()
            #print(tempdir)
            sys.argv = ['ptrepack', datafilename, os.path.join(tempdir,'repackedfile')]
            ptrepack.main()
            os.rename(datafilename, "{}_back".format(datafilename))
            os.rename(os.path.join(tempdir,'repackedfile'), datafilename)

