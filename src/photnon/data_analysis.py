import os

import pandas as pd

from colorama import init, Fore
init(autoreset=True)

REMOVAL_CODE_IGNORE = 0
REMOVAL_CODE_POSIBLE = 1
REMOVAL_CODE_SCHEDULE = 2
REMOVAL_CODE_KEEP = 3

REMOVAL_CODE_LEGEND = { REMOVAL_CODE_IGNORE: "Ignore", 
                        REMOVAL_CODE_POSIBLE: "Considered",
                        REMOVAL_CODE_SCHEDULE: "Scheduled",
                        REMOVAL_CODE_KEEP: "Kept"}

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


def report_dupes(photos_df, dup_indexes, goal = None, verbose=0):
  if verbose >= 1: print("{}Remove report:{}".format(Fore.GREEN,Fore.RESET))
  if verbose >= 1: print("schedule removal: listed {} / total: {}".format(
                          sum(photos_df[dup_indexes].should_remove == REMOVAL_CODE_SCHEDULE),
                          sum(photos_df.should_remove == REMOVAL_CODE_SCHEDULE)
                        ))
  if goal:
    if goal == sum(photos_df.should_remove == REMOVAL_CODE_SCHEDULE):
      print("      {}REMOVAL GOAL ACHIEVED:{} {}".format(Fore.GREEN,Fore.RESET, goal))
    else:
      print("      {}REMOVAL GOAL NOT YET ACHIEVED {} ({} instead of {})".format(Fore.YELLOW,Fore.RESET, sum(photos_df.should_remove == REMOVAL_CODE_SCHEDULE), goal))

  #print("{}Remove report (duplicated entries):\n{}{}".format(Fore.GREEN,Fore.RESET,
  #            (photos_df[dup_indexes].should_remove.value_counts(sort=False).rename(REMOVAL_CODE_LEGEND)).to_string()))
  #print("{}Remove report (total entries):\n{}{}".format(Fore.GREEN,Fore.RESET,
  #            (photos_df.should_remove.value_counts(sort=False).rename(REMOVAL_CODE_LEGEND)).to_string()))

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
    #print("best -----",master_candidates, '-----',best_alternative)
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

def generate_dupes_info(photos_df, dup_indexes, verbose=0):
  if len(photos_df[dup_indexes]) == 0:
    return

  if verbose >= 1: print("{}Remove pre-report{}".format(Fore.GREEN,Fore.RESET))
  # All that shouldn't be removed yet (and therefore susceptible of analysis)
  photos_df_dups = photos_df.loc[dup_indexes][photos_df.loc[dup_indexes].should_remove != REMOVAL_CODE_SCHEDULE]
  list_digest = photos_df_dups.digest.value_counts()
  if verbose >= 1: print("entries in process: {} / possible removal: {}".format(
                          len(photos_df_dups),
                          len(photos_df_dups) - len(list_digest)
                        ))
  # Which from the intended set can we really check now?
  # Let's reduce the intended set based on duplicates ocurring within it
  list_digest_dup = list_digest[list_digest > 1].index.values
  photos_df_dups = photos_df_dups[photos_df_dups.digest.isin(list_digest_dup)]
  if verbose >= 1: print("filtered entries in process: {} / possible removal: {}".format(
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

def read_datafiles(running_working_info, datafiles, deduplicate=True):
  ph_working_info = pd.DataFrame()
  ph_ok = pd.DataFrame()
  ph_error = pd.DataFrame()

  for datafile in datafiles:
    last_working_info = None
    store = pd.HDFStore("{}.pho".format(datafile))
    if '/info' in store:
      store.close()
      last_working_info = pd.read_hdf("{}.pho".format(datafile), key='info')
      if last_working_info.loc[0, 'hostname'] != running_working_info['hostname'][0]:
        print("Data file was generated at {}, but analysis is running on {}".format(
            last_working_info.loc[0, 'hostname'], running_working_info['hostname'][0]
          ))
      ph_working_info = pd.concat([ph_working_info, last_working_info])
      #print(ph_working_info)
    else:
      store.close()
      print("{}Datafile '{}{}{}' doesn't contain 'info':{} be extra vigilant\n".format(Fore.RED, Fore.GREEN, datafile, Fore.RED, Fore.RESET))
  
    ph_ok = pd.concat([ph_ok, pd.read_hdf("{}.pho".format(datafile), key='ok')])
    ph_error = pd.concat([ph_error, pd.read_hdf("{}.pho".format(datafile), key='error')])
    if last_working_info is not None:
      # Sanitize folders
      wd = last_working_info.loc[0, 'wd']
      ph_ok['folder'] = ph_ok.folder.apply(lambda x: os.path.realpath(os.path.join(wd, x)))
      ph_error['folder'] = ph_error.folder.apply(lambda x: os.path.realpath(os.path.join(wd, x)))


  num_read_ok = len(ph_ok)
  num_read_error = len(ph_error)
  if deduplicate:
    ph_ok = ph_ok.drop_duplicates(keep='first')
    ph_error = ph_error.drop_duplicates(keep='first')
  
  return ph_working_info, ph_ok, ph_error, num_read_ok, num_read_error
