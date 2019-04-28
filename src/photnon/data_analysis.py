import os

import pandas as pd
from tqdm import tqdm
tqdm.pandas()

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

PERSIST_VERSION_KEEP = -1

LOG_PROGRESS_THRESHOLD = 2000

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

  num_name = alternatives['name'].nunique()
  num_mtime = alternatives['mtime'].nunique()
  num_datetime = alternatives['datetime'].nunique()
  # All alternatives are equivalent
  if (num_name == 1) and (num_mtime == 1) and (num_datetime == 1):
    # If names, mtime and datetime_date are the same, choose at random (the first one, for instance)
    #print(alternatives['name'].unique()[0])
    return alternatives.iloc[0]

  if (num_mtime == 1) and (num_datetime == 1):
    return alternatives.loc[alternatives['name'].str.split('.', expand=True)[0].sort_values().index[0]]

  if (num_datetime == 1):
    # Internal time is the same
    return alternatives.loc[alternatives['name'].str.split('.', expand=True)[0].sort_values().index[0]]

  num_digest = alternatives['digest'].nunique()
  if (num_digest == 1):
    return alternatives.sort_values('mtime').iloc[0]

  # Really, at least the datetime or the digest should be equivalent
  return None


def decide_removal_action(x, persist_candidates, decide_removal_entries):
#def decide_removal_action(x, decide_removal_entries):
  #master_candidates = persist_candidates[persist_candidates.digest==x.digest]
  if x.digest in persist_candidates.index:
    master_candidates = persist_candidates.loc[x.digest]
    if len(master_candidates) == 1:
      # Single master candidate (we want to keep the entry if that's the master)
      if master_candidates['index'] != x.name:
        return {'persist_version':master_candidates.iloc[0, 'index'] , 'should_remove':REMOVAL_CODE_SCHEDULE}
      else:
        return {'persist_version': PERSIST_VERSION_KEEP, 'should_remove': REMOVAL_CODE_KEEP}

    if len(master_candidates) > 1:
      # Several master candidates
      best_alternative = select_best_alternative(master_candidates.set_index('index'), 'preferred')
      #print("best -----",master_candidates, '-----',best_alternative)
      if (best_alternative is not None) and (best_alternative.name != x.name):
        return {'persist_version': best_alternative.name, 'should_remove':REMOVAL_CODE_SCHEDULE}
      else:
        # Either there is no alternative or right now there is only one: this entry
        return {'persist_version': PERSIST_VERSION_KEEP, 'should_remove':REMOVAL_CODE_POSIBLE}

  # No master candidates, let the algorithm decide
  #return {'persist_version': PERSIST_VERSION_KEEP, 'should_remove':REMOVAL_CODE_POSIBLE}
  best_alternative = select_best_alternative(decide_removal_entries, 'digests')
  #return {'persist_version': PERSIST_VERSION_KEEP, 'should_remove':REMOVAL_CODE_POSIBLE}
  if (best_alternative is not None) and (best_alternative.name != x.name):
    #if best_alternative.name == 86127 or best_alternative.name == 5410 or best_alternative.name == 86795:
    #  print("digest alternative for ", x, best_alternative)
    return {'persist_version': best_alternative.name, 'should_remove':REMOVAL_CODE_SCHEDULE}
  else:
    # Either there is no alternative or right now there is only one: this entry
    return {'persist_version': PERSIST_VERSION_KEEP, 'should_remove':REMOVAL_CODE_POSIBLE}

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
  persist_candidates_index = photos_df_dups[persist_candidates_list].index
  persist_candidates = photos_df.loc[persist_candidates_index]
  persist_candidates = persist_candidates.reset_index().set_index('digest',drop=False)

  photos_df.loc[photos_df_dups.index, 'should_remove'] = REMOVAL_CODE_POSIBLE

  decide_removal_entries = photos_df.loc[photos_df_dups.index]
  if len(decide_removal_entries) > LOG_PROGRESS_THRESHOLD:
    decide_removal = decide_removal_entries.progress_apply
  else:
    decide_removal = decide_removal_entries.apply

  decide_removal_entries = decide_removal_entries.reset_index().set_index('digest',drop=False)
  photos_df.loc[photos_df_dups.index, ['persist_version', 'should_remove']] = decide_removal(
#          lambda x: decide_removal_action(x, decide_removal_entries.loc[x.digest].set_index('index')),
          lambda x: decide_removal_action(x, persist_candidates, decide_removal_entries.loc[x.digest].set_index('index')),
          axis=1, result_type='expand')

  # Once all content duplicates have been identified and tagged for removal, let's double check we are not doing anything stupid
  # This block guarantees that all refered masters (persist_version) are kept.
  # It does this by reassigning the underlying master
  # The scenario is when there are two sets of name duplicates which ALSO match digests
  # By the time digests are processed, there would be to-be-removed entries used as reference for already-pending-removal entries
  all_replaceable = photos_df.loc[photos_df[photos_df.persist_version != PERSIST_VERSION_KEEP].index]
  if any(all_replaceable.persist_version.isin(all_replaceable.index)):
    print("{}some master files are also replaceable. fixing".format(Fore.YELLOW))
    new_persist_version = all_replaceable[all_replaceable.persist_version.isin(all_replaceable.index)].apply(
      lambda x: all_replaceable.loc[x.persist_version].persist_version,
#      lambda x: print(x.name, x.persist_version, all_replaceable.loc[x.persist_version]),
      axis=1 )
    all_replaceable.loc[new_persist_version.index, 'persist_version'] = new_persist_version.values

  all_persisted_version_kept = all(photos_df.loc[all_replaceable.persist_version].persist_version == PERSIST_VERSION_KEEP)
  if not all_persisted_version_kept:
    raise Exception("Some file intented as a master are to be removed!")

      
def produce_dupes_script(photos_df, dup_indexes, dupes_script="dupes.sh"):
  str = ""
  '''
  if [[ $(hostname) != 'Wintermute-Manoeuvre.local' ]]; then
    echo 'SCRIPT run on a different machine, confirm to proceed';
  fi
  '''
  for i, p in photos_df.loc[dup_indexes][photos_df.loc[dup_indexes, 'should_remove'] == REMOVAL_CODE_SCHEDULE].sort_values('digest').iterrows():
    try:
      keeper = photos_df.loc[int(p.persist_version)]
    except:
      #print(p)
      keeper = {'folder':'', 'name':''}
    
    if os.path.join(keeper['folder'], keeper['name']) == os.path.join(p['folder'], p['name']):
      print("{}Why would we want to diff a file with itself?{}: {}".format(Fore.RED, Fore.RESET, os.path.join(p['folder'], p['name'])))

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

  # Because persist_version references indexes...
  ph_ok.reset_index(inplace=True)
  if 'persist_version' in ph_ok.columns:
    map_reindex = {v:k for k,v in ph_ok['index'].to_dict().items()}
    ph_ok['persist_version'] = ph_ok['persist_version'].transfom(lambda x: map_reindex[x])
  ph_ok.drop('index', axis=1, inplace=True)
  ph_error.reset_index(inplace=True)
  if 'persist_version' in ph_error.columns:
    map_reindex = {v:k for k,v in ph_error['index'].to_dict().items()}
    ph_error['persist_version'] = ph_error['persist_version'].transfom(lambda x: map_reindex[x])
  ph_error.drop('index', axis=1, inplace=True)

  num_read_ok = len(ph_ok)
  num_read_error = len(ph_error)
  if deduplicate:
    ph_ok = ph_ok.drop_duplicates(keep='first')
    ph_error = ph_error.drop_duplicates(keep='first')
  
  return ph_working_info, ph_ok, ph_error, num_read_ok, num_read_error
