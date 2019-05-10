import os
import re
from jinja2 import Environment,PackageLoader
template_env = Environment(
    loader=PackageLoader('photnon', 'templates')
)
def datetimeformat(value, format='%Y'):
    return value.strftime(format)
template_env.filters['datetimeformat'] = datetimeformat
template_env.filters['pathjoin'] = lambda *paths: os.path.join(*paths)


from photnon import storage

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

def select_best_alternative_index(alternatives):
  '''
  This works like this:
  1. If a single alternative, choose that: This is the case for name based duplication (preferred folder wins)
  2. If several are equivalent, choose the first one: they have the same name
  3. Choose better name when times match (have different clauses depending on which times match, just in case) 
  4. If the only match is the digest (check it again), then choose the lower 'mtime' (older file)
  5. No alternative is good

  There are a couple of 'copy' statements. This is to ensure the 'best_criteria' info doesn't pollute the original DFs
  '''
  # Single alternative
  if len(alternatives) == 1:
    return alternatives['index'].values[0]

  num_name = alternatives['name'].nunique(dropna=False)
  num_mtime = alternatives['mtime'].nunique(dropna=False)
  # All alternatives are equivalent
  if (num_name == 1) and (num_mtime == 1):
    # If names, mtime and datetime_date are the same, choose at random (the first one, for instance)
    return alternatives['index'].values[0]

  num_mtime = alternatives['mtime'].nunique(dropna=False)
  if (num_mtime == 1):
    alternatives = alternatives.copy()
    alternatives['best_criteria'] = alternatives['name'].str.split('.', expand=True)[0]
    return alternatives.sort_values(by='best_criteria')['index'].values[0]

  # At least the digest should match
  return alternatives.sort_values('mtime')['index'].values[0]


def decide_removal_action(x, preferred_candidates, decide_removal_entries):
  '''
  'decide_removal_entries' are indexed by digest, with the original 'index' (which links to the original DataFrame) stored in an 'index' column
  'preferred_candidates' is indexed by digest, with the original 'index' (which links to the original DataFrame) stored in an 'index' column
  '''
  if (preferred_candidates is not None) and (x.digest in preferred_candidates.index):
    master_candidates = preferred_candidates.loc[[x.digest]]
    if len(master_candidates) == 1:
      # Single master candidate (we want to keep the entry if that's the master)
      if master_candidates['index'].values[0] != x.name:
        return {'persist_version':master_candidates['index'].values[0], 'should_remove':REMOVAL_CODE_SCHEDULE}
      else:
        return {'persist_version': PERSIST_VERSION_KEEP, 'should_remove': REMOVAL_CODE_KEEP}

    if len(master_candidates) > 1:
      # Several master candidates
      best_alternative = select_best_alternative_index(master_candidates)
      #print("best -----",master_candidates, '-----',best_alternative)
      if (best_alternative is not None) and (best_alternative != x.name):
        return {'persist_version': best_alternative, 'should_remove':REMOVAL_CODE_SCHEDULE}
      else:
        # Either there is no alternative or right now there is only one: this entry
        return {'persist_version': PERSIST_VERSION_KEEP, 'should_remove':REMOVAL_CODE_POSIBLE}

  # No master candidates, let the algorithm decide based on all available
  best_alternative = select_best_alternative_index(decide_removal_entries.loc[x.digest])
  if (best_alternative is not None) and (best_alternative != x.name):
    return {'persist_version': best_alternative, 'should_remove':REMOVAL_CODE_SCHEDULE}
  else:
    # Either there is no alternative or right now there is only one: this entry
    return {'persist_version': PERSIST_VERSION_KEEP, 'should_remove':REMOVAL_CODE_POSIBLE}

def generate_dupes_info(photos_df, dup_indexes, preferred_folder = False, verbose=0):
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

  # Preserve 'preferred folder'. This doesn't work when duplicates are on the same one
  if preferred_folder:
    preferred_candidates_list = photos_df_dups['folder'].str.match(preferred_folder)
    preferred_candidates_index = photos_df_dups[preferred_candidates_list].index
    preferred_candidates = photos_df.loc[preferred_candidates_index]
    preferred_candidates = preferred_candidates.reset_index().set_index('digest',drop=False)
  else:
    preferred_candidates = None

  photos_df.loc[photos_df_dups.index, 'should_remove'] = REMOVAL_CODE_POSIBLE

  decide_removal_entries = photos_df.loc[photos_df_dups.index]
  if len(decide_removal_entries) > LOG_PROGRESS_THRESHOLD:
    decide_removal = decide_removal_entries.progress_apply
  else:
    decide_removal = decide_removal_entries.apply

  # Sorting the index makes the ,loc somewhat faster (2x-3x)
  decide_removal_entries = decide_removal_entries.reset_index().set_index('digest',drop=False).sort_index()
  photos_df.loc[photos_df_dups.index, ['persist_version', 'should_remove']] = decide_removal(
          lambda x: decide_removal_action(x, preferred_candidates, decide_removal_entries),
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
      axis=1 )
    all_replaceable.loc[new_persist_version.index, 'persist_version'] = new_persist_version.values

  all_persisted_version_kept = all(photos_df.loc[all_replaceable.persist_version].persist_version == PERSIST_VERSION_KEEP)
  if not all_persisted_version_kept:
    raise Exception("Some file intented as a master are to be removed!")

      
def produce_dupes_scripts(photos_df, dup_indexes, label=None):
  script_parts = []
  '''
  if [[ $(hostname) != 'Wintermute-Manoeuvre.local' ]]; then
    echo 'SCRIPT run on a different machine, confirm to proceed';
  fi
  '''
  '''
  for i, p in photos_df.loc[dup_indexes][photos_df.loc[dup_indexes, 'should_remove'] == REMOVAL_CODE_SCHEDULE].sort_values('digest').iterrows():
    try:
      keeper = photos_df.loc[int(p.persist_version)]
    except:
      print("{}There should always be a master file for all files to be removed{}".format(Fore.RED, Fore.RESET))
      continue
    
    keeper_path = os.path.join(keeper['folder'], keeper['name'])
    removal_path = os.path.join(p['folder'], p['name'])

    if keeper_path == removal_path:
      print("{}Why would we want to diff a file with itself?{}: {}".format(Fore.RED, Fore.RESET, removal_path))
      continue

    script_parts.append("diff \"{0}\" \"{1}\" && echo \'rm \"{1}\"\'".format(
                    keeper_path, removal_path
                ))

  script_content = "\n".join(script_parts)
  with open("dup_actions_{}.sh".format(label), 'w') as f:
      f.write(script_content)
  '''
  print("{}WITH the duplicates identified and cross-checked, JSON metadata can be generated".format(Fore.YELLOW))


  print("For every processed duplicate that we keep, we want to find the groups with JSON files")
  kept_digests = photos_df.loc[dup_indexes][photos_df.loc[dup_indexes, 'persist_version'] == PERSIST_VERSION_KEEP].digest
  kept_digests_and_json = photos_df.loc[dup_indexes, ['digest', 'has_json']][photos_df.loc[dup_indexes, 'digest'].isin(kept_digests)].groupby('digest').sum()
  print(len(kept_digests_and_json[kept_digests_and_json.has_json == 0]),
        len(kept_digests_and_json[kept_digests_and_json.has_json > 0]))

  a = kept_digests_and_json[kept_digests_and_json.has_json == 0].index
  b = kept_digests_and_json[kept_digests_and_json.has_json > 0].index
  print(len(a.intersection(b)))
  print( len(photos_df[photos_df.digest.isin(a)]), ' (entries without json) +',
          len(photos_df[photos_df.digest.isin(b)]),' (entries with json) = ',
          len(photos_df[photos_df.digest.isin(a.append(b))])
        )

  print("As the count seems ok, let, it is time to process this")
  print("for the {} digests with an existing JSON ".format(len(kept_digests_and_json[kept_digests_and_json.has_json > 0])))
  #for json_digest in kept_digests_and_json[kept_digests_and_json.has_json > 0].index:
  #  print(json_digest)

  photos_dfa = photos_df[['folder', 'name', 'digest', 'has_json', 'persist_version']].copy()
  photos_dfa['fullpath'] = photos_dfa.apply(lambda x: os.path.join(x['folder'], x['name']), axis=1)

  template = template_env.get_template('dedup')
  #print(template.render(entries=photos_df[photos_df['should_remove'] != REMOVAL_CODE_SCHEDULE]))
  grouped = photos_dfa.loc[dup_indexes][photos_dfa.loc[dup_indexes, 'digest'].isin(kept_digests)].groupby('digest')

  template.stream(digest_groups = grouped,
                  KEPT_MARK=PERSIST_VERSION_KEEP).dump("dedup_{}.sh".format(label))

#  2375:    "ph_ok[dup_digest].groupby('digest', sort=True).apply(lambda x: x)"
  #print(photos_df.loc[dup_indexes][photos_df.loc[dup_indexes, 'should_remove'] == REMOVAL_CODE_SCHEDULE].digest)
  #print(photos_df[photos_df.digest =='8475834aeda8bb5907c2da3b085017b172213ce8'])

def produce_retime_script(photos_df, script="retime.sh"):
  ''' A pure python implementation is slower than 'touch' when executed file by file.
  It might be possible (and better) executing as a loop, but using python is less portable.
  In any case, a pure python version would look like this:

  import time, os
  atime = os.stat(filepath).st_atime
  mtime = pic['date'].timestamp()  # Or from some other time variable in string format (needs to be transformed into timestamp)
  # or mtime = time.mktime(time.strptime(pic['date'], '%Y-%m-%d %H:%M:%S'));
  os.utime(filepath, (atime, mtime))  # This typically modifies the 'ctime' as well on macOS

  Also, using Jinja2 templates is slightly slower than generating the output by hand. but it is definitely cleaner and easier to expand
  '''

  
  template = template_env.get_template('retime.sh')
  #print(template.render(entries=photos_df[photos_df['should_remove'] != REMOVAL_CODE_SCHEDULE]))

  photos_dfa = photos_df[['folder', 'name','datetime', 'should_remove']].copy()
  photos_dfa['fullpath'] = photos_dfa.apply(lambda x: os.path.join(x['folder'], x['name']), axis=1)
  template.stream(entries=photos_dfa[photos_dfa['should_remove'] != REMOVAL_CODE_SCHEDULE]).dump(script)
  '''

  script_parts = []
  for i, p in photos_df[photos_df['should_remove'] != REMOVAL_CODE_SCHEDULE].iterrows():
    script_parts.append("touch -m -t \"{0:%Y%m%d%H%M.%S}\" \"{1}\"".format(
                      p['datetime'], os.path.join(p['folder'], p['name']),
                  ))
  script_content = "\n".join(script_parts)
  with open(script, 'w') as f:
      f.write(script_content)
  '''

def read_datafiles(running_working_info, datafiles, deduplicate=True):
  ph_working_info = pd.DataFrame()
  ph_ok = pd.DataFrame()
  ph_error = pd.DataFrame()

  for datafile in datafiles:
    last_working_info = None
    store = pd.HDFStore(storage.normalize(datafile))
    if '/info' in store:
      store.close()
      last_working_info = pd.read_hdf(storage.normalize(datafile), key='info')
      if last_working_info.loc[0, 'hostname'] != running_working_info['hostname'][0]:
        print("Datafile '{}{}{}' was generated at {}, but analysis is running on {}".format(
            Fore.GREEN, datafile, Fore.RESET,
            last_working_info.loc[0, 'hostname'], running_working_info['hostname'][0]
          ))
      ph_working_info = pd.concat([ph_working_info, last_working_info])
      #print(ph_working_info)
    else:
      store.close()
      print("{}Datafile '{}{}{}' doesn't contain 'info':{} be extra vigilant\n".format(Fore.RED, Fore.GREEN, datafile, Fore.RED, Fore.RESET))
  
    ph_ok = pd.concat([ph_ok, pd.read_hdf(storage.normalize(datafile), key='ok')])
    ph_error = pd.concat([ph_error, pd.read_hdf(storage.normalize(datafile), key='error')])
    if last_working_info is not None:
      # Sanitize folders
      wd = last_working_info.loc[0, 'wd']
      ph_ok['folder'] = ph_ok.folder.apply(lambda x: os.path.realpath(os.path.join(wd, x)))
      ph_error['folder'] = ph_error.folder.apply(lambda x: os.path.realpath(os.path.join(wd, x)))

  # Because persist_version references indexes...
  ph_ok.reset_index(inplace=True)
  if 'persist_version' in ph_ok.columns:
    map_reindex = {v:k for k,v in ph_ok['index'].to_dict().items()}
    map_reindex[PERSIST_VERSION_KEEP] = PERSIST_VERSION_KEEP
    ph_ok['persist_version'] = ph_ok['persist_version'].transform(lambda x: map_reindex[x])
  ph_ok.drop('index', axis=1, inplace=True)
  ph_error.reset_index(inplace=True)
  if 'persist_version' in ph_error.columns:
    map_reindex = {v:k for k,v in ph_error['index'].to_dict().items()}
    map_reindex[PERSIST_VERSION_KEEP] = PERSIST_VERSION_KEEP
    ph_error['persist_version'] = ph_error['persist_version'].transform(lambda x: map_reindex[x])
  ph_error.drop('index', axis=1, inplace=True)

  num_read_ok = len(ph_ok)
  num_read_error = len(ph_error)
  if deduplicate:
    ph_ok = ph_ok.drop_duplicates(keep='first')
    ph_error = ph_error.drop_duplicates(keep='first')
  
  return ph_working_info, ph_ok, ph_error, num_read_ok, num_read_error

def deduplication_process(photos_df, dup_full, dup_digest, output_script, label, preferred_folder=False, goal=0, verbose=0):
  # process all entries from the input
  photos_df.loc[:, 'should_remove'] = REMOVAL_CODE_IGNORE
  photos_df.loc[:, 'persist_version'] = PERSIST_VERSION_KEEP

  if sum(dup_full) != 0:
    print("{}   - full -{}".format(Fore.GREEN,Fore.RESET))
    generate_dupes_info(photos_df, dup_full, preferred_folder, verbose = verbose)
    report_dupes(photos_df, dup_full, goal, verbose = verbose)

  if sum(dup_digest) != 0:
    print("{}   - digest -{}".format(Fore.GREEN,Fore.RESET))
    generate_dupes_info(photos_df, dup_digest, preferred_folder, verbose = verbose)
    report_dupes(photos_df, dup_digest, goal, verbose = verbose)

  produce_dupes_scripts(photos_df, dup_digest, label=label)

def preduplication_info(photos_df, dup_full, dup_full_except_first, dup_digest, dup_digest_except_first):
  num_photos = len(photos_df)
  if sum(dup_full) != 0 | sum(dup_digest) != 0:
    print("photos {}, after reducing:".format(
        num_photos
      ))
  else:
    print("photos {}".format(
        num_photos
      ))

  if sum(dup_full) != 0:
    print("   - full   -> {} (removing {} and processing {})".format(
      sum(~dup_full_except_first),
      sum(dup_full_except_first),
      sum(dup_full)
      ))
  if sum(dup_digest) != 0:
    print("   - digest -> {} (removing {} and processing {})".format(
      sum(~dup_digest_except_first),
      sum(dup_digest_except_first),
      sum(dup_digest)
      ))

  if 'size' in photos_df:
    size_photos = bsize_value(photos_df['size'].sum())
    if sum(dup_full) != 0 | sum(dup_digest) != 0:
      print("size {:.3f} {}, after reducing:".format(
          *size_photos
        ))
    else:
      print("size {:.3f} {}".format(
          *size_photos
        ))

    if sum(dup_full) != 0:
      print("   - full   -> {:.3f} {} (removing {:.3f} {} and processing {:.3f} {})".format(
          *bsize_value(photos_df[~dup_full_except_first]['size'].sum()),
          *bsize_value(photos_df[dup_full_except_first]['size'].sum()),
          *bsize_value(photos_df[dup_full]['size'].sum())
        ))
    if sum(dup_digest) != 0:
      print("   - digest -> {:.3f} {} (removing {:.3f} {} and processing {:.3f} {})".format(
          *bsize_value(photos_df[~dup_digest_except_first]['size'].sum()),
          *bsize_value(photos_df[dup_digest_except_first]['size'].sum()),
          *bsize_value(photos_df[dup_digest]['size'].sum())
        ))
  print()

def general_info(ph_ok, ph_error):
  num_ok = len(ph_ok)
  num_error = len(ph_error)
  num_total = num_ok + num_error
  print("processing   : {} (ok: {}/ error: {})".format(num_total, num_ok, num_error))
  print("{}% errors{}: {:.2%}".format(Fore.GREEN, Fore.RESET, num_error/ num_total))
  print("{}% with JSON metadata{}: {:.2%}, {}".format(Fore.GREEN, Fore.RESET, (sum(ph_ok.has_json)+sum(ph_error.has_json))/ num_total, sum(ph_ok.has_json)))

def enrich(photos_df):
  '''
  The list of columns added is returned. There is no need to store those values are they'll be recreated.
  Also, they might not be storable as HDF due to the types.
  '''
  photos_df['mtime_date'] = photos_df.mtime.apply(lambda x: x.date())
  photos_df['datetime_date'] = photos_df.datetime.apply(lambda x: x.date())
  photos_df['second_discrepancy'] = (photos_df.datetime - photos_df.mtime).apply(lambda x: abs(x.total_seconds()))
  # photos_df['folder_date'] = photos_df['folder'].apply(lambda x: len(x.split('/')))
  #photos_df['folder_date'] = photos_df['folder'].str.split('/').str[-1].str.match('\d{4}_\d{2}_\d{2}')

  # Up to this point, the baseline is .8459 s -> 2.106 (1.98 without bool) for the hd_bank_master.pho set / and .16045 -> 0.36 for the hd_back.pho set
#
  #Always a day
  a = re.compile(".*{0}(\d{{4}}_\d{{2}}_\d{{2}})(?:\s+[^{0}]*)?".format(os.path.sep))
  #photos_df['folder_has_date'] = photos_df['folder'].str.match(a)
  photos_df['folder_date'] = photos_df['folder'].str.extract(a)
  photos_df['folder_date'] = pd.to_datetime(photos_df['folder_date'], errors='coerce', format='%Y_%m_%d')

  # Months without day?
  a = re.compile(".*{0}(\d{{4}}{0}\d{{2}}){0}(?!\d{{4}}_\d{{2}}_\d{{2}})".format(os.path.sep))
  #photos_df['folder_has_month_date'] = photos_df['folder'].str.match(a)
  photos_df['folder_month_date'] = photos_df['folder'].str.extract(a)
  photos_df['folder_month_date'] = pd.to_datetime(photos_df['folder_month_date'], errors='coerce', format='%Y{}%m'.format(os.path.sep))

  return ['mtime_date', 'datetime_date', 'second_discrepancy',
          'folder_date', 'folder_month_date'] # Values that cannot be stored as HDF and are computable


def timed_info(photos_df_timed):
  print("{0}% matching times / dates{1}: {2:.2%} {0}/{1} {3:.2%}".format(Fore.GREEN,Fore.RESET,
            sum(photos_df_timed.mtime == photos_df_timed.datetime)/len(photos_df_timed),
            sum(photos_df_timed.mtime_date == photos_df_timed.datetime_date)/len(photos_df_timed)))

  #print("{0}% with discrepancy{1}:{0} <1 minute:{1} {2:.2%} {0}/ <1 hour:{1} {3:.2%} {0}/ <1 day:{1} {4:.2%} {0}/ >1 day:{1} {5:.2%}".format(Fore.GREEN,Fore.RESET,
  #          sum(photos_df_timed.second_discrepancy <= 60)/len(photos_df_timed),
  #          sum(photos_df_timed.second_discrepancy <= 3600)/len(photos_df_timed),
  #          sum(photos_df_timed.second_discrepancy <= 24*3600)/len(photos_df_timed),
  #          sum(photos_df_timed.second_discrepancy > 24*3600)/len(photos_df_timed)))

  photos_df_timed_not = photos_df_timed[photos_df_timed.second_discrepancy != 0]
  if len(photos_df_timed_not) > 0:
    print("{0}% with discrepancy ({1}{6}{0}):{1} {2:.2%} {0}<= 1 minute <{1} {3:.2%} {0}<= 1 hour <{1} {4:.2%} {0}<= 1 day <{1} {5:.2%}".format(Fore.GREEN,Fore.RESET,
              sum(photos_df_timed_not.second_discrepancy <= 60)/len(photos_df_timed_not),
              sum(photos_df_timed_not.second_discrepancy.between(61, 3600))/len(photos_df_timed_not),
              sum(photos_df_timed_not.second_discrepancy.between(3601, 24*3600))/len(photos_df_timed_not),
              sum(photos_df_timed_not.second_discrepancy > 24*3600)/len(photos_df_timed_not),
              len(photos_df_timed_not)))

  print("{}% timeless{}: {:.2%}".format(Fore.GREEN,Fore.RESET,
            len(photos_df_timed[photos_df_timed.timeless])/len(photos_df_timed)))
  #if len(photos_df_timed[photos_df_timed.timeless]) > 0:
  #  print("{0}% with discrepancy (timeless){1}:{0} <1 minute:{1} {2:.2%} {0}/ <1 hour:{1} {3:.2%} {0}/ <1 day:{1} {4:.2%} {0}/ >1 day:{1} {5:.2%}".format(Fore.GREEN,Fore.RESET,
  #            sum(photos_df_timed[photos_df_timed.timeless].second_discrepancy <= 60)/len(photos_df_timed[photos_df_timed.timeless]),
  #            sum(photos_df_timed[photos_df_timed.timeless].second_discrepancy <= 3600)/len(photos_df_timed[photos_df_timed.timeless]),
  #            sum(photos_df_timed[photos_df_timed.timeless].second_discrepancy <= 24*3600)/len(photos_df_timed[photos_df_timed.timeless]),
  #            sum(photos_df_timed[photos_df_timed.timeless].second_discrepancy > 24*3600)/len(photos_df_timed[photos_df_timed.timeless])))

  if len(photos_df_timed[photos_df_timed.timeless]) > 0:
    print("{0}% with discrepancy (timeless){1}: {2:.2%} {0}<= 1 minute <{1} {3:.2%} {0}<= 1 hour <{1} {4:.2%} {0}<= 1 day <{1} {5:.2%}".format(Fore.GREEN,Fore.RESET,
              sum(photos_df_timed[photos_df_timed.timeless].second_discrepancy <= 60)/len(photos_df_timed[photos_df_timed.timeless]),
              sum(photos_df_timed[photos_df_timed.timeless].second_discrepancy.between(61, 3600))/len(photos_df_timed[photos_df_timed.timeless]),
              sum(photos_df_timed[photos_df_timed.timeless].second_discrepancy.between(3601, 24*3600))/len(photos_df_timed[photos_df_timed.timeless]),
              sum(photos_df_timed[photos_df_timed.timeless].second_discrepancy > 24*3600 )/len(photos_df_timed[photos_df_timed.timeless])))
