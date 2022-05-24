#  python .\json_keys.py E:\Datasets\reddit\comments > comment_field_types.txt
#  python .\json_keys.py E:\Datasets\reddit\submissions\ > submission_field_types.txt
from collections import defaultdict, Counter
import collections
from itertools import islice
import argparse
import os
import glob

import pandas as pd

from zreader import Zreader
import ujson as json

parser = argparse.ArgumentParser()
parser.add_argument('dir')
args = parser.parse_args()

glob_string = os.path.join(args.dir,'*.zst')
files = glob.glob(glob_string)
type_spreads = dict()
keys_spread = dict()
for file in files:
    keys = []
    types = []
    try:
        for string in islice(Zreader(file).readlines(),10):
            obj = json.loads(string)
            for key,item in obj.items():
                types.append((key,type(item)))
                keys.append(key)
        keys_spread[file] = Counter(keys)
        type_spreads[file] = Counter(types)
    except:
        print('Failed:',file)

df = pd.DataFrame(keys_spread)
df = df.fillna(0)
Total = df.sum(axis=1)
Occurences = df.gt(0).sum(axis=1)
FirstUsage = df.columns.get_indexer(df.gt(0).idxmax(axis=1).values)
LastUsage = df.columns.get_indexer(df.iloc[:, ::-1].gt(0).idxmax(axis=1).values)
df['Total'] = Total
df['Occurences'] = Occurences
df['In Effect'] = LastUsage - FirstUsage + 1
# def first_file(t):
#     return min(filter(lambda filectr: t in filectr[1],files_keys.items()))[0]
# def last_file(t):
#     return max(filter(lambda filectr: t in filectr[1],files_keys.items()))[0]
# def occurences_file(t):
#     return len(list(filter(lambda filectr: t in filectr[1],files_keys.items())))

# files_paths = sorted(files_keys.keys())
# def print_stats(total):
#     items = total.most_common(1)[0][1]
#     for t,count in total.most_common():
#         first_file_path = first_file(t)
#         last_file_path = last_file(t)
#         occurences_in_files = occurences_file(t)
#         effetive_time = files_paths.index(last_file_path)-files_paths.index(first_file_path)
#         print(effetive_time,occurences_in_files,first_file_path,last_file_path,t,":",count/items)

# showupallset = set()

# for file,total in files_keys.items():
#     items = total.most_common(1)[0][1]
#     most_common = total.most_common()
#     showupall = filter(lambda item: item[1] == items, most_common)
#     for item,count in showupall:
#         showupallset.add(item)
#     print(file,":")
#     print_stats(total)

# total_all_files = sum(files_keys.values(),collections.Counter())
# print('all :')
# print_stats(total_all_files)
# # all_items = total_all_files.most_common(1)[0][1]
# # print('most important :')
# # important = [(i,total_all_files[i]/all_items) for i in showupallset]

# # for (t,count) in sorted(important,key=lambda x: x[1],reverse=True):
# #     first_file_path = first_file(t)
# #     last_file_path = last_file(t)
# #     time = files_paths.index(last_file_path)-files_paths.index(first_file_path)
# #     print(time,last_file_path,first_file_path,t,count)
