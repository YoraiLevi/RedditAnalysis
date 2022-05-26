#  python .\json_keys.py E:\Datasets\reddit\comments > comment_field_types.txt
#  python .\json_keys.py E:\Datasets\reddit\submissions\ > submission_field_types.txt
from collections import defaultdict, Counter
from itertools import islice, groupby
import argparse
import os
import glob
import operator

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
Occurrences = df.gt(0).sum(axis=1)
FirstUsage = df.columns.get_indexer(df.gt(0).idxmax(axis=1).values)
LastUsage = df.columns.get_indexer(df.iloc[:, ::-1].gt(0).idxmax(axis=1).values)
df['Total'] = Total
df['Occurrences'] = Occurrences
df['In Effect'] = LastUsage - FirstUsage + 1
df['Since'] = FirstUsage
df['Last'] = LastUsage
df['Deprecated?'] = LastUsage < LastUsage.max()

df.to_csv('json_keys.csv')

df = pd.DataFrame(type_spreads)
df = df.fillna(0)
Total = df.sum(axis=1)
Occurrences = df.gt(0).sum(axis=1)
FirstUsage = df.columns.get_indexer(df.gt(0).idxmax(axis=1).values)
LastUsage = df.columns.get_indexer(df.iloc[:, ::-1].gt(0).idxmax(axis=1).values)
df['Total'] = Total
df['Occurrences'] = Occurrences
df['In Effect'] = LastUsage - FirstUsage + 1
df['Since'] = FirstUsage
df['Last'] = LastUsage
df['Deprecated?'] = LastUsage < LastUsage.max()

df.to_csv('json_types.csv')


for i in list(((key,[t for key1,t in list(group)]) for key,group in groupby(sorted(sum(type_spreads.values(),Counter()),key=operator.itemgetter(0)),key=operator.itemgetter(0)))):
    print(i)