#  python .\json_keys.py E:\Datasets\reddit\comments > comment_field_types.txt
#  python .\json_keys.py E:\Datasets\reddit\submissions\ > submission_field_types.txt
from collections import defaultdict, Counter
import collections
from itertools import islice
import argparse
import itertools
import os
import glob
from typing import Collection

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
df['Since'] = FirstUsage
df['Last'] = LastUsage
df['Deprecated?'] = LastUsage < LastUsage.max()

itertools.groupby((t for (t,count) in sum(type_spreads.values(),Collection.Counter)),lambda x: x[0])