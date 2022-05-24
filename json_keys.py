#  python .\json_keys.py E:\Datasets\reddit\comments > comment_field_types.txt
#  python .\json_keys.py E:\Datasets\reddit\submissions\ > submission_field_types.txt
from collections import defaultdict, Counter
import collections
from itertools import islice
import argparse
import os
import glob

from zreader import Zreader
import ujson as json

parser = argparse.ArgumentParser()
parser.add_argument('dir')
args = parser.parse_args()

glob_string = os.path.join(args.dir,'*.zst')
files = glob.glob(glob_string)
files_keys = dict()
for file in files:
    keys = []
    try:
        for string in islice(Zreader(file).readlines(),10):
            obj = json.loads(string)
            for key,item in obj.items():
                keys.append((key,type(item)))
                # if key == 'author_flair_richtext':
                    # print(file,string,item)
                # if(isinstance(item,str) and len(item)>5):
                    # item = "this is likely a string"
                # set_of_keys[key].add(type(item))
                # if(isinstance(item,(list))):
                    # print(key,string)
        file_keys = Counter(keys)
        files_keys[file] = file_keys
    except:
        print('Failed:',file)

def print_stats(total):
    try:
        items = total.most_common(1)[0][1]
        for key,val in total.most_common():
            print(key,":",val/items)
    except:
        print('Failed')

showupallset = set()
for file,total in files_keys.items():
    items = total.most_common(1)[0][1]
    most_common = total.most_common()
    showupall = filter(lambda item: item[1] == items, most_common)
    for item,count in showupall:
        showupallset.add(item)
    print(file,":")
    print_stats(total)

total_all_files = sum(files_keys.values(),collections.Counter())
print('all :')
print_stats(total_all_files)
for i in showupallset:
    print(i)