#  python .\json_keys.py E:\Datasets\reddit\comments > comment_field_types.txt
#  python .\json_keys.py E:\Datasets\reddit\submissions\ > submission_field_types.txt
from collections import defaultdict, Counter
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
            keys.append(list(map(obj.items(),lambda key,val: (key,type(val)))))

            # for key,item in obj.items():
                # if key == 'author_flair_richtext':
                    # print(file,string,item)
                # if(isinstance(item,str) and len(item)>5):
                    # item = "this is likely a string"
                # set_of_keys[key].add(type(item))
                # if(isinstance(item,(list))):
                    # print(key,string)
    except:
        pass
    print(keys)
    file_keys = Counter(keys)
    files_keys[file] = file_keys

# for key,value in set_of_keys.items():
    # print(key,set(map(type,value)))
    # print(key,value)
print(files_keys)