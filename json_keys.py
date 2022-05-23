from collections import defaultdict
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
set_of_keys = defaultdict(set)
for file in files:
    try:
        for string in islice(Zreader(file).readlines(),10):
            obj = json.loads(string)
            for key,item in obj.items():
                if(isinstance(item,str) and len(item)>15):
                    item = "this is likely a string"
                set_of_keys[key].add(item)
    except:
        pass
for key,value in set_of_keys.items():
    print(key,value)