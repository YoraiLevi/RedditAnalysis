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
for file in files:
    try:
        for string in islice(Zreader(file).readlines(),10):
            print(string)
    except:
        pass
    # print(file)