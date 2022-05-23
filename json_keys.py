from itertools import islice
import argparse
import os
import glob
from zreader import Zreader
import ujson as json

parser = argparse.ArgumentParser()
parser.add_argument('dir')
args = parser.parse_args()

glob_string = os.path.join(args.dir,'*.zst').replace('\\','/')
files = glob.glob(glob_string)
print(files)
for file in files:
    print(file)