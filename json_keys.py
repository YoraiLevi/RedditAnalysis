import imp
from itertools import islice
import argparse
import glob
from zreader import Zreader
import ujson as json

parser = argparse.ArgumentParser()
parser.add_argument('dir')
args = parser.parse_args()

directory = args['dir']

files = glob.glob('**/*.zst')
for file in files:
    pass