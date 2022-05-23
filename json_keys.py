import imp
from itertools import islice

from zreader import Zreader
import ujson as json

def take(n, iterable):
    "Return first n items of the iterable as a list"
    return list(islice(iterable, n))