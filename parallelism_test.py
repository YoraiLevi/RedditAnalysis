# import dask.bag as db
from dask import delayed
from dask.distributed import Client, LocalCluster
from streamz import Stream
from zreader import Zreader
import ujson as json

import time

# N = 10**8
# def load():
#     return delayed(range(N))
def inc(x):
    time.sleep(1)
    return x
def chunk(iterable,chunk_size=10*6):
    iterator = iter(iterable)
    while(True):
        buffer = [None]*chunk_size
        try:
            for i in range(chunk_size):
                buffer[i] = next(iterator)
            yield buffer
        except StopIteration:
            yield buffer[:i]
            return
if __name__ == '__main__':
    client = Client()
    # client = Client()
#     bag = db.from_delayed([load(),load()]).map(lambda x: 2*x)
#     # bag = db.from_delayed([load(),load()]).repartition(npartitions=4).map(lambda x: 2*x)
#     out = bag.count().compute()
#     print(out)
    # def nothing(x):
    #     pass
    # source = Stream()
    # source.buffer(10**6).scatter().map(inc).buffer(10**6).gather().sink(nothing)

    # start = time.time()
    # print(start)
    # for i in chunk(range(10**8)):
    #     source.emit(i)

    # end = time.time()
    # print(end-start)


    filename = "D:/Downloads/reddit/comments/RC_2020-08.zst"
    reader = Zreader(filename)
    for lines in chunk(reader.readlines()):
        source.emit(lines)

    sleep(1)  # simulate actual work
