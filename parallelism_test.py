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


def chunk(iterable, chunk_size=10 ** 4):
    iterator = iter(iterable)
    while True:
        buffer = [None] * chunk_size
        try:
            for i in range(chunk_size):
                buffer[i] = next(iterator)
            yield buffer
        except StopIteration:
            yield buffer[:i]
            return


def process_chunk(lines):
    return [json.loads(line) for line in lines]

def nothing(x):
    pass


async def main():
    client = Client()
    # client = Client()
    #     bag = db.from_delayed([load(),load()]).map(lambda x: 2*x)
    #     # bag = db.from_delayed([load(),load()]).repartition(npartitions=4).map(lambda x: 2*x)
    #     out = bag.count().compute()
    #     print(out)
    source = Stream()
    source.scatter().map(process_chunk).gather().sink(nothing)

    # start = time.time()
    # print(start)
    # for i in chunk(range(10**8)):
    #     source.emit(i)

    # end = time.time()
    # print(end-start)

    filename = "D:/Downloads/reddit/comments/RC_2020-07.zst"
    reader = Zreader(filename)
    async for lines in chunk(reader.readlines()):
        source.emit(lines)

    sleep(1)  # simulate actual work

if __name__ == "__main__":
    main()
    while(True):
        pass