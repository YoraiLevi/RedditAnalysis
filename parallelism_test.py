# import dask.bag as db
from dask import delayed
from dask.distributed import Client, LocalCluster
from streamz import Stream
from zreader import Zreader
import ujson as json
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from tornado import gen

# N = 10**8
# def load():
#     return delayed(range(N))
def inc(x):
    time.sleep(1)
    return x


def chunk(iterable, chunk_size=10**4):
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



async def async_range(count):
    for i in range(count):
        yield(i)
async def main():
    client = await Client(processes=False, asynchronous=True)
    # client = Client()
    #     bag = db.from_delayed([load(),load()]).map(lambda x: 2*x)
    #     # bag = db.from_delayed([load(),load()]).repartition(npartitions=4).map(lambda x: 2*x)
    #     out = bag.count().compute()
    #     print(out)
    source = Stream(asynchronous=True)
    result = source.scatter().map(lambda x:x).gather().sink(nothing)
    async for i in async_range(10*6):
        source.emit(i,asynchronous=True)
    # filename = "D:/Downloads/reddit/comments/RC_2020-07.zst"
    # reader = Zreader(filename)
    # for lines in reader.readlines():
        # source.emit(lines, asynchronous=True)

    # start = time.time()
    # print(start)
    # for i in chunk(range(10**8)):
    #     source.emit(i)

    # end = time.time()
    # print(end-start)

    # with ThreadPoolExecutor(max_workers=1) as executor:
    # future = executor.submit(read,filename)
    # await asyncio.sleep(1)
    # print(future.result())


from tornado import gen
import time

def increment(x):
    """ A blocking increment function

    Simulates a computational function that was not designed to work
    asynchronously
    """
    time.sleep(0.1)
    return x + 1

@gen.coroutine
def write(x):
    """ A non-blocking write function

    Simulates writing to a database asynchronously
    """
    yield gen.sleep(0.2)
    print(x)

from dask.distributed import Client
from tornado.ioloop import IOLoop

async def f():
    client = await Client(processes=False, asynchronous=True)
    source = Stream(asynchronous=True)
    source.scatter().map(increment).rate_limit('500ms').gather().sink(write)

    async for x in async_range(10):
        print(source.emit(x))


# import asyncio
from tornado.ioloop import IOLoop
if __name__ == "__main__":
    IOLoop().run_sync(f)
    # IOLoop().run_sync(main)
    # asyncio.run(main())