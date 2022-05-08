# import dask.bag as db
from asyncio import streams
from functools import partial
from gc import callbacks
from threading import Thread
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
    time.sleep(0.1)
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

from concurrent.futures import ThreadPoolExecutor
executor = ThreadPoolExecutor(max_workers=8)

# def main():
#     client = Client()
#     source = Stream()
#     result = source.scatter().map(lambda x:x).gather().sink(print)
#     def read():
#         for i in range(10*6):
#             source.emit(i)
#     executor.submit(read)
#     time.sleep(10)


# if __name__ == '__main__':
#     main()


# async def async_range(count):
#     for i in range(count):
#         yield(i)
# async def main():
#     client = await Client(processes=False, asynchronous=True)
#     # client = Client()
#     #     bag = db.from_delayed([load(),load()]).map(lambda x: 2*x)
#     #     # bag = db.from_delayed([load(),load()]).repartition(npartitions=4).map(lambda x: 2*x)
#     #     out = bag.count().compute()
#     #     print(out)
#     source = Stream(asynchronous=True)
#     result = source.scatter().map(lambda x:x).gather().sink(nothing)
#     async for i in async_range(10*6):
#         source.emit(i,asynchronous=True)
#     # filename = "D:/Downloads/reddit/comments/RC_2020-07.zst"
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


# from tornado import gen
# import time

# def increment(x):
#     """ A blocking increment function

#     Simulates a computational function that was not designed to work
#     asynchronously
#     """
#     time.sleep(1)
#     return x + 1

# @gen.coroutine
# def write(x):
#     """ A non-blocking write function

#     Simulates writing to a database asynchronously
#     """
#     # yield gen.sleep(0.2)
#     print(x)

# from dask.distributed import Client
# from tornado.ioloop import IOLoop
# from functools import partial

# async def f():
#     client = await Client(processes=False, asynchronous=True)
#     source = Stream(asynchronous=True)
#     source.scatter().map(increment).rate_limit('500ms').gather().sink(write)

#     print('start')
#     async def h():
#         for x in range(10**9):
#             # await source.emit(x)
#             loop.add_future(source.emit(x),callback=nothing)
#             # .run_in_executor(None,partial(source.emit,x))
#     loop.run_in_executor(None,h)
#     print('end')

#     await asyncio.sleep(10)
# # import asyncio
# from tornado.ioloop import IOLoop
# loop: IOLoop = IOLoop()

# import asyncio

# result = 0
# async def main(loop):
#     def sub1():
#         global result
#         print('[*] sub1() start')
#         for i in range(1, 10000000):
#             result += i
#             loop.create_task(source.emit(i,asynchronous=True))
#         print('[*] sub1() end')

#     def sub2():
#         global result
#         print('[*] sub2() start')
#         for i in range(10000000, 20000000):
#             result += i
#         print('[*] sub2() end')
#     client = await Client(processes=False, asynchronous=True,loop=loop)
#     # client = Client()
#     #     bag = db.from_delayed([load(),load()]).map(lambda x: 2*x)
#     #     # bag = db.from_delayed([load(),load()]).repartition(npartitions=4).map(lambda x: 2*x)
#     #     out = bag.count().compute()
#     #     print(out)
#     source = Stream(asynchronous=True)
#     result = source.scatter().map(lambda x:x).gather().sink(nothing)
        
        
#     res = await asyncio.gather(
#         loop.run_in_executor(None, sub1),
#         loop.run_in_executor(None, sub2)
#     )




# import asyncio
# if __name__ == "__main__":
#     # loop.run_sync(f)
#     # IOLoop().run_sync(main)
#     # asyncio.run(main())
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(main(loop))

# from dask.distributed import Client
# from tornado.ioloop import IOLoop
# from concurrent.futures import ThreadPoolExecutor

# # async def read():
# #     reader = Zreader(filename)
# #     for x in range(10):
# #         executor.submit(partial(source.emit,x))
# #     for line in reader.readlines():
# #         source.emit(lines, asynchronous=True)



def increment(x):
    """ A blocking increment function

    Simulates a computational function that was not designed to work
    asynchronously
    """
    # time.sleep(0.1)
    return x + 1

@gen.coroutine
def write(x):
    """ A non-blocking write function

    Simulates writing to a database asynchronously
    """
    # yield gen.sleep(0.2)
    print(x)

from dask.distributed import Client
from tornado.ioloop import IOLoop
async def f():
    client = Client()
    # source = Stream(asynchronous=True,loop=loop)
    source = Stream()
    source.scatter().map(increment).rate_limit('500ms').gather().sink(write)
    # async def h():
    for x in range(10**6):
        executor.submit(partial(source.emit,x))
        # print(x)
        # loop.add_future(source.emit(x,asynchronous=True),callback=print)
        # await source.emit(x)
            # loop.run_in_executor(executor=executor,func=partial(source.emit,x))
    print(11)
    # await h()
    # time.sleep(10)
    
    # executor.submit(h)

loop : IOLoop = None
executor : ThreadPoolExecutor = None
if __name__ == "__main__":
    executor = ThreadPoolExecutor(max_workers=8)
    loop = IOLoop()
    loop.run_sync(f)
    time.sleep(1)