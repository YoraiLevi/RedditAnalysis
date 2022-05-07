# import dask.bag as db
# from dask import delayed
# from dask.distributed import Client, LocalCluster

# N = 10**8
# def load():
#     return delayed(range(N))
# if __name__ == '__main__':
#     client = Client(LocalCluster())
#     bag = db.from_delayed([load(),load()]).map(lambda x: 2*x)
#     # bag = db.from_delayed([load(),load()]).repartition(npartitions=4).map(lambda x: 2*x)
#     out = bag.count().compute()
#     print(out)

from dask.distributed import Client
from streamz import Stream
from time import sleep
def inc(x):
    sleep(1)  # simulate actual work
    return x + 1
if __name__ == '__main__':
    client = Client()
    # source = Stream()
    # source.map(inc).sink(print)
    # for i in range(10):
        # source.emit(i)
    source = Stream()
    source.scatter().map(inc).buffer(8).gather().sink(print)

    for i in range(10):
        source.emit(i)
