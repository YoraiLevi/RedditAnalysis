# import dask.bag as db
from dask import delayed
from dask.distributed import Client, LocalCluster
from streamz import Stream
from zreader import Zreader
import ujson as json

# N = 10**8
# def load():
#     return delayed(range(N))
if __name__ == '__main__':
    client = Client(threads_per_worker=1, n_workers=1)
    # client = Client()
#     bag = db.from_delayed([load(),load()]).map(lambda x: 2*x)
#     # bag = db.from_delayed([load(),load()]).repartition(npartitions=4).map(lambda x: 2*x)
#     out = bag.count().compute()
#     print(out)
    def nothing(x):
        pass
    source = Stream()
    source.buffer(1).gather().sink(nothing)
    filename = "D:/Downloads/reddit/comments/RC_2020-09.zst"
    reader = Zreader(filename)
    for line in reader.readlines():
        source.emit(line)

    sleep(10)  # simulate actual work
