# import dask.bag as db
from dask import delayed
from dask.distributed import Client, LocalCluster
from streamz import Stream
from zreader import Zreader

# N = 10**8
# def load():
#     return delayed(range(N))
if __name__ == '__main__':
    client = Client(LocalCluster())
#     bag = db.from_delayed([load(),load()]).map(lambda x: 2*x)
#     # bag = db.from_delayed([load(),load()]).repartition(npartitions=4).map(lambda x: 2*x)
#     out = bag.count().compute()
#     print(out)

    source = Stream()
    source.scatter().map(inc).buffer(8)
    filename = "D:/Downloads/reddit/comments/RC_2021-02.zst"
    for i in range(10):
        source.emit(i)

    sleep(10)  # simulate actual work
