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
    client = Client(LocalCluster())
#     bag = db.from_delayed([load(),load()]).map(lambda x: 2*x)
#     # bag = db.from_delayed([load(),load()]).repartition(npartitions=4).map(lambda x: 2*x)
#     out = bag.count().compute()
#     print(out)

    source = Stream()
    source.scatter().buffer(10**8).map(json.loads).gather()
    filename = "D:/Downloads/reddit/comments/RC_2021-02.zst"
    reader = Zreader(filename)
    for line in reader.readlines():
        source.emit(line)

    sleep(30)  # simulate actual work
