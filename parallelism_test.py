import dask.bag as db
from dask import delayed
from dask.distributed import Client, LocalCluster

N = 10**6
def gen():
    for i in range(N):
        yield delayed(i)

def load():
    return delayed(range(N))
if __name__ == '__main__':
    client = Client(LocalCluster())
    bag = db.from_delayed([gen(),gen()]).map(lambda x: 2*x)
    # bag = db.from_delayed([load(),load()]).repartition(npartitions=4).map(lambda x: 2*x)
    out = bag.count().compute()
    print(out)
