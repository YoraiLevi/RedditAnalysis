import ujson as json
import dask.bag as db
from dask import delayed
from dask.distributed import Client, LocalCluster
from itertools import count

N = 10**6
def load():
    return delayed(range(N))
if __name__ == '__main__':
    client = Client(LocalCluster())
    bag = db.from_delayed([load(),load()]).map(lambda x: 2*x)
    out = bag.count().compute()
    print(out)
# df = bag.to_dataframe(meta=metaComment).body
# bag.to_dataframe(meta=metaComment).body.str.normalize('NFKD').str.lower().split().compute() 
# a = bag.map(lambda x:x['body']).str.lower().str.rstrip().str.lstrip().str.split().flatten().compute()