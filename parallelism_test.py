from turtle import delay
import ujson as json
import dask.bag as db
from dask import delayed
from dask.distributed import Client, progress
from itertools import count

N = 10**8
def load():
    return delayed(range(N))
if __name__ == '__main__':

    client = Client(threads_per_worker=2, n_workers=2)

    bag = db.from_delayed([load()]).
    print(out)
# df = bag.to_dataframe(meta=metaComment).body
# bag.to_dataframe(meta=metaComment).body.str.normalize('NFKD').str.lower().split().compute() 
# a = bag.map(lambda x:x['body']).str.lower().str.rstrip().str.lstrip().str.split().flatten().compute()