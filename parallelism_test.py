import dask.bag as db
from dask import delayed
from dask.distributed import Client, LocalCluster

N = 10**6
def load():
    return delayed(range(N))
if __name__ == '__main__':
    # client = Client(LocalCluster())
    # bag = db.from_delayed([load(),load()]).map(lambda x: 2*x)
    # # bag = db.from_delayed([load(),load()]).repartition(npartitions=4).map(lambda x: 2*x)
    # out = bag.count().compute()
    # print(out)
    
    import numpy as np

    class DataIterator:
        def __init__(self, x, y):
            self.x = x
            self.y = y

        def __len__(self):
            return len(self.x)

        @delayed
        def __getitem__(self):
            item1 = x.mean()
            item2 = y.sum()
            # Do some very heavy computations here by
            # calling other methods and return  
            return item1, item2

    x = np.random.randint(20, size=(20,))
    y = np.random.randint(50, size=(20,))

    data_gen = DataIterator(x, y)
    for i, (item1, item2) in enumerate(data_gen):
        print(item1, item2)



# df = bag.to_dataframe(meta=metaComment).body
# bag.to_dataframe(meta=metaComment).body.str.normalize('NFKD').str.lower().split().compute() 
# a = bag.map(lambda x:x['body']).str.lower().str.rstrip().str.lstrip().str.split().flatten().compute()