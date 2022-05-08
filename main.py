import ujson as json
import dask.bag as db
from dask import delayed
from dask.distributed import Client, progress
from zreader import Zreader

def chunk(iterable, chunk_size=10**5):
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
def process_chunk(iterable):
    for i in iterable:
        yield json.loads(i)
def load(filename):
    return chunk(Zreader(filename).readlines())
if __name__ == '__main__':
    client = Client()
    metaComment = [
        ("gilded", int),
        ("retrieved_on", int),
        ("distinguished", str),
        ("author_flair_text", str),
        ("author", str),
        ("edited", bool),
        ("parent_id", str),
        ("id", str),
        ("subreddit", str),
        ("author_flair_css_class", str),
        ("created_utc", int),
        ("score", int),
        ("ups", int),
        ("controversiality", int),
        ("body", str),
        ("link_id", str),
        ("stickied", bool),
        ("subreddit_id", str),
    ]
    name = "RC_2021-04.zst"
    bag = db.from_delayed(delayed(load)(name)).map(process_chunk).flatten()
    frequencyList = bag.map(lambda x:x['body']).str.lower().str.rstrip().str.lstrip().str.split().flatten().frequencies(sort=True)
    out = frequencyList.to_dataframe().to_csv('2021-*.csv')
    print(out)
# df = bag.to_dataframe(meta=metaComment).body
# bag.to_dataframe(meta=metaComment).body.str.normalize('NFKD').str.lower().split().compute() 
# a = bag.map(lambda x:x['body']).str.lower().str.rstrip().str.lstrip().str.split().flatten().compute()