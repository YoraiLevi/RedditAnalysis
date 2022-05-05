import ujson as json
import dask.bag as db
from zreader import Zreader
from dask.distributed import Client, progress

if __name__ == '__main__':
    client = Client(threads_per_worker=2, n_workers=2)
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
    filenames = ["RC_2020-10.zst","RC_2020-09.zst","RC_2020-11.zst"]
    bag = db.from_sequence(filenames).map(lambda filename: Zreader(filename).readlines()).map(json.loads)
    frequencyList = bag.map(lambda x:x['body']).str.lower().str.rstrip().str.lstrip().str.split().flatten().frequencies(sort=True)
    out = frequencyList.to_dataframe().to_csv('2021-*.csv')
    print(out)
# df = bag.to_dataframe(meta=metaComment).body
# bag.to_dataframe(meta=metaComment).body.str.normalize('NFKD').str.lower().split().compute() 
# a = bag.map(lambda x:x['body']).str.lower().str.rstrip().str.lstrip().str.split().flatten().compute()