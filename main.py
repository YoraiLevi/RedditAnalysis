import ujson as json
import dask.bag as db
from dask.distributed import Client, progress

if __name__ == '__main__':
    client = Client(n_workers=4, threads_per_worker=1)
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
    bag = db.read_text("RC_2006-*.zst").map(json.loads)
    frequencyList = bag.map(lambda x:x['body']).str.lower().str.rstrip().str.lstrip().str.split().flatten().frequencies(sort=True)
    frequencyList.to_dataframe().to_csv('*.csv').compute()
# df = bag.to_dataframe(meta=metaComment).body
# bag.to_dataframe(meta=metaComment).body.str.normalize('NFKD').str.lower().split().compute() 
# a = bag.map(lambda x:x['body']).str.lower().str.rstrip().str.lstrip().str.split().flatten().compute()