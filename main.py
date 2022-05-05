import ujson as json
import dask.bag as db


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
bag = db.read_text("RC_2006-10.zst").map(json.loads)
df = bag.to_dataframe(meta=metaComment).body
bag.to_dataframe(meta=metaComment).body.str.normalize('NFKD').str.lower().split().compute() 
