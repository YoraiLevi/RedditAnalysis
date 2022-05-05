from sched import scheduler
from numpy import iterable
import ujson as json
import dask.bag as db
from zreader import Zreader
from dask.distributed import progress, Client, LocalCluster
from dask import delayed

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
filenames = [
    # "D:/Downloads/reddit/comments/RC_2006-01.zst",
    # "D:/Downloads/reddit/comments/RC_2006-02.zst",
    # "D:/Downloads/reddit/comments/RC_2006-03.zst",
    # "D:/Downloads/reddit/comments/RC_2006-04.zst",
    # "D:/Downloads/reddit/comments/RC_2006-05.zst",
    # "D:/Downloads/reddit/comments/RC_2006-06.zst",
    # "D:/Downloads/reddit/comments/RC_2006-07.zst",
    # "D:/Downloads/reddit/comments/RC_2006-08.zst",
    # "D:/Downloads/reddit/comments/RC_2006-09.zst",
    # "D:/Downloads/reddit/comments/RC_2006-10.zst",
    # "D:/Downloads/reddit/comments/RC_2006-11.zst",
    # "D:/Downloads/reddit/comments/RC_2006-12.zst",
    # "D:/Downloads/reddit/comments/RC_2007-01.zst",
    # "D:/Downloads/reddit/comments/RC_2007-02.zst",
    # "D:/Downloads/reddit/comments/RC_2007-03.zst",
    # "D:/Downloads/reddit/comments/RC_2015-05.zst",
    # "D:/Downloads/reddit/comments/RC_2017-06.zst",
    # "D:/Downloads/reddit/comments/RC_2018-02.zst",
    # "D:/Downloads/reddit/comments/RC_2019-05.zst",
    # "D:/Downloads/reddit/comments/RC_2019-11.zst",
    # "D:/Downloads/reddit/comments/RC_2020-07.zst",
    # "D:/Downloads/reddit/comments/RC_2020-08.zst",
    # "D:/Downloads/reddit/comments/RC_2020-09.zst",
    # "D:/Downloads/reddit/comments/RC_2020-10.zst",
    # "D:/Downloads/reddit/comments/RC_2020-11.zst",
    # "D:/Downloads/reddit/comments/RC_2020-12.zst",
    # "D:/Downloads/reddit/comments/RC_2021-01.zst",
    "D:/Downloads/reddit/comments/RC_2021-02.zst",
    # "D:/Downloads/reddit/comments/RC_2021-03.zst",
    # "D:/Downloads/reddit/comments/RC_2021-04.zst",
    # "D:/Downloads/reddit/comments/RC_2021-05.zst",
    # "D:/Downloads/reddit/comments/RC_2021-06.zst"
]


@delayed
def load(filename,buffer_size=10*6):
    iterable = Zreader(filename).readlines()
    while(line:=next(iterable)):
        buffer = [None]*buffer_size
        buffer[0] = line
        for i in range(1,buffer_size):
            try:
                buffer[i] = next(iterable)
            except StopIteration:
                yield buffer
        yield buffer
    raise StopIteration

def main():
    cluster = LocalCluster()
    client = Client(cluster)
    # db.from_delayed([delayed(load)(filename) for filename in filenames])
    # bag = db.from_sequence(filenames).map().map(json.loads)
    # bag = db.from_delayed([delayed(load)(filename) for filename in filenames])
    bag = db.from_sequence(filenames).map(load)
    frequencyList = (
        bag.map(lambda x: x["body"])
        .str.lower()
        .str.rstrip()
        .str.lstrip()
        .str.split()
        .flatten()
        .frequencies(sort=True)
    )

    out = frequencyList.to_dataframe().to_csv("2021-*.csv")
    print(out)


# df = bag.to_dataframe(meta=metaComment).body
# bag.to_dataframe(meta=metaComment).body.str.normalize('NFKD').str.lower().split().compute()
# a = bag.map(lambda x:x['body']).str.lower().str.rstrip().str.lstrip().str.split().flatten().compute()

#  bag = db.from_sequence(filenames).map_partitions(lambda filename: Zreader(filename).readlines)
if __name__ == "__main__":
    main()
