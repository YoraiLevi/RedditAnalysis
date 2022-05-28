import ujson
import json
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('file')
args = parser.parse_args()

fields = [  # comment
    "author",
    "author_flair_text",
    "author_fullname",
    "author_premium",
    "body",
    "comment_type",
    "controversiality",
    "distinguished",
    "gilded",
    "id",  # id
    "link_id",
    "name",
    "parent_id",
    "permalink",
    "created_utc",
    "approved_at_utc",
    "author_created_utc",
    "retrieved_utc",
    "score",
    "stickied",
    "subreddit",
    "subreddit_id",
    "subreddit_type",
    "total_awards_received",
]
def pg_text_format(d):
    item = [d[field] for field in fields]
    item = [repr(i) if isinstance(i,str) else i for i in item]
    item = [i if i is not None else '\\N' for i in item]
    item = [str(i) for i in item]
    item = item+[d["json"]]
    return '\t'.join(item)

base_data = {key:None for key in fields}
def process_line(line):
    obj = ujson.decode(line)
    data = base_data.copy()
    # data = defaultdict(None)
    data["retrieved_utc"] = obj.get("retrieved_on")
    data.update({key: obj[key] for key in obj if key in fields})
    if isinstance(data["created_utc"], str):
        data["created_utc"] = int(data["created_utc"])

    left_over = {key: obj[key] for key in obj if key not in fields}

    data["json"] = ujson.encode(left_over)
    # print(data["body"])
    return dict(data)
from itertools import islice
import codecs
import models
db = models.init_database()
models = models.init_models(db)
with codecs.open(args.file,'r',encoding='utf-8') as f:
    for line in islice(f.readlines(),7,8):
        print(models["comment"].insert(line).sql)
        # print(ujson.decode(line)["body"].encode("iso-8859-1"))
        # print(ujson.decode(line))
        # (pg_text_format(process_line(line)))