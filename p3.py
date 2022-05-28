import ujson

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
    # print(data["json"])
    return dict(data)
with open("C:/Users/devic/OneDrive/Documents/Datasets/reddit/comments/RC_2005-12.ndjson",encoding="utf8") as f:
    with open('1.ndjson','w+') as f1:
        for line in f.readlines():
            print(line)
            f1.write(pg_text_format(process_line(line)))
            f1.write('\n')