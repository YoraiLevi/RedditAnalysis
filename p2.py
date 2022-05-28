import argparse
import argparse
import ujson
from zreader import Zreader
from models import init_database, init_models
from peewee import chunked
from itertools import islice
fields = {  # comment
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
}
default_data = {field: None for field in fields}
def process_line(line):
    obj = ujson.decode(line)

    # data = defaultdict(None)
    data = default_data.copy()
    data["retrieved_utc"] = obj.get("retrieved_on")
    data.update({key: obj[key] for key in obj if key in fields})
    if isinstance(data["created_utc"], str):
        data["created_utc"] = int(data["created_utc"])

    left_over = {key: obj[key] for key in obj if key not in fields}

    data["json"] = ujson.encode(left_over)

    return dict(data)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('zstd_file')
    args = parser.parse_args()

    db = init_database()
    models = init_models(db)
    with db:
        db.create_tables([models["comment"], models["comment"]])
    items = islice(map(process_line,Zreader(args.zstd_file).readlines()),6*10**3)
    # with db:
    with db.atomic():
        for batch in chunked(items, 10*3):
            models["comment"].insert_many(batch).execute()
        