from itertools import islice
import argparse
from multiprocessing import Queue,Process
import ujson
from models import init_database, init_models
from zreader import Zreader
from peewee import chunked

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
    "json"
}
db = None
models = None

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

def to_db(input):
    global db, models
    db = init_database()
    models = init_models(db)
    # Insert rows 100 at a time.
    with db.atomic():
        for batch in chunked(iter(input.get, "STOP"), 100):
            models["comment"].insert_many(batch).execute()

def worker(input, output):
    for line in iter(input.get, "STOP"):
        output.put(process_line(line))

NUMBER_OF_PROCESSES = 4

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('zstd_file')
    args = parser.parse_args()


    line_queue = Queue()
    db_queue = Queue()
    for i in range(NUMBER_OF_PROCESSES):
        Process(target=worker, args=(line_queue, db_queue)).start()
    Process(target=to_db, args=[db_queue]).start()
    try:
        pass
        for line in Zreader(args.zstd_file).readlines():
            line_queue.put(line)
    finally:
        for i in range(NUMBER_OF_PROCESSES):
            line_queue.put("STOP")
        db_queue.put("STOP")