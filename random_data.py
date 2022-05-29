# rm log; for FILE in $(wslpath E:/Datasets/reddit/comments/*); do echo $FILE; zstd -cdq --long=31 $FILE | python3 piped_json.py >> log; done
from concurrent.futures import ThreadPoolExecutor, thread
from glob import glob
from itertools import islice
import time
import sys
import argparse
from multiprocessing import Pool, Queue, Process
from threading import Thread
import traceback
import os
import random

import ujson
from collections import defaultdict
from models import init_database, init_models


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
db = None
models = None


def process_line(line):
    obj = ujson.decode(line)

    data = defaultdict(None)
    data["retrieved_utc"] = obj.get("retrieved_on")
    data.update({key: obj[key] for key in obj if key in fields})
    if isinstance(data["created_utc"], str):
        data["created_utc"] = int(data["created_utc"])

    left_over = {key: obj[key] for key in obj if key not in fields}

    data["json"] = ujson.encode(left_over)

    return dict(data)

from peewee import chunked
def to_db(processed_chunk, output,atomic=True,chunk_size_db = 100):
    pass
    # try:
    #     if(atomic):
    #         with db.atomic():
    #             for chunk in chunked(processed_chunk,chunk_size_db):
    #                 models["comment"].insert_many(chunk).execute()
    #     else:
    #         for chunk in chunked(processed_chunk,chunk_size_db):
    #             models["comment"].insert_many(chunk).execute()
    # except Exception as e:
    #     output.put(("Exception:", traceback.format_exc(), "Data:", processed_chunk))


def print_errors(input):
    for item in iter(input.get, "STOP"):
        print(item)


def worker(input, output,n_threads,atomic=True,chunk_size_db=100):
    global db, models
    db = init_database()
    models = init_models(db)
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        for chunk in iter(input.get, "STOP"):
            try:
                to_db(chunk,output,atomic,chunk_size_db)
            except Exception as e:
                output.put((traceback.format_exc(), chunk))
    output.put("STOP")



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


def generate_data(total_data,chunk_size, task_queue, obj):
    types = {k: type(v) for k, v in obj.items()}
    partitions = int(total_data/chunk_size)
    # generate data
    for _ in range(partitions):
        new_obj = lambda types: {k: v(random.uniform(-10,10)) if v is not type(None) else None for k, v in types.items()}
        chunk = [new_obj(types) for _ in range(chunk_size)]
        task_queue.put(chunk)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('file')
    parser.add_argument('--n_rows',type=int,default=10**6)
    parser.add_argument('--chunk_size_db',type=int,default=10**2)
    parser.add_argument('--chunk_size_enqueue',type=int,default=10**0)

    parser.add_argument('--atomic',type=bool,default=True)
    parser.add_argument('--threads',type=int,default=4)
    parser.add_argument('--processes',type=int,default=4)
    parser.add_argument('--data_generators',type=int,default=4)
    args = parser.parse_args()

    db = init_database()
    models = init_models(db,True)
    task_queue = Queue()
    done_queue = Queue()
    with db:
        db.create_tables([models["comment"], models["comment"]])

    for i in range(args.processes):
        Process(target=worker, args=(task_queue, done_queue,args.threads,args.atomic,args.chunk_size_db)).start()
    t = Thread(target=print_errors, args=[done_queue])
    t.start()
    try:
        with open(args.file) as f:
            for line in islice(f.readlines(),0,1):
                obj = process_line(line)
        for i in range(args.data_generators):
            Process(target=generate_data, args=(int(args.n_rows/args.data_generators),args.chunk_size_enqueue, task_queue, obj)).start()
    except Exception as e:
        print(traceback.format_exc())
    finally:
        for i in range(args.processes):
            task_queue.put("STOP")
        running = False
        t.join()