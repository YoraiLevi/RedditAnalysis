import ujson as json
import dask.bag as db
from dask.distributed import Client, progress

if __name__ == '__main__':
    from dask.distributed import Client

    async def f():
        client = await Client(asynchronous=True)
        future = client.submit(lambda x: x + 1, 10)
        result = await future
        await client.close()
        return result

# Either use Tornado
    from tornado.ioloop import IOLoop
    IOLoop().run_sync(f)

    async def main():
        # print('Hello ...')
        # await asyncio.sleep(1)
        # print('... World!')
        # client = awaitClient(asynchronous=True) #await Client(threads_per_worker=2, n_workers=2,asynchronous=True)
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
        name = "RC_2021-05.zst"
        bag = db.read_text(name).map(json.loads)
        frequencyList = bag.map(lambda x:x['body']).str.lower().str.rstrip().str.lstrip().str.split().flatten().frequencies(sort=True)
        out = frequencyList.to_dataframe().to_csv('2021-*.csv')
        # print(out)
    import asyncio
    # asyncio.run(main())
    # asyncio.get_event_loop().run_until_complete(main())
# df = bag.to_dataframe(meta=metaComment).body
# bag.to_dataframe(meta=metaComment).body.str.normalize('NFKD').str.lower().split().compute() 
# a = bag.map(lambda x:x['body']).str.lower().str.rstrip().str.lstrip().str.split().flatten().compute()