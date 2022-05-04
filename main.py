# https://www.reddit.com/r/pushshift/comments/aibhec/reddit_comments_working_with_zst_files/
# zstd -cdq ../../Datasets/Reddit/set1/Reddit_Subreddits.ndjson.zst
import ujson as json
import dask.bag as db

if __name__ == '__main__':
    bag = db.read_text("RC_2020-10.zst").map(json.loads)
    # df = bag.to_dataframe()
    bag.count().compute()
    exit()