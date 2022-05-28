from ast import Try
import peewee as pw
from peewee import *
from playhouse.postgres_ext import *
from playhouse.pool import (
    PooledPostgresqlExtDatabase,
    PostgresqlDatabase,
    PooledPostgresqlDatabase,
)


def init_database():
    db = PooledPostgresqlDatabase(
        "postgres",
        user="postgres",
        host="localhost",
        port=5432,
        password="postgres",
        stale_timeout=30,
    )
    return db


def init_models(db, reset_tables=False):
    class BaseModel(Model):
        class Meta:
            database = db

    class Comment(BaseModel):
        author = TextField()
        author_flair_text = TextField(null=True)
        author_fullname = TextField(null=True)
        author_premium = BooleanField(null=True)
        body = TextField()
        comment_type = TextField(null=True)
        controversiality = BigIntegerField()
        distinguished = TextField(null=True)
        gilded = BigIntegerField()
        id = TextField()  # id
        link_id = TextField()
        name = TextField(null=True)
        parent_id = TextField()
        permalink = TextField(null=True)

        created_utc = TimestampField(utc=True)
        approved_at_utc = TimestampField(null=True,utc=True)
        author_created_utc = TimestampField(null=True,utc=True)
        retrieved_utc = TimestampField(utc=True)  #  retrieved_on = BigIntegerField()

        score = BigIntegerField()
        stickied = BooleanField(null=True)
        subreddit = TextField()
        subreddit_id = TextField()
        subreddit_type = TextField(null=True)
        total_awards_received = BigIntegerField(null=True)

        json = JSONField(null=True)

    # class Submission(BaseModel):
        # pass

    # class Subreddit(BaseModel):
        # pass

    if db.is_closed():
        db.connect()
    if reset_tables and Comment.table_exists():
        db.drop_tables([Comment])
    db.create_tables([Comment])
    return {
        'comment': Comment,
        'submission': Submission
    }

if __name__ == "__main__":
    db = init_database()
    init_models(db,True)