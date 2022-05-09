CREATE TABLE IF NOT EXISTS comments(
   controversiality       BIT 
  ,score                  INTEGER 
  ,parent_id              VARCHAR(10)
  ,ups                    INTEGER 
  ,archived               VARCHAR(4)
  ,link_id                VARCHAR(8)
  ,author_flair_text      VARCHAR(30)
  ,retrieved_on           INTEGER 
  ,downs                  INTEGER 
  ,body                   VARCHAR(30)
  ,edited                 VARCHAR(5)
  ,author                 VARCHAR(11)
  ,id                     VARCHAR(7) PRIMARY KEY
  ,subreddit              VARCHAR(8)
  ,author_flair_css_class VARCHAR(30)
  ,score_hidden           VARCHAR(5)
  ,name                   VARCHAR(10)
  ,created_utc            INTEGER 
  ,subreddit_id           NVARCHAR(8)
  ,distinguished          VARCHAR(30)
  ,gilded                 BIT 
);
-- INSERT INTO comments(controversiality,score,parent_id,ups,archived,link_id,author_flair_text,retrieved_on,downs,body,edited,author,id,subreddit,author_flair_css_class,score_hidden,name,created_utc,subreddit_id,distinguished,gilded) VALUES (0,3,'t1_c02kjs5',3,'true','t3_61vdm',NULL,1427420993,0,'As a Canadian:

-- Fuck you, sir.','false','Battleloser','c02kkc5','politics',NULL,'false','t1_c02kkc5',1196499365,N't5_2cneq',NULL,0);
