CREATE TABLE IF NOT EXISTS comments(
  controversiality        BIT --convert
  ,score                  INTEGER 
  ,parent_id              INTEGER --convert
  ,ups                    INTEGER 
  ,archived               BIT --convert
  ,link_id                INTEGER --convert
  ,author_flair_text      TEXT
  ,retrieved_on           INTEGER 
  ,downs                  INTEGER 
  ,body                   TEXT
  ,edited                 BIT --convert
  ,author                 TEXT
  ,id                     INTEGER PRIMARY KEY
  ,subreddit              TEXT
  ,author_flair_css_class TEXT
  ,score_hidden           BIT --convert
  ,name                   TEXT
  ,created_utc            TIME --convert? 
  ,subreddit_id           INTEGER --convert
  ,distinguished          
  ,gilded                 BIT --convert
);
-- INSERT INTO comments(controversiality,score,parent_id,ups,archived,link_id,author_flair_text,retrieved_on,downs,body,edited,author,id,subreddit,author_flair_css_class,score_hidden,name,created_utc,subreddit_id,distinguished,gilded) VALUES (0,3,'t1_c02kjs5',3,'true','t3_61vdm',NULL,1427420993,0,'As a Canadian:

-- Fuck you, sir.','false','Battleloser','c02kkc5','politics',NULL,'false','t1_c02kkc5',1196499365,N't5_2cneq',NULL,0);
