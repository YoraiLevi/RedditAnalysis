CREATE TABLE IF NOT EXISTS comments(
   controversiality       BIT 
  ,score                  INTEGER 
  ,parent_id              
  ,ups                    INTEGER 
  ,archived               VARCHAR(4)
  ,link_id                
  ,author_flair_text      
  ,retrieved_on           INTEGER 
  ,downs                  INTEGER 
  ,body                   
  ,edited                 VARCHAR(5)
  ,author                 
  ,id                      PRIMARY KEY
  ,subreddit              
  ,author_flair_css_class 
  ,score_hidden           VARCHAR(5)
  ,name                   
  ,created_utc            INTEGER 
  ,subreddit_id           
  ,distinguished          VARCHAR(30)
  ,gilded                 BIT 
);
-- INSERT INTO comments(controversiality,score,parent_id,ups,archived,link_id,author_flair_text,retrieved_on,downs,body,edited,author,id,subreddit,author_flair_css_class,score_hidden,name,created_utc,subreddit_id,distinguished,gilded) VALUES (0,3,'t1_c02kjs5',3,'true','t3_61vdm',NULL,1427420993,0,'As a Canadian:

-- Fuck you, sir.','false','Battleloser','c02kkc5','politics',NULL,'false','t1_c02kkc5',1196499365,N't5_2cneq',NULL,0);
