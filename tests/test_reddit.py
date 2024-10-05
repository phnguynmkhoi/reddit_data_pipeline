from praw import Reddit
import praw
from dotenv import load_dotenv
import os

load_dotenv()
client_id = os.getenv('id')
secret = os.getenv('secret')
subreddit = 'datascience'

keys = ('id', 'title', 'author','num_comments','upvote_ratio', 'score','created_utc', 'url', 'over_18')

try:
    reddit_instance = Reddit(client_id=client_id,client_secret=secret, user_agent='ubuntu:data_pipeline:v0.1 (by /u/Chance_Strategy_9522)')
    subreddit = reddit_instance.subreddit(subreddit)
    hot_sub = subreddit.top(time_filter='day',limit=100)
    for x in hot_sub:
        x = vars(x)
        print(sorted(x.keys()))
        print([x.get(key) for key in keys])
        break

except Exception as e:
    print(e)