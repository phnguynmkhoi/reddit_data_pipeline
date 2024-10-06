from praw import Reddit
import praw
from dotenv import load_dotenv
import os

load_dotenv()
client_id = os.getenv('client_id')
secret = os.getenv('secret_key')
agent = os.getenv('user_agent')
subreddit = 'dataengineering'

keys = ('id', 'title', 'selftext', 'author','num_comments','upvote_ratio', 'score','created_utc', 'over_18', 'url')

try:
    reddit_instance = Reddit(client_id=client_id,client_secret=secret, user_agent=agent)
    subreddit = reddit_instance.subreddit(subreddit)
    hot_sub = subreddit.top(time_filter='day',limit=100)
    for x in hot_sub:
        x = vars(x)
        print(sorted(x.keys()))
        print([x.get(key) for key in keys])
        break

except Exception as e:
    print(e)