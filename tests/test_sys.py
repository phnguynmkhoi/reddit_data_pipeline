
import sys
import os

sys.path.append('/home/mkhoii/PycharmProjects/reddit_data_pipeline')

print(os.path.dirname(os.path.abspath(__file__)))
from pipelines.reddit_pipeline import *

reddit_pipeline('temp','nsfw','day',10)