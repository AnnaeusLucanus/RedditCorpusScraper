#!/usr/bin/env python
# coding: utf-8

# In[1]:


import praw
from psaw import PushshiftAPI

import time
import datetime as dt

import pandas as pd

import os

import re

# necessary to use PSAW
api = PushshiftAPI()

# necessary to use PRAW
reddit = praw.Reddit(
    client_id = "[YOUR_CLIENT_ID]",
    client_secret = "[YOUR_CLIENT_SECRET]",
    username = "[YOUR_USERNAME]",
    password = "[YOUR_PASSWORD]",
    user_agent = "corpus_generation_scraper_agent by u/[YOUR USERNAME]"
)

pd.set_option("display.max_rows", None)


# In[2]:


# specify subreddits and timeframe to be searched

subreddits = ['de','ich_iel']
start_year = 2010
end_year = 2022

# define directory on which to store the data

basecorpus = '/Users/maxim/Reddit_Corpus'

if not os.path.exists(basecorpus):
    os.makedirs(basecorpus)


# In[3]:


# define logging action as a simple print of ongoing activity

def log_action(action):
    print(action)
    return


# In[4]:


def sanitize_text(text):
    
    sanitized_text = str(text)
    sanitized_text = sanitized_text.replace('\r', '')
    sanitized_text = sanitized_text.replace('\n', ' ')
    sanitized_text = sanitized_text.replace('*', '')
    
    return sanitized_text


# In[5]:


complete_df = pd.DataFrame()

start_time_total = time.time()

for year in range(start_year, end_year+1):
    
    start_time = time.time()
    
    action = "[Year] " + str(year)
    log_action(action)

    # timestamps that define window of posts
    ts_after = int(dt.datetime(year, 1, 1).timestamp())
    ts_before = int(dt.datetime(year + 1, 1, 1).timestamp())

    for subreddit in subreddits:
        
        action = "\t[Subreddit] " + str(subreddit)
        log_action(action)

        # define the dictionary the comments will be stored on

        comments_dict = {
            "comment_id" : [],
            "comment_parent_id" : [],
            "comment_body" : [],
            "comment_link_id" : [],
            "comment_permalink" : [],
            "comment_created_utc" : [],
            "comment_score" : [],
            "comment_year" : [],
            "comment_subreddit" : []
        }

        # use PSAW to search for comments in the specified time frame and subreddit and for the specified phrase;
        # get only the comment IDs, and only a limited number of comments

        gen = api.search_comments(
            after = ts_after,
            before = ts_before,
            subreddit = subreddit,
            q = "uneingeweiht*", # "uninitiiert*"
            filter = ['id'],           
            limit = None
        )
        
        # for each PSAW comment found, check if it was deleted or remvoved;
        # then use PRAW to append the comments dictionary with the comment's data

        for comment in gen:
            
            comment_id = comment.d_['id']
            comment_praw = reddit.comment(id=comment_id)
            if not comment_praw.body == "[deleted]" and not comment_praw.body == "[removed]":
                
                sanitized_body = sanitize_text(comment_praw.body)
                
                comments_dict["comment_id"].append(comment_praw.id)
                comments_dict["comment_parent_id"].append(comment_praw.parent_id)
                comments_dict["comment_body"].append(sanitized_body)
                comments_dict["comment_link_id"].append(comment_praw.link_id)
                comments_dict["comment_permalink"].append(comment_praw.permalink)
                comments_dict["comment_created_utc"].append(comment_praw.created_utc)
                comments_dict["comment_score"].append(comment_praw.score)
                comments_dict["comment_year"].append(year)
                comments_dict["comment_subreddit"].append(subreddit)
                
        action = f"\t\tFound comments: {pd.DataFrame(comments_dict).shape[0]}"
        log_action(action)

        comments_df = pd.DataFrame(comments_dict)

        complete_df = pd.concat([comments_df,complete_df], ignore_index = True)
        
    # log the time passed

    action = f"\t\t[Info] Elapsed time: {time.time() - start_time: .2f}s"
    log_action(action)
    
action = f"[Info] Total elapsed time: {time.time() - start_time_total: .2f}s"
log_action(action)

print(complete_df)


# In[6]:


for i in complete_df.index:
    print("https://reddit.com" + str(complete_df.at[i, "comment_permalink"]))

