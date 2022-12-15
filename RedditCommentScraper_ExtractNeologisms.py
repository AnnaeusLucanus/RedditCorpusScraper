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

from difflib import SequenceMatcher

import GetNumGoogleResults as gngr

# language specific imports
import langid    # pip install langid
import enchant   # pip install pyenchant
dict_en = enchant.Dict("en_US")
dict_de = enchant.Dict("de_DE")

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

pd.set_option("display.max_rows",None)


# In[2]:


# specify subreddits and timeframe to be searched

subreddits = ['de','ich_iel']
start_year = 2017
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
    sanitized_text = re.sub(r'\(?http[^ \n\r\t]+\)?', '', sanitized_text)
    return sanitized_text


# In[7]:


complete_df = pd.DataFrame()

innovations = {
    "new_word" : [],
    "comment_id" : [],
    "comment_body" : [],
    "comment_permalink" : [],
    "tag_comment_id" : []
}

for year in range(start_year, end_year+1):
    
    action = "[Year] " + str(year)
    log_action(action)

    # timestamps that define window of posts
    ts_after = int(dt.datetime(year, 1, 1).timestamp())
    ts_before = int(dt.datetime(year + 1, 1, 1).timestamp())

    for subreddit in subreddits:
        
        action = "[Subreddit] " + str(subreddit)
        log_action(action)

        # use PSAW to search for comments in the specified time frame and subreddit and for the specified phrase;
        # get only the comment IDs, and only a limited number of comments

        gen = api.search_comments(
            after = ts_after,
            before = ts_before,
            subreddit = subreddit,
            q = "eingedeutsch*",
            filter = ['id'],
            # use <None> here to get all posts in timeframe            
            limit = 100
        )

        # for each PSAW comment found, check if it was deleted or remvoved;
        # then use PRAW to append the comments dictionary with the comment's data

        for comment in gen:
            comment_id = comment.d_['id']
            tag_comment = reddit.comment(id = comment_id)
            
            if not "t3_" in tag_comment.parent_id:
                tagged_comment_id = tag_comment.parent_id.split("_")[1]                
                tagged_comment = reddit.comment(id = tagged_comment_id)                
                
                sanitized_body = sanitize_text(tagged_comment.body)

                if not sanitized_body == "[deleted]" and not sanitized_body == "[removed]":

                    for token in re.findall(r'\w+', sanitized_body, re.UNICODE):
                        if not token in innovations["new_word"]:
                            if dict_de.check(token) is False and dict_en.check(token) is False:
                                try:
                                    ratio1 = SequenceMatcher(a = token, b = dict_de.suggest(token)[0]).ratio()
                                    ratio2 = 1 - 1 / len(token)
                                    if ratio1 < ratio2:
                                        innovations["new_word"].append(token)
                                        innovations["comment_id"].append(tagged_comment_id)
                                        innovations["comment_body"].append(sanitized_body)
                                        innovations["comment_permalink"].append(tagged_comment.permalink)
                                        innovations["tag_comment_id"].append(comment_id)

                                except:
                                    try:
                                        ratio1 = SequenceMatcher(a = token, b = dict_en.suggest(token)[0]).ratio()
                                        ratio2 = 1 - 1 / len(token)
                                        if ratio1 < ratio2:
                                            innovations["new_word"].append(token)
                                            innovations["comment_id"].append(tagged_comment_id)
                                            innovations["comment_body"].append(sanitized_body)
                                            innovations["comment_permalink"].append(tagged_comment.permalink)
                                            innovations["tag_comment_id"].append(comment_id)
                                    except:
                                        innovations["new_word"].append(token)
                                        innovations["comment_id"].append(tagged_comment_id)
                                        innovations["comment_body"].append(sanitized_body)
                                        innovations["comment_permalink"].append(tagged_comment.permalink)
                                        innovations["tag_comment_id"].append(comment_id)


# In[8]:


innovations


# In[11]:


dict_de.suggest("gibts")


# In[14]:


token = "gibts"
ratio1 = SequenceMatcher(a = token, b = dict_en.suggest(token)[0]).ratio()
ratio2 = 1 - 1 / len(token)
print(ratio1)
print(ratio2)


# In[10]:


innovations_df = pd.DataFrame(innovations)
print(innovations_df)


# In[11]:


subcorpus = "neologisms"
date = str(dt.date.today())

if not os.path.exists(basecorpus + '/' + subcorpus):
    os.makedirs(basecorpus + '/' + subcorpus)
    
innovations_df.to_csv(basecorpus + '/' + subcorpus + '/' + "innovations_df-" + date + ".csv",
                                                          index=False)


# In[12]:


subcorpus = "neologisms"
date = str(dt.date.today())

if not os.path.exists(basecorpus + '/' + subcorpus):
    os.makedirs(basecorpus + '/' + subcorpus)
    
innovations_df.to_excel(basecorpus + '/' + subcorpus + '/' + "innovations_df-" + date + ".xlsx",
                                                          index=False)

