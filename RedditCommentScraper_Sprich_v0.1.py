#!/usr/bin/env python
# coding: utf-8

# In[1]:


import praw # pip install praw
from psaw import PushshiftAPI # pip install psaw

import time
import datetime as dt

import pandas as pd

import os

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

subreddits = ['ich_iel']
start_year = 2016
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


# In[ ]:


complete_df = pd.DataFrame()

for year in range(start_year, end_year+1):
    
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
            "comment_level" : [],
            "comment_year" : [],
            "comment_subreddit" : []
        }
        
        # use PSAW to search for comments in the specified time frame and subreddit and for the specified phrase;
        # get only the comment IDs, and only a limited number of comments
        
        gen = api.search_comments(
            after = ts_after,
            before = ts_before,
            subreddit = subreddit,
            q = "Sprich",
            filter = ['id'],
            # use <None> here to get all posts in timeframe            
            limit = 500
        )

        # for each PSAW comment found, use PRAW to append the comments dictionary with the comment's data

        for comment in gen:
                     
            comment_id = comment.d_['id']
            comment_praw = reddit.comment(id=comment_id)
            
            # if the comment contains only the specified phrase and wasn't deleted,
            # append the dictionary with the comment's data

            if not " " in comment_praw.body and not comment_praw.body == "[deleted]" and not comment_praw.body == "[removed]":
                comments_dict["comment_id"].append(comment_praw.id)
                comments_dict["comment_parent_id"].append(comment_praw.parent_id)
                comments_dict["comment_body"].append(comment_praw.body)
                comments_dict["comment_link_id"].append(comment_praw.link_id)
                comments_dict["comment_permalink"].append(comment_praw.permalink)
                comments_dict["comment_created_utc"].append(comment_praw.created_utc)
                comments_dict["comment_score"].append(comment_praw.score)
                comments_dict["comment_level"].append(0)
                comments_dict["comment_year"].append(year)
                comments_dict["comment_subreddit"].append(subreddit)
                
                # check if the comment has replies;
                # if it does and the replies weren't deleted,
                # append the dictionary with the best reply's data and check if the reply has replies;
                # do this until no replies are found
                
                f = 1
                
                while f != 0:
                    try:
                        comment_praw.refresh()
                        comment_praw.reply_sort = "top"                                                
                        comment_praw = comment_praw.replies[0]
                        
                        if not comment_praw.body == "[deleted]" and not comment_praw.body == "[removed]":
                            comments_dict["comment_id"].append(comment_praw.id)
                            comments_dict["comment_parent_id"].append(comment_praw.parent_id)
                            comments_dict["comment_body"].append(comment_praw.body)
                            comments_dict["comment_link_id"].append(comment_praw.link_id)
                            comments_dict["comment_permalink"].append(comment_praw.permalink)
                            comments_dict["comment_created_utc"].append(comment_praw.created_utc)
                            comments_dict["comment_score"].append(comment_praw.score)
                            comments_dict["comment_level"].append(f)
                            comments_dict["comment_year"].append(year)
                            comments_dict["comment_subreddit"].append(subreddit)
                            f += 1
                            
                        else:
                            f = 0
                            
                    except:
                        f = 0

    # turn the comment dictionary into a dataframe and add that to the complete dataframe
        
    comments_df = pd.DataFrame(comments_dict)

    print(comments_df)
    
    complete_df = pd.concat([comments_df,complete_df], ignore_index = True)    


# In[2]:


complete_df


# In[ ]:


subcorpus = "speak"
date = str(dt.date.today())

if not os.path.exists(basecorpus + '/' + subcorpus):
    os.makedirs(basecorpus + '/' + subcorpus)
    
complete_df.to_csv(basecorpus + '/' + subcorpus + '/' + "complete_df-" + date + ".csv",
                                                          index=False)
complete_df.to_excel(basecorpus + '/' + subcorpus + '/' + "complete_df-" + date + ".xlsx",
                                                          index=False)

