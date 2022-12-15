#!/usr/bin/env python
# coding: utf-8

# - a scraper that searches specified subreddits in a specified timeframe for selfposts flaired as a "Tirade"
# - adapted from https://towardsdatascience.com/how-to-collect-a-reddit-dataset-c369de539114
# - TO DO:
#     - implement a caps finder and caps percentage calculator
#     - implement a punctuation analyzer
#     - search comments for a rate ("/10")

# In[1]:


import praw
from psaw import PushshiftAPI

import time
import datetime as dt

import pandas as pd

import os

import re

import enchant
dict_de = enchant.Dict("de_DE")
dict_en = enchant.Dict("en_US")

from difflib import SequenceMatcher

from string import digits

import statistics as stat

# to use PSAW
api = PushshiftAPI()

# to use PRAW
reddit = praw.Reddit(
    client_id = "[YOUR_CLIENT_ID]",
    client_secret = "[YOUR_CLIENT_SECRET]",
    username = "[YOUR_USERNAME]",
    password = "[YOUR_PASSWORD]",
    user_agent = "corpus_generation_scraper_agent by u/[YOUR USERNAME]"
)


# In[20]:


# define subreddits and timeframe to be searched

subreddits = ['de']
start_year = 2021
end_year = 2022

# define directory on which to store the data

basecorpus = '/Users/maxim/Reddit_Corpus'

if not os.path.exists(basecorpus):
    os.makedirs(basecorpus)
    
pd.set_option("display.max_rows",None)


# In[3]:


# define logging action

def log_action(action):
    print(action)
    return


# In[21]:


# define the maximum number of threads to be searched for the flair specified below

max_searches = 100000

score_tally = []

complete_df = pd.DataFrame()

for year in range(start_year, end_year+1):
    
    action = "[Year] " + str(year)
    log_action(action)


    # timestamps that define window of posts
    ts_after = int(dt.datetime(year, 1, 1).timestamp())
    ts_before = int(dt.datetime(year+1, 1, 1).timestamp())

    for subreddit in subreddits:
        start_time = time.time()

        action = "\t[Subreddit] " + subreddit
        log_action(action)
        
        # define dictionary that submissions are stored on

        submissions_dict = {
            "id" : [],
            "permalink" : [],
            "score" : [],
            "num_comments": [],
            "created_utc" : [],
            "flair" : []
        }
        
        j = 0        

        # use PSAW to search for comments in the specified time frame and subreddit and for the specified phrase;
        # get only the comment IDs, and only a limited number of comments

        gen = api.search_submissions(
            after = ts_after,
            before = ts_before,
            subreddit = subreddit,
            filter = ['id','score'],
            # use <None> here to get all posts in timeframe            
            limit = max_searches
        )

        # for each PSAW comment found, check if it was deleted or remvoved;
        # then use PRAW to append the comments dictionary with the comment's data
        
        j = 0
        l = 0

        for submission in gen:
            
            # every 100 submissions searched, display how many submissions have been searched
            
            j += 1
            if j / 1000 == int(j / 1000):
                action = "\t\t" + str(j) + " submissions searched"
                log_action(action)
                
                if j / 5000 == int(j / 5000):
                    action = f"\t\t[Info] Elapsed time: {time.time() - start_time: .2f}s"
                    log_action(action)
            
            if int(submission.d_['score']) > 1:
                
                l += 1
                if l / 100 == int(l / 100):
                    action = "\t\t" + str(l) + " submissions with a score greater than 1 found"
                    log_action(action)

                submission_id = submission.d_['id']
                submission_praw = reddit.submission(id=submission_id)

                submissions_dict["id"].append(submission_praw.id)
                submissions_dict["permalink"].append(submission_praw.permalink)
                submissions_dict["score"].append(submission_praw.score)
                submissions_dict["num_comments"].append(submission_praw.num_comments)
                submissions_dict["created_utc"].append(submission_praw.created_utc)
                submissions_dict["flair"].append(submission_praw.link_flair_text)
                
            else:
                score_tally.append(int(submission.d_['score']))
                

        # log how many submissions were found
        
        action = f"\t\t[Info] Found submissions: {pd.DataFrame(submissions_dict).shape[0]}"
        log_action(action)

        # log the time passed
        
        action = f"\t\t[Info] Elapsed time: {time.time() - start_time: .2f}s"
        log_action(action)
        
        # turn the submissions dictionary into a dataframe and display that dataframe;
        # then add that dataframe to the complete dataframe
        
        submissions_df = pd.DataFrame(submissions_dict)
        print(submissions_df)
        
        complete_df = pd.concat([submissions_df,complete_df], ignore_index = True)    


# In[22]:


print(score_tally)
print(len(score_tally))


# In[23]:


print(complete_df)


# In[25]:


flair_dict = {
    "flairs" : [],
    "counter" : [],
    "ids" : [],
    "permalinks" : []
}

for i in range(len(complete_df.index)):
    flair = complete_df.at[i, "flair"]
    if flair not in flair_dict["flairs"]:
        flair_dict["flairs"].append(str(flair))
        flair_dict["counter"].append(1)
        flair_dict["ids"].append(str(complete_df.at[i, "id"]))
        flair_dict["permalinks"].append(str(complete_df.at[i, "permalink"]))
    else:
        k = flair_dict["flairs"].index(flair)
        flair_dict["counter"][k] += 1
        flair_dict["ids"][k] = str(flair_dict["ids"][k] + " , " + str(complete_df.at[i, "id"]))
        flair_dict["permalinks"][k] = str(flair_dict["permalinks"][k] + " , " + str(complete_df.at[i, "permalink"]))
        
flair_df = pd.DataFrame(flair_dict)
print(flair_df)


# In[26]:


flair_df.sort_values(by=["counter"], inplace=True, ascending=False)
flair_df.reset_index(drop=True)


# In[20]:


print(flair_df)


# In[27]:


score_dict = {
    "flairs" : [],
    "counter" : [],
    "total_score" : [],
    "avg_score" : [],
    "median_score" : []
}

score_lists = {}

for i in range(len(complete_df.index)):
    flair = complete_df.at[i, "flair"]
    if flair not in score_dict["flairs"]:
        score_dict["flairs"].append(str(flair))
        score_dict["counter"].append(1)
        score_dict["total_score"].append(complete_df.at[i, "score"])
        score_dict["avg_score"].append(0)
        score_dict["median_score"].append(0)
        score_lists[flair] = []
        score_lists[flair].append(complete_df.at[i, "score"])
    else:
        k = score_dict["flairs"].index(flair)
        score_dict["counter"][k] += 1
        score_dict["total_score"][k] = score_dict["total_score"][k] + complete_df.at[i, "score"]
        score_lists[flair].append(complete_df.at[i, "score"])
        
score_df = pd.DataFrame(score_dict)

for k in range(score_df.index.size):
    score_df.at[k, "avg_score"] = round(score_df.at[k, "total_score"] / score_df.at[k, "counter"])
    flair = score_df.at[k, "flairs"]
    try:
        score_df.at[k, "median_score"] = round(stat.median(score_lists[flair]))
    except:
        score_df.at[k, "median_score"] = "n/a"
    
print(score_df)


# In[28]:


score_df.sort_values(by=["counter"], inplace=True, ascending=False)
score_df.reset_index(drop=True)


# In[31]:


print(score_df)


# In[33]:


subcorpus = "flair_analysis"
date = str(dt.date.today())

if not os.path.exists(basecorpus + '/' + subcorpus):
    os.makedirs(basecorpus + '/' + subcorpus)
    
submissions_df.to_csv(basecorpus + '/' + subcorpus + '/' + "submissions_df-" + date + ".csv",
                                                          index=False)
complete_df.to_csv(basecorpus + '/' + subcorpus + '/' + "complete_df-" + date + ".csv",
                                                          index=False)
flair_df.to_csv(basecorpus + '/' + subcorpus + '/' + "flair_df-" + date + ".csv",
                                                          index=False)
score_df.to_csv(basecorpus + '/' + subcorpus + '/' + "score_df-" + date + ".csv",
                                                          index=False)


# In[34]:


subcorpus = "flair_analysis"
date = str(dt.date.today())

if not os.path.exists(basecorpus + '/' + subcorpus):
    os.makedirs(basecorpus + '/' + subcorpus)
    
submissions_df.to_excel(basecorpus + '/' + subcorpus + '/' + "submissions_df-" + date + ".xlsx",
                                                          index=False)
complete_df.to_excel(basecorpus + '/' + subcorpus + '/' + "complete_df-" + date + ".xlsx",
                                                          index=False)
flair_df.to_excel(basecorpus + '/' + subcorpus + '/' + "flair_df-" + date + ".xlsx",
                                                          index=False)
score_df.to_excel(basecorpus + '/' + subcorpus + '/' + "score_df-" + date + ".xlsx",
                                                          index=False)

