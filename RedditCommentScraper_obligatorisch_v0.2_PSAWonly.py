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

pd.set_option("display.max_rows",None)


# In[2]:


# specify subreddits and timeframe to be searched

subreddits = ['de']
start_year = 2020
end_year = 2020

# define directory on which to store the data

basecorpus = '/Users/maxim/Reddit_Corpus'

if not os.path.exists(basecorpus):
    os.makedirs(basecorpus)


# In[3]:


# define logging action as a simple print of ongoing activity

def log_action(action):
    print(action)
    return


# In[19]:


def sanitize_text(text):
    
    sanitized_text = str(text)
    sanitized_text = sanitized_text.replace('\r', '')
    sanitized_text = sanitized_text.replace('\n', ' ')
    sanitized_text = sanitized_text.replace('*', '')
    sanitized_text = sanitized_text.replace('&gt;', '')
    sanitized_text = re.sub(r'\(?http[^ \n\r\t]+\)?', '', sanitized_text)
    
    return sanitized_text


# In[26]:


complete_df = pd.DataFrame()

oblig_links = {
    "links" : [],
    "counter" : [],
    "tag_ids" : [],
    "tag_permalinks" : []
}

sub_links_dict = {
    "links" : [],
    "counter" : [],
    "tag_ids" : [],
    "tag_permalinks" : []
}

non_link_comments = {
    "id" : [],
    "permalink" : [],
    "body" : []
}

for year in range(start_year, end_year+1):
    
    action = "[Year] " + str(year)
    log_action(action)

    # timestamps that define window of posts
    ts_after = int(dt.datetime(year, 1, 1).timestamp())
    ts_before = int(dt.datetime(year + 1, 1, 1).timestamp())

    for subreddit in subreddits:
        
        start_time = time.time()
        
        action = "\t[Subreddit] " + str(subreddit)
        log_action(action)


        # use PSAW to search for comments in the specified time frame and subreddit and for the specified phrase;
        # get only the comment IDs, and only a limited number of comments

        gen = api.search_comments(
            after = ts_after,
            before = ts_before,
            subreddit = subreddit,
            q = "obligatorisch*",
            filter = ['id', 'body', 'permalink'],
            # use <None> here to get all posts in timeframe            
            limit = None
        )

        # for each PSAW comment found, check if it was deleted or remvoved;
        # then use PRAW to append the comments dictionary with the comment's data
        
        j = 0

        for comment in gen:
            
            j += 1
            if j / 100 == int(j / 100):
                print("\t\t" + str(j) + " comments searched")
            
            body = comment.d_['body']
            comm_id = comment.d_['id']
            permalink = comment.d_["permalink"]
            
            links = re.findall(r"https*:\/\/[^)\] \n]*", body)

            if links:

                links = list(set(links))

                for link in links:

                    if not link in oblig_links["links"]:
                        oblig_links["links"].append(link)
                        oblig_links["counter"].append(1)
                        oblig_links["tag_ids"].append(comm_id)
                        oblig_links["tag_permalinks"].append(permalink)

                    else:
                        i = oblig_links["links"].index(link)
                        oblig_links["counter"][i] += 1
                        oblig_links["tag_ids"][i] = str(oblig_links["tag_ids"][i] + " , " + comm_id)
                        oblig_links["tag_permalinks"][i] = str(oblig_links["tag_permalinks"][i] + " , " + permalink)
            
            else:
                sub_links = re.findall(r"[^. \n\t\r]\/?r\/\w+", body) # needs improvement

                if sub_links:

                    sub_links = list(set(sub_links))

                    for link in sub_links:

                        if not link in sub_links_dict["links"]:
                            sub_links_dict["links"].append(link)
                            sub_links_dict["counter"].append(1)
                            sub_links_dict["tag_ids"].append(comm_id)
                            sub_links_dict["tag_permalinks"].append(permalink)

                        else:
                            i = sub_links_dict["links"].index(link)
                            sub_links_dict["counter"][i] += 1
                            sub_links_dict["tag_ids"][i] = str(sub_links_dict["tag_ids"][i] + " , " + comm_id)
                            sub_links_dict["tag_permalinks"][i] = str(sub_links_dict["tag_permalinks"][i] + " , " + permalink)
        
                else:
                    non_link_comments["id"].append(comm_id)
                    non_link_comments["permalink"].append(permalink)
                    body = sanitize_text(body)
                    non_link_comments["body"].append(body)
                    
        # log the time passed
        
        action = f"\t[Info] Elapsed time: {time.time() - start_time: .2f}s"
        log_action(action)


# In[28]:


collocations_df = pd.DataFrame(non_link_comments)

print(collocations_df)


# In[27]:


sub_links_df = pd.DataFrame(sub_links_dict)

print(sub_links_df)


# In[25]:


permalinks = sub_links_df.at[13, "tag_permalinks"].split(" , ")

for permalink in permalinks:
    print("https://reddit.com" + str(permalink))


# In[11]:


for i in range(len(non_link_comments)):
    print(non_link_comments[i])


# In[29]:


links_df = pd.DataFrame(oblig_links)

print(links_df.sort_values("counter"))


# In[7]:


permalinks = links_df.at[148, "tag_permalinks"].split(" , ")

for permalink in permalinks:
    print("https://reddit.com" + str(permalink))


# In[24]:


for i in links_df.index:
    
    permalinks = links_df.at[i, "tag_permalinks"].split(" , ")
    
    for permalink in permalinks:
        print("https://reddit.com" + str(permalink))

