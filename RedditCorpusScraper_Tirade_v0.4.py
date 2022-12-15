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

# to use PRAW
reddit = praw.Reddit(
    client_id = "[YOUR_CLIENT_ID]",
    client_secret = "[YOUR_CLIENT_SECRET]",
    username = "[YOUR_USERNAME]",
    password = "[YOUR_PASSWORD]",
    user_agent = "corpus_generation_scraper_agent by u/[YOUR USERNAME]"
)


# In[2]:


# define subreddits to be searched

subreddits = ['de']


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


# In[4]:


def sanitize_text(text):
    sanitized_text = str(text)
    sanitized_text = sanitized_text.replace('\r', '')
    sanitized_text = sanitized_text.replace('\n', ' ')
    sanitized_text = sanitized_text.replace('*', '')
    sanitized_text = re.sub(r'\(?http[^ \n\r\t]+\)?', '', sanitized_text)
    return sanitized_text


# In[5]:


# define the maximum number of threads to be searched for the flair specified below;
# absolute maximum due to PRAW restrictions is 250

max_searches = 250

complete_df = pd.DataFrame()

methods = ["relevance", "hot", "top", "new", "comments"]


for sort_method in methods:

    action = "[Method] " + sort_method
    log_action(action)
            
    
    # define dictionary that submissions are stored on

    submissions_dict = {
        "id" : [],
        "url" : [],
        "title" : [],
        "score" : [],
        "num_comments": [],
        "created_utc" : [],
        "selftext" : []
    }

    for subreddit in subreddits:
        start_time = time.time()

        action = "\t[Subreddit] " + subreddit
        log_action(action)

        
        j = 0

        for submission_praw in reddit.subreddit(subreddit).search('flair:"tirade"', limit=max_searches, sort=sort_method):
            
            # every 100 submissions searched, display how many submissions have been searched
            
            j += 1
            if j / 100 == int(j / 100):
                print("\t\t" + str(j) + " submissions searched")
            
            # check whether the submission is a self-post and hasn't been deleted or removed;
            # check whether the submission crosses a score threshhold;
            # if all checks are passed, add the submission to the submission dictionary
            
            if submission_praw.is_self == True:
                if not submission_praw.selftext == "[deleted]" and not submission_praw.selftext == "[removed]":
                    if submission_praw.score > 99:
                        if sort_method == "relevance":                        
                            submissions_dict["id"].append(submission_praw.id)
                            submissions_dict["url"].append(submission_praw.url)
                            submissions_dict["title"].append(submission_praw.title)
                            submissions_dict["score"].append(submission_praw.score)
                            submissions_dict["num_comments"].append(submission_praw.num_comments)
                            submissions_dict["created_utc"].append(submission_praw.created_utc)
                            submissions_dict["selftext"].append(submission_praw.selftext)
                        else:
                            if not submission_praw.id in set(complete_df['id']):
                                submissions_dict["id"].append(submission_praw.id)
                                submissions_dict["url"].append(submission_praw.url)
                                submissions_dict["title"].append(submission_praw.title)
                                submissions_dict["score"].append(submission_praw.score)
                                submissions_dict["num_comments"].append(submission_praw.num_comments)
                                submissions_dict["created_utc"].append(submission_praw.created_utc)
                                submissions_dict["selftext"].append(submission_praw.selftext)

        # log how many submissions were found
        
        subm_num = pd.DataFrame(submissions_dict).shape[0]
        action = f"\t\t[Info] Found submissions: {subm_num}"
        log_action(action)

        # log the time passed
        
        action = f"\t\t[Info] Elapsed time: {time.time() - start_time: .2f}s"
        log_action(action)
        
        # turn the submissions dictionary into a dataframe and display that dataframe;
        # then add that dataframe to the complete dataframe
        
        submissions_df = pd.DataFrame(submissions_dict)
        
        complete_df = pd.concat([submissions_df,complete_df], ignore_index = True)    


# In[6]:


complete_df


# In[7]:


# create empty lists to later add to the dataframe

san_st_list = []
num_token_list = []
num_caps_list = []
perc_caps_list = []

caps_de = []
caps_en = []
caps_typo = []
caps_unknown = []

# do the following for each submission found in the previous step

for i in range(len(complete_df.index)):
    
    # sanitize the selftext of the submission and save that
    
    sanitized_selftext = sanitize_text(str(complete_df.at[i, "selftext"]))    
    san_st_list.append(sanitized_selftext)
    
    token_counter = 0
    caps_counter = 0
    
    # tokenize the sanitized selftext and count the tokens
    
    for token in re.findall(r'\w+', sanitized_selftext, re.UNICODE):
        
        token_counter += 1
        
        # look for tokens in all caps
        
        if token == token.upper():
            
            # check if they're only numbers; if they aren't, count them as caps tokens
            # then, try to figure out whether they're German or English words or misspellings of those
            # if they aren't, mark them as unknown
            
            remove_digits = str.maketrans('', '', digits)
            only_letters = token.translate(remove_digits)
            if len(only_letters) > 1:
                
                caps_counter += 1
                
                if dict_de.check(token) is True:
                    caps_de.append(token)
                        
                elif dict_en.check(token) is True:
                    caps_en.append(token)
                            
                else:
                    try:
                        ratio1 = SequenceMatcher(a = token, b = dict_de.suggest(token)[0]).ratio()
                        ratio2 = 1 - 1 / len(token)
                        if ratio1 >= ratio2:
                            caps_typo.append(token)
                        else:
                            caps_unknown.append(token)
                    except:
                        try:
                            ratio1 = SequenceMatcher(a = token, b = dict_en.suggest(token)[0]).ratio()
                            ratio2 = 1 - 1 / len(token)
                            if ratio1 >= ratio2:
                                caps_typo.append(token)
                            else:
                                caps_unknown.append(token)
                        except:
                            caps_unknown.append(token)
                            
    # save how many tokens and caps tokens were in the submission's selftext
    # then, calculate how many of the total number of tokens were caps tokens
    
    num_token_list.append(token_counter)
    num_caps_list.append(caps_counter)
    perc_caps_list.append(caps_counter / token_counter)
            
# finally, append the dataframe created in the previous step with all the new data

complete_df['sanitized_selftext'] = san_st_list
complete_df['num_tokens'] = num_token_list
complete_df['num_caps_tokens'] = num_caps_list
complete_df['percentage_caps_tokens'] = perc_caps_list


# In[8]:


complete_df


# In[31]:


# define dictionary for results to be stored on

cluster_dict = {
    "id" : [],
    "url" : [],
    "san_st" : [],
    "cluster" : [],
    "length" : [],
    "is_verbal" : [],
    "rant_score" : []
}

cluster_list = []
cluster_len_list = []

# go through each submission found

for i in range(len(complete_df.index)):
    
    san_st = str(complete_df.at[i, "sanitized_selftext"])
    
    # look for punctuation mark clusters via a regular expression, consisting of the following parts:
    # a) 1 or more "?" or "!" b) 0 or more "?", "!", or "1"
    # c) 0 or more "eins" or "elf" in upper or lower case d) 0 or more "?", "!", or "1"
    # check if the found cluster consists of more than 1 mark
    # if it does record the corresponding data
    # finally, turn all the data into a dataframe
    
    k = 0
    l = 0
    
    for cluster in re.findall(r'([\?!]+[\?!1]*(eins|elf)*[\?!1]*)', san_st, re.IGNORECASE):
        if len(cluster[0]) > 1:
            k += 1
            l += len(cluster[0])
            cluster_dict["id"].append(complete_df.at[i, "id"])
            cluster_dict["url"].append(complete_df.at[i, "url"])
            cluster_dict["san_st"].append(complete_df.at[i, "sanitized_selftext"])
            cluster_dict["cluster"].append(cluster[0])
            cluster_dict["length"].append(len(cluster[0]))
            if "e" in cluster[0] or "E" in cluster[0]:
                cluster_dict["is_verbal"].append(True)
            else:
                cluster_dict["is_verbal"].append(False)
            cluster_dict["rant_score"].append(complete_df.at[i, "score"])
            
    cluster_list.append(k)
    if k != 0:
        cluster_len_list.append(l / k)
    else:
        cluster_len_list.append(0)

cluster_df = pd.DataFrame(cluster_dict)
complete_df['cluster'] = cluster_list
complete_df['cluster_avg_len'] = cluster_len_list


# In[32]:


cluster_df


# In[11]:


def search_rating(praw_comment):
    if "/10" in praw_comment.body or "von 10" in praw_comment.body:
        return praw_comment.id


# In[23]:


# define dictionary for results to be stored on

comments_dict = {
    "comment_id" : [],
    "comment_sanitized_body" : [],
    "comment_link_id" : [],
    "comment_permalink" : [],
    "comment_created_utc" : [],
    "comment_score" : []
}

comm_counter = 0

# start timer for logging purposes

start_time = time.time()

# for every submission found

for i in range(len(complete_df.index)):
    
    # get that submission and its comments
    # do not load more comments (as that takes a very significant amount of time)
    
    submission_id = complete_df.at[i, "id"]
    tirade = reddit.submission(id = submission_id)
    
    tirade.comments.replace_more(limit = 0)
    
    # for every comment found, count it
    # then check whether the comment contains a rating (i.e., "/10" or "von 10")
    # if it does, save the data
    
    for comment in tirade.comments:
        
        comm_counter += 1
        if comm_counter / 100 == int(comm_counter / 100):
            action = "\t\t" + str(i + 1) + " submission(s) and " + str(comm_counter) + " comments searched"
            log_action(action)
           
        comment_id = search_rating(comment)
        
        if comment_id:
            
            sanitized_body = sanitize_text(comment.body)
            
            comments_dict["comment_id"].append(comment_id)
            comments_dict["comment_sanitized_body"].append(sanitized_body)
            comments_dict["comment_link_id"].append(comment.link_id)
            comments_dict["comment_permalink"].append(comment.permalink)
            comments_dict["comment_created_utc"].append(comment.created_utc)
            comments_dict["comment_score"].append(comment.score)
        
# log the time this operation took and how many submissions and comments were looked at

action = f"\t\t[Info] Elapsed time: {time.time() - start_time: .2f}s for " + str(i + 1) + " submission(s) and " + str(comm_counter) + " comments"
log_action(action)

# turn the dicitionary with the saved data into a dataframe and print that

ratings_df = pd.DataFrame(comments_dict)
print(ratings_df)


# In[24]:


for i in range(len(ratings_df.index)):
    ratings_df.at[i, "comment_permalink"] = "https://www.reddit.com" + str(ratings_df.at[i, "comment_permalink"])
    
ratings_df


# In[25]:


for i in range(len(ratings_df.index)):
    ratings_df.at[i, "comment_id"] = "<t1_" + str(ratings_df.at[i, "comment_id"]) + ">"
    
ratings_df


# In[21]:


ges_bew_text


# In[33]:


subcorpus = "Tiraden"
date = str(dt.date.today())

if not os.path.exists(basecorpus + '/' + subcorpus):
    os.makedirs(basecorpus + '/' + subcorpus)
    
complete_df.to_csv(basecorpus + '/' + subcorpus + '/' + "complete_df-" + date + ".csv",
                                                          index=False)
ratings_df.to_csv(basecorpus + '/' + subcorpus + '/' + "ratings_df-" + date + ".csv",
                                                          index=False)
cluster_df.to_csv(basecorpus + '/' + subcorpus + '/' + "cluster_df-" + date + ".csv",
                                                          index=False)


# In[36]:


subcorpus = "Tiraden"
date = str(dt.date.today())

if not os.path.exists(basecorpus + '/' + subcorpus):
    os.makedirs(basecorpus + '/' + subcorpus)
    
complete_df.to_excel(basecorpus + '/' + subcorpus + '/' + "complete_df-" + date + ".xlsx",
                                                          index=False)
ratings_df.to_excel(basecorpus + '/' + subcorpus + '/' + "ratings_df-" + date + ".xlsx",
                                                          index=False)
cluster_df.to_excel(basecorpus + '/' + subcorpus + '/' + "cluster_df-" + date + ".xlsx",
                                                          index=False)


# In[ ]:


# trying to see if PSAW is faster
# unfortunately, some PSAW parameters seem to not be functioning
# also, some comments are lost (see below)
# userWarning: Got non 200 code 429
# warnings.warn("Got non 200 code %s" % response.status_code)
# C:\Users\maxim\anaconda3\lib\site-packages\psaw\PushshiftAPI.py:180: UserWarning: Unable to connect to pushshift.io. Retrying after backoff.

"""
comments_dict = {
    "comment_id" : [],
    "comment_body" : [],
    "comment_link_id" : [],
    "comment_permalink" : [],
    "comment_created_utc" : [],
    "comment_score" : []
}

comm_counter = 0
non_top_counter = 0

start_time = time.time()

for i in complete_df.index:
    
    tirade_id = complete_df.at[i, "id"]
    
    gen = api.search_comments(
        q = "10",
        link_id = tirade_id, # this works
        nest_level = 1, # this doesn't seem to do anything
        size = 500
    )

    for comment in gen:
        
        parent_id = comment.d_['parent_id']
        
        if "t3_" in parent_id:
        
            comm_counter += 1
            if comm_counter / 10 == int(comm_counter / 10):
                print("\t\t" + str(comm_counter) + " potential comments found")

            body = comment.d_['body']

            if "/10" in body or "von 10" in body:

                comment_id = comment.d_['id']
                praw_comment = reddit.comment(id=comment_id)
                
                if not praw_comment.body == "[deleted]" and not praw_comment.body == "[removed]":

                    sanitized_body = sanitize_text(praw_comment.body)

                    comments_dict["comment_id"].append(praw_comment.id)
                    comments_dict["comment_body"].append(sanitized_body)
                    comments_dict["comment_link_id"].append(praw_comment.link_id)
                    comments_dict["comment_permalink"].append(praw_comment.permalink)
                    comments_dict["comment_created_utc"].append(praw_comment.created_utc)
                    comments_dict["comment_score"].append(praw_comment.score)
                    
        else:
            non_top_counter += 1

action = f"\n\t\t[Info] Elapsed time: {time.time() - start_time: .2f}s for " + str(i + 1) + " submissions and " + str(comm_counter) + " comments\n\t\t\t[Notice] " + str(non_top_counter) + " non-top-level comments were considered\n"
log_action(action)                
                
ratings_df = pd.DataFrame(comments_dict)
print(ratings_df)
"""


# In[5]:


# older version checking google.de for unknown words
"""
    for token in re.findall(r'\w+', sanitized_selftext, re.UNICODE):
        
        token_counter += 1
        
        if token == token.upper():
            remove_digits = str.maketrans('', '', digits)
            only_letters = token.translate(remove_digits)
            if len(only_letters) > 1:
                if dict_de.check(token) is False:
                    if dict_de.check(token[0] + token[1:].lower()) is True or dict_de.check(token.lower()) is True:
                        caps_counter += 1
                        caps_dict.append(token)
                    else:
                        if dict_en.check(token[0] + token[1:].lower()) is True or dict_en.check(token.lower()) is True:
                            caps_counter += 1
                            angl_dict.append(token)
                        else:
                            try:
                                ratio1 = SequenceMatcher(a = token, b = dict_de.suggest(token)[0]).ratio()
                                ratio2 = 1 - 1 / len(token)
                                if ratio1 >= ratio2:
                                    caps_counter += 1
                                    caps_dict.append(token)
                                else:
                                    num_google = gngr.get_num(token)
                                    if num_google > 1500:
                                        caps_counter += 1
                                        caps_dict.append(token)
                                    else:
                                        neol_dict.append(token)
                            except:
                                num_google = gngr.get_num(token)
                                if num_google > 1500:
                                    caps_counter += 1
                                    caps_dict.append(token)
                                else:
                                    neol_dict.append(token)
"""


# In[4]:


# older version thinking dict_de/en.check were case sensitive
"""
            remove_digits = str.maketrans('', '', digits)
            only_letters = token.translate(remove_digits)
            if len(only_letters) > 1:
                
                if dict_de.check(token) is False:
                    caps_counter += 1
                
                    if dict_de.check(token[0] + token[1:].lower()) is True or dict_de.check(token.lower()) is True:
                        caps_de.append(token)
                        
                    elif dict_en.check(token[0] + token[1:].lower()) is True or dict_en.check(token.lower()) is True:
                        caps_en.append(token)
                            
                    else:
                        try:
                            ratio1 = SequenceMatcher(a = token, b = dict_de.suggest(token)[0]).ratio()
                            ratio2 = 1 - 1 / len(token)
                            if ratio1 >= ratio2:
                                caps_typo.append(token)
                            else:
                                caps_unknown.append(token)
                        except:
                            try:
                                ratio1 = SequenceMatcher(a = token, b = dict_en.suggest(token)[0]).ratio()
                                ratio2 = 1 - 1 / len(token)
                                if ratio1 >= ratio2:
                                    caps_typo.append(token)
                                else:
                                    caps_unknown.append(token)
                            except:
                                caps_unknown.append(token)
"""

