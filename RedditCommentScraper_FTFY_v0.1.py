#!/usr/bin/env python
# coding: utf-8

# - to add maybe
#     - detect Eindeutschungen by splitting the string at "rdfd" and looking for English words with dict_en.check() at \[0\]
#     - better detection of neologisms
#     - faster tokenization
#         - https://stackoverflow.com/questions/15057945/how-do-i-tokenize-a-string-sentence-in-nltk
#         - https://www.nltk.org/
#         - https://stackoverflow.com/questions/41912083/nltk-tokenize-faster-way

# In[16]:


import praw
from psaw import PushshiftAPI

import time
import datetime as dt

import pandas as pd

import os

import re

import nltk
tokenizer = nltk.RegexpTokenizer('\w+')

import enchant
dict_de = enchant.Dict("de_DE")
dict_en = enchant.Dict("en_US")

from difflib import SequenceMatcher

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


# In[5]:


# specify subreddits and timeframe to be searched

subreddits = ['de','ich_iel']
start_year = 2020
end_year = 2020

# define directory on which to store the data

basecorpus = '/Users/maxim/Reddit_Corpus'

if not os.path.exists(basecorpus):
    os.makedirs(basecorpus)


# In[6]:


# define logging action as a simple print of ongoing activity

def log_action(action):
    print(action)
    return


# In[15]:


def sanitize_text(text):
    
    sanitized_text = str(text)
    sanitized_text = sanitized_text.replace('\r', '')
    sanitized_text = sanitized_text.replace('\t', ' ')
    sanitized_text = sanitized_text.replace('\n', ' ')
    sanitized_text = sanitized_text.replace('*', '')
    sanitized_text = re.sub(r'\(?http[^ \n\r\t]+\)?', '', sanitized_text)
    
    return sanitized_text


# In[8]:


#from difflib import SequenceMatcher

def is_known_word(word):
    
    if dict_de.check(word) is True or dict_en.check(word) is True:
        decision = True
        
    elif dict_de.check(word.lower()) == True or dict_en.check(word.lower()) == True:
        decision = True
        
    elif dict_de.check(word.upper()[0] + word.lower()[1:]) == True or dict_en.check(word.upper()[0] + word.lower()[1:]) == True:
        decision = True
                            
    else:
        try:
            ratio1 = SequenceMatcher(a = word, b = dict_de.suggest(word)[0]).ratio()
            ratio2 = 1 - 1 / len(word)
            if ratio1 >= ratio2:
                decision = True
            else:
                decision = False
        except:
            try:
                ratio1 = SequenceMatcher(a = word, b = dict_en.suggest(word)[0]).ratio()
                ratio2 = 1 - 1 / len(word)
                if ratio1 >= ratio2:
                    decision = True
                else:
                    decision = False
            except:
                decision = False
    
    return decision


# In[14]:


complete_df = pd.DataFrame()

neol_dict = {
    "potential_neologism" : [],
    "comment_id" : [],
    "comment_body" : [],
    "comment_permalink" : []
}

complete_neol_df = pd.DataFrame()

comment_counter = 0

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
            q = "rdfd",
            filter = ['id'],           
            limit = 100
        )

        # for each PSAW comment found, check if it was deleted or remvoved;
        # then use PRAW to append the comments dictionary with the comment's data

        for comment in gen:

            comment_counter += 1
            if comment_counter / 100 == int(comment_counter / 100):
                print("\t\t" + str(comment_counter) + " comments searched")

            comment_id = comment.d_['id']
            comment_praw = reddit.comment(id = comment_id)

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

                token_list = tokenizer.tokenize(sanitized_body)

                for token in token_list:

                    if token.lower() != "rdfd":

                        if not is_known_word(token):

                            neol_dict["potential_neologism"].append(token)
                            neol_dict["comment_id"].append(comment_praw.id)
                            neol_dict["comment_body"].append(sanitized_body)
                            neol_dict["comment_permalink"].append(comment_praw.permalink)



        action = f"\t\tFound comments: {pd.DataFrame(comments_dict).shape[0]}"
        log_action(action)

        comments_df = pd.DataFrame(comments_dict)
        complete_df = pd.concat([comments_df,complete_df], ignore_index = True)

        neol_df = pd.DataFrame(neol_dict)
        complete_neol_df = pd.concat([neol_df,complete_neol_df], ignore_index = True)



    # log the time passed

    action = f"\t\t[Info] Elapsed time: {time.time() - start_time: .2f}s"
    log_action(action)

action = f"[Info] Total elapsed time: {time.time() - start_time_total: .2f}s"
log_action(action)

print(complete_df)


# In[10]:


print(complete_neol_df)


# In[15]:


for i in complete_df.index:
    print("https://reddit.com" + str(complete_df.at[i, "comment_permalink"]))


# In[17]:


text = "Bahnhof Görlitz ist der zentrale Personenbahnhof der Stadt Görlitz in Sachsen. Er verknüpft im Eisenbahnknoten Görlitz die Strecken in Richtung Berlin, Dresden, Breslau und Zittau miteinander. Bis zum Zweiten Weltkrieg war der am 1.  September 1847 eröffnete Bahnhof Görlitz ein bedeutender Knotenpunkt im deutschen Fernverkehr. Das steigende Verkehrsaufkommen erforderte in den 1860er Jahren sowie Anfang des 20. Jahrhunderts eine Erweiterung seiner Anlagen. Nach der Verschiebung der deutschen Ostgrenze an Oder und Neiße kam es zu einem enormen Bedeutungsverlust. Heute ist er nur noch ein Regionalknoten im Schienenpersonennahverkehr. Fernverkehr in der einst bedeutsamen Relation (Paris –) Dresden – Breslau (– Warschau) gibt es seit 2004 nicht mehr. Hier werden täglich 120 Züge sowie 3600 Reisende und Besucher gezählt. Er ist Deutschlands östlichster Bahnhof.[1] Görlitz ist Grenzbahnhof zwischen Deutschland und Polen. Bis zum EU-Beitritt Polens bzw. dem Beitritt zum Schengen-Raum erfolgte dort in allen internationalen Zügen die Zoll- und Passkontrolle."

wordlist = tokenizer.tokenize(text)

for word in wordlist:
    if is_known_word(word):
        print("yes")
    else:
        print("no")

