#!/usr/bin/env python
# coding: utf-8

# In[6]:


import requests

import urllib

import pandas as pd

from requests_html import HTML # pip install requests_html
from requests_html import HTMLSession


# In[11]:


# Return the source code for the provided URL 
    # Args:
        # url (string): URL of the page to scrape
    # Returns:
        # response (object): HTTP response object from requests_html

def get_source(url):
    
    try:
        session = HTMLSession()
        response = session.get(url)
        return response

    except requests.exceptions.RequestException as e:
        print(e)


# In[49]:


# Return the number of google.de results for a search query 
    # Args:
        # query (string): term to search
    # Returns:
        # num (integer): number of google.de results for the query

def get_num(query):
    
    # replace spaces in URL with "+"
    query = urllib.parse.quote_plus(query)
    
    # get HTTP response corresponding to page of google.de search for query
    response = get_source("https://www.google.de/search?q=" + query)
    
    # search that response for CSS id "result-stats" (cf. https://www.w3schools.com/cssref/css_selectors.asp)
    num_text = response.html.find("#result-stats", first = True).text
    
    # isolate and sanitize the number of results, then turn it into an integer
    num = num_text.split(" Ergebnisse")[0]    
    num = num.split("Ungefähr ")[1]    
    num = num.replace(".", "")    
    num = int(num)
    
    return num

