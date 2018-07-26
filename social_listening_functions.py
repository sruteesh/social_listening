
# coding: utf-8

import time
import numpy as np
import math,json,random
import csv,pandas as pd


from elasticsearch import helpers, Elasticsearch
import time,sys,os,re
from requests import get
from gmail import GMail, Message

import twitter
import googlemaps

import nltk
from collections import defaultdict
import datefinder,datetime
import webhoseio
from datetime import timedelta
import languages_countries_dict

from config import *
from flask import Flask, jsonify,request
from flask_cors import CORS

global path

import logging

api = twitter.Api(consumer_key=twitter_app_key,
                  consumer_secret=twitter_app_secret,
                  access_token_key=twitter_access_token,
                  access_token_secret=twitter_access_secret)


gmaps = googlemaps.Client(key=google_key)


language_dict = dict(languages_countries_dict.languages)
countries_dict = dict(languages_countries_dict.countries)

def get_key(_string):
    return ''.join(chr(int(i) + 97) for i in _string.split('|'))


alerts_password = get_key(password_encr)



from googleapiclient.discovery import build

def getService():
    service = build("customsearch", "v1",
            developerKey="AIzaSyCzXI8xt7_zy7FTQXaZgP37dhe49cmkAcs")

    return service


# from multiprocessing import Process,Manager

# m = Manager()
# master_locations = m.dict()


# #Declare our database variable and the file to store our data in
# master_location_coords = TinyDB('master_location_coords_tinydb.json')
# Search=Query()

# def get_location_coords(location):
#     result = master_location_coords.search(Search.location==location)
#     if len(result) >0:
#         return (result[0]['lat'],result[0]['lng'])
#     elif location is not None:
#         try:
#             print(location)
#             geocode_result = gmaps.geocode(location)
#             _dict = geocode_result[0]['geometry']['location']
#             master_locations[location] = (_dict['lat'],_dict['lng'])
#             return (_dict['lat'],_dict['lng'])
#         except Exception as e:
#             print(location)
#             logger.warning(e)
#             return None
#     else:
#         return None


def get_location_coords(location):

    master_location_coords = defaultdict(list)
    try:
        with open("./master_location_coords.json") as fout:
            for line in fout:
                line = json.loads(line)
                master_location_coords[line[0]] = line[1]
    except Exception as e:
        print(e)
        pass

    if location in master_location_coords:
        return master_location_coords[location]
    elif location is not None and len(location)>3:
        with open("./master_location_coords.json",'a') as fin:
            try:
                print(location)
                geocode_result = gmaps.geocode(location)
                _dict = geocode_result[0]['geometry']['location']
                json.dump((location,(_dict['lng'],_dict['lat'])),fin)
                fin.write("\n")
                return (_dict['lng'],_dict['lat'])
            except Exception as e:
                print("{} failed as {} ".format(location,e))
                return None
    else:
        return None

# master_location_coords = defaultdict(list)

# def get_location_coords(location):

#     if location in master_location_coords:
#         return master_location_coords[location]
#     elif location is not None and len(location)>3:
#         try:
#             print(location)
#             geocode_result = gmaps.geocode(location)
#             _dict = geocode_result[0]['geometry']['location']
#             return (_dict['lat'],_dict['lng'])
#         except Exception as e:
#             print(location)
#             logger.warning(e)
#             return None
#     else:
#         return None




def stop_words_list():
    '''
        A stop list specific to the observed timelines composed of noisy words
        This list would change for different set of timelines
    '''
    return ['amp','get','got','hey','hmm','hoo','hop','iep','let','ooo','par',
            'pdt','pln','pst','wha','yep','yer','aest','didn','nzdt','via',
            'one','com','new','like','great','make','top','awesome','best',
            'good','wow','yes','say','yay','would','thanks','thank','going',
            'new','use','should','could','really','see','want','nice',
            'while','know','free','today','day','always','last','put','live',
            'week','went','wasn','was','used','ugh','try','kind', 'http','much',
            'need', 'next','app','ibm','appleevent','using']


stoplist  = set(  nltk.corpus.stopwords.words("english")
                    + nltk.corpus.stopwords.words("french")
                    + nltk.corpus.stopwords.words("german")
                    + stop_words_list())





def get_clean_tweet(tweet):
    cleaned_tweet = re.sub('@\w+|RT', '', tweet)
    cleaned_tweet = re.sub(r"http\S+|www\S+", "", cleaned_tweet)
    cleaned_tweet = re.sub(r"[^A-z]", " ", cleaned_tweet)
    cleaned_tweet = re.sub(' +',' ',cleaned_tweet)
    cleaned_tweet = cleaned_tweet.strip()
    return cleaned_tweet





def get_clean_post(post):
    if post is not None:
        cleaned_post = re.sub("[^A-z]", " ", post)
        cleaned_post = re.sub(' +',' ',cleaned_post)
        cleaned_post = cleaned_post.strip()
        return cleaned_post
    else:
        return None

def get_post_tokens(post):
    if post is not None:
        return [i.lower() for i in post.split() if i not in stoplist and len(i)>2]
    else:
        return None





#blogs/news/discussions
def get_post_info(post):
    try:
        result= defaultdict(dict)
        output_thread = post['thread']
        result['id'] = output_thread['uuid']
        result['source_category'] = output_thread['site_type']
        result['domain'] = str(output_thread['site']).lower()
        result['domain_full'] = str(output_thread['site_full']).lower()
            
        if 'reddit' in result['domain']:
            result['source_category'] = 'reddit'
        elif 'youtube' in result['domain']:
            result['source_category'] = 'youtube'
        elif 'pinterest' in result['domain']:
            result['source_category'] = 'pinterest'

        result['source_url'] = output_thread['url']
        result['post_metrics']['num_shares'] = sum([(output_thread['social'][j]['shares']) for j in output_thread['social']])
        result['post_metrics']['num_comments'] = output_thread['replies_count']
        result['post_metrics']['num_likes'] = output_thread['participants_count']
        result['location']['country'] = countries_dict.get(output_thread['country'], output_thread['country']).lower()
        result['geo_coordinates'] = get_location_coords(result['location']['country'])
        result['language'] = post['language'].lower()
        
        result['text']['text'] = [post['text'] if post['text']!='' else None][0]
        result['text']['title'] = output_thread['title']
        result['text']['cleaned_text'] = get_clean_post(str(result['text']['text']))
        result['text']['text_tokens'] = get_post_tokens(result['text']['cleaned_text'])
        result['text']['text_type'] = None
        
        result['entities']['people'] = [i['name'].lower() for i in post['entities']['persons']]
        result['entities']['organizations'] = [i['name'].lower() for i in post['entities']['organizations']]
        result['entities']['hashtags'] = re.findall("#(\w+)",str(result['text']['text']).lower())
        
        result['user']['name'] = [post['author'].lower() if post['author']!='' else None][0]
        result['user']['vintage'] = None
        result['user']['screen_name'] = None
        result['user']['statuses_count'] = None
        result['user']['followers_count'] = None
        result['user']['favourites_count'] = None
        result['user']['friends_count'] = None
        result['user']['location'] = None
            
        result['published_date'] = datetime.datetime.strptime( post['published'].split('.')[0], "%Y-%m-%dT%H:%M:%S")
        result['crawled_date'] = datetime.datetime.strptime( post['crawled'].split('.')[0], "%Y-%m-%dT%H:%M:%S")

        return result
    except Exception as e:
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
        return 





def get_tweet_info(tweet):
    try:
        selected_info = defaultdict(dict)
        selected_info['id'] = str(tweet['id'])
        selected_info['source_category'] = 'twitter'
        selected_info['domain'] = [tweet['entities']['urls'][0]['display_url'].split('/')[0].lower() if len(tweet['entities']['urls'])>0 else None][0]
        selected_info['domain_full'] = selected_info['domain']
        
        if selected_info['domain'] is not None:
            if 'reddit' in selected_info['domain']:
                selected_info['source_category'] = 'reddit'
            elif 'youtube' in selected_info['domain']:
                selected_info['source_category'] = 'youtube'
            elif 'pinterest' in selected_info['domain']:
                selected_info['source_category'] = 'pinterest'
        
        selected_info['source_url'] = "https://twitter.com/i/web/status/"+tweet['id_str']
        selected_info['post_metrics']['num_likes'] = tweet['favorite_count']
        selected_info['post_metrics']['num_shares'] = tweet['retweet_count']
        selected_info['post_metrics']['num_comments'] = None
        selected_info['language'] = language_dict.get(tweet['lang'],tweet['lang']).lower()
        
        if tweet['place'] is not None:
            selected_info['location']['city'] = tweet['place']['name'].lower()
            selected_info['location']['country'] = tweet['place']['country'].lower()
        elif tweet['user']['time_zone'] is not None:
            selected_info['location']['country'] = tweet['user']['time_zone'].lower().replace('time','')
        else:
            selected_info['location']['country']=None
        selected_info['geo_coordinates'] = get_location_coords(selected_info['location']['country'])

        selected_info['text']['text'] = [tweet['text'] if tweet['text']!='' else None][0]
        selected_info['text']['cleaned_text'] = get_clean_tweet(selected_info['text']['text'])
        selected_info['text']['text_tokens'] = get_post_tokens(selected_info['text']['cleaned_text'] )
        selected_info['text']['title'] = [tweet['text'] if tweet['text']!='' else None][0]
    #     selected_info['text']['text_type'] = get_flag(tweet)


        selected_info['user']['name'] = tweet['user']['name'].lower()
        selected_info['user']['vintage'] = tweet['user']['created_at']
        selected_info['user']['screen_name'] = tweet['user']['screen_name'].lower()
        selected_info['user']['statuses_count'] = tweet['user']['statuses_count']
        selected_info['user']['followers_count'] = tweet['user']['followers_count']
        selected_info['user']['favourites_count'] = tweet['user']['favourites_count']
        selected_info['user']['friends_count'] = tweet['user']['friends_count']
        selected_info['user']['location'] = tweet['user']['location'].lower()
        selected_info['user']['id'] = tweet['user']['id']

        selected_info['entities']['hashtags'] = [i['text'].lower() for i in tweet['entities']['hashtags']]
        selected_info['entities']['people'] = [i['screen_name'].lower() for i in tweet['entities']['user_mentions']]
        selected_info['entities']['people_ids'] = [i['id'] for i in tweet['entities']['user_mentions']]

        selected_info['published_date'] = [i for i in datefinder.find_dates(tweet['created_at'])][0]
        selected_info['crawled_date'] = datetime.datetime.today()
        return selected_info
    except Exception as e:
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
        return 


def get_articles_info(post_tuple):
    try:
        post,source = post_tuple
        selected_info = defaultdict(dict)
        selected_info['id'] = None
        selected_info['post_metrics']['num_likes'] = None
        selected_info['post_metrics']['num_shares'] = None
        selected_info['post_metrics']['num_comments'] = None
        selected_info['language'] = 'en'
        selected_info['location']['city'] = None
        selected_info['location']['country'] = None
        selected_info['user']['vintage'] = None
        selected_info['user']['statuses_count'] = None
        selected_info['user']['favourites_count'] = None
        selected_info['user']['friends_count'] = None
        selected_info['user']['location'] = None
        selected_info['user']['id'] = None
        selected_info['entities']['people'] = None
        selected_info['entities']['people_ids'] = None

        selected_info['published_date'] = datetime.datetime.today() - datetime.timedelta(days=random.randint(0,30))
        selected_info['crawled_date'] = datetime.datetime.today()

        selected_info['source_url'] = post['link']
        selected_info['text']['text'] = post['snippet']
        selected_info['text']['cleaned_text'] = get_clean_tweet(selected_info['text']['text'])
        selected_info['text']['text_tokens'] = get_post_tokens(selected_info['text']['cleaned_text'] )
        selected_info['text']['title'] = post['title']

        if 'pinterest' in source:
            selected_info['source_category'] = 'pinterest'
            selected_info['domain'] = "pinterest.com"
            selected_info['domain_full'] = "www.pinterest.com"

            try:
                if 'pinner' in post['pagemap']['metatags'][0]:
                    author = post['pagemap']['metatags'][0]['pinterestapp:pinner']
                    followers = post['pagemap']['metatags'][0]['followers']
                elif 'pinterestapp:followers' in post['pagemap']['metatags'][0]:
                    author = post['link'].split('/')[-2]
                    followers = post['pagemap']['metatags'][0]['pinterestapp:followers']
                else:
                    author= None
                    followers=None
            except Exception as e:
                author= None
                followers=None
                print(e)
                pass

            selected_info['user']['name'] = author
            selected_info['user']['screen_name'] = author
            selected_info['user']['followers_count'] = followers
            selected_info['entities']['hashtags'] = [i for i in post['title'].split() if '#' in i]


        elif 'youtube' in source:
            
            try:
                author = post['pagemap']['person'][0]['url']
                if 'channel' in author:
                    author = post['pagemap']['metatags'][0]['twitter:title']
            except Exception as e:
                print('author error',e)
                author = None

            selected_info['user']['name'] = author
            selected_info['user']['screen_name'] = author
            selected_info['user']['followers_count'] = None
            selected_info['entities']['hashtags'] = [i for i in post['title'].split() if '#' in i]

            try:
                date = post['pagemap']['videoobject'][0]['datepublished']
                selected_info['published_date'] = [i for i in datefinder.find_dates(date)][0]
            except Exception as e:
                print(e)
                pass

            selected_info['source_category'] = 'youtube'
            selected_info['domain'] = "youtube.com"
            selected_info['domain_full'] = "www.youtube.com"

        elif 'reddit' in source:

            selected_info['user']['name'] = None
            selected_info['user']['screen_name'] = None
            selected_info['user']['followers_count'] = None
            selected_info['entities']['hashtags'] = [i for i in post['title'].split() if '#' in i]


            selected_info['source_category'] = 'reddit'
            selected_info['domain'] = post['displayLink']
            selected_info['domain_full'] = post['displayLink']

        else:

            selected_info['user']['name'] = None
            selected_info['user']['screen_name'] = None
            selected_info['user']['followers_count'] = None
            selected_info['entities']['hashtags'] = [i for i in post['title'].split() if '#' in i]


            selected_info['source_category'] = 'google'
            selected_info['domain'] = post['displayLink']
            selected_info['domain_full'] = post['displayLink']
    
        return selected_info

    except Exception as e:
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
        return None




def get_subscribed_keyword_posts(type,keywords,final_df):
    tokens=[]
    alerts_dict = defaultdict(list)
    if 'keyword' in type:
        for _,i in final_df.iterrows():
            tokens.extend(i['text']['text_tokens'])
            for keyword in keywords:
                if keyword in i['text']['text_tokens']:
                    alerts_dict[keyword].append(dict(i))
    elif type in ['hashtags','people']:
        for _,i in final_df.iterrows():
            tokens.extend(i['entities'][type])
            for keyword in keywords:
                if keyword in i['entities'][type]:
                    alerts_dict[keyword].append(dict(i))
    elif 'country' in type:
        for _,i in final_df.iterrows():
            tokens.append(i['location']['country'])
            for keyword in keywords:
                if keyword in i['location']['country']:
                    alerts_dict[keyword].append(dict(i))
                
    return alerts_dict





def remove_duplicates(articles):
    tmp_articles= []
    for _,i in enumerate(articles):
        tmp_articles.append((_,i['text']['cleaned_text']))
    df= pd.DataFrame(tmp_articles).drop_duplicates(subset=1)
    return [articles[i] for i in df[0].values]






def get_top_n(master_articles,n=5):
    articles = defaultdict(list)
    for i in master_articles:
        articles[i['source_category']].append(i)
        
    top_articles= defaultdict(list)
    for cat in articles:
        category_articles = remove_duplicates(articles[cat])
        by_shares= {}
        for _,i in enumerate(category_articles):
            by_shares[_] = i['post_metrics']['num_shares']
        sorted_by_shares = sorted(by_shares.items(),key=operator.itemgetter(1), reverse=True)
        top_articles[cat] = [category_articles[i[0]] for i in sorted_by_shares[:n]]
    return top_articles






def send_email(recipient,sender,password,keyword,html,attachment=False):

    # enter actual password, otherwise, nothing happens.
    gmail = GMail('Alerts <'+sender+'>',password)
    if attachment:
        message = Message('Alerts for '+keyword,
                          to=recipient,
                          html=html,
                          attachments=[attachment])
    else:
        message = Message('Alerts for '+keyword,
                  to=recipient,
                  html=html)

    gmail.send(message)





def get_article(i,keyword,media_type,top_n):
    
    url = top_n[media_type][i]['source_url']
    text = top_n[media_type][i]['text']['text'][:250]+'....'
    text_new= re.sub(keyword,"<b> "+keyword.upper()+" </b>", text, flags=re.IGNORECASE)
    published_time = str(top_n[media_type][i]['published_date'])
    location = top_n[media_type][i]['location']['country']

    if media_type=='twitter':
        title = top_n[media_type][i]['user']['name']
        domain = "twitter.com"
    else:
        title = top_n[media_type][i]['text']['title']
        domain = top_n[media_type][i]['domain']

    if location is None:
        location=' '
    if domain is None:
        domain = ' '
    article = """
<tr><td><table cellpadding="0" cellspacing="0" border="0" style="border-bottom:1px solid #e1e1e1;width:100%;padding:16px"><tbody><tr><td><table cellpadding="0" cellspacing="0" border="0" style="font-family:Helvetica,Verdana,Geneva,Arial,sans-serif"><tbody><tr><td colspan="2"><a style="color:#007aaf;font-weight:bold;text-decoration:none" href="{1}" rel="noreferrer" target="_blank" data-saferedirecturl="{1}&amp;source=gmail&amp;ust=1527408489512000&amp;usg=AFQjCNH6RmnH6I7dLwSRdv96CMPVjF1uKQ"
>{2}
</a></td></tr>
<tr><td colspan="2" style="padding:10px 0">
{3}
</tr>
<tr><td style="color:#878aaa;font-size:12px">{4} | {5} |
<a href="{1}" rel="noreferrer" target="_blank" data-saferedirecturl="https://www.google.com/url?hl=en&amp;q={1}&amp;source=gmail&amp;ust=1527408489512000&amp;usg=AFQjCNEStQKD0AMkGkrE5eR27Y2so_65DA">{6}</a></td></tr></tbody></table></td></tr></tbody></table></td></tr>
""".format(keyword,url,title,text_new,published_time, location,domain)
    
    return article





def get_html(keyword,media_type,alert_type, article1,article2):
    
    html = """<div style="background:#f6f6f6;padding:10px"><center>

<table cellpadding="0" cellspacing="0" border="0" bgcolor="#FFFFFF" style="border:1px solid #e1e1e1;width:600px"><tbody><tr><td><table cellpadding="0" cellspacing="0" border="0" style="border-bottom:1px solid #e1e1e1;width:100%;padding:16px 16px 20px 16px"><tr style="font-family:Helvetica,Verdana,Geneva,Arial,sans-serif;text-align:center"><td style="font-size:20px";padding-top:10px">
Latest {2} Alerts for <b>{0}</b>
</td></tr>

<tr><td><table cellpadding="0" cellspacing="0" border="0" bgcolor="#F2F2F2" style="border-bottom:1px solid #e1e1e1;color:#333333;font-family:Helvetica,Verdana,Geneva,Arial,sans-serif;width:100%;padding:8px 16px"><tbody><tr><td style="font-size:20px;font-weight:bold">
{1}
</td>

<td style="font-size:14px;text-align:right">
Top Results
</td></tr></tbody></table></td></tr>

{3}

{4}

<tr><td><table cellpadding="0" cellspacing="10" border="0" style="width:100%;padding:6px 6px"><tbody><tr><td style="width:25%"><table cellpadding="0" cellspacing="0" border="0" style="font-family:Helvetica,Verdana,Geneva,Arial,sans-serif;width:100%"><tbody><tr><td style="background:#e4e4e4;font-size:14px;font-weight:bold;padding:10px;text-align:center">

If you like our Alerts, please provide your review <a href="sruteeshkumar@gmail.com" style="color:#007aaf;text-decoration:none" rel="noreferrer" target="_blank" >here</a>!


</td></tr></tbody></table>

</center><div class="yj6qo"></div><div class="adL"></div></div><div class="adL"></div></div><div class="adL"></div></div><div class="adL">""".format(keyword,media_type,alert_type,article1,article2)
    
    return html



# <a href="https://www.facebook.com/talkwalker" style="color:#007aaf;text-decoration:none" rel="noreferrer" target="_blank" data-saferedirecturl="https://www.google.com/url?hl=en&amp;q=https://www.facebook.com/talkwalker&amp;source=gmail&amp;ust=1527408489512000&amp;usg=AFQjCNFCA9cvmt94pASZhOUYzSizNeu65A">liking</a> and 
# <a href="https://twitter.com/talkwalker" style="color:#007aaf;text-decoration:none" rel="noreferrer" target="_blank" data-saferedirecturl="https://www.google.com/url?hl=en&amp;q=https://twitter.com/talkwalker&amp;source=gmail&amp;ust=1527408489512000&amp;usg=AFQjCNGVcFj5j2UqXnjMYlV3NDVleisSVg">following</a>!

