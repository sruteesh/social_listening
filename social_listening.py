
# coding: utf-8

import time
import numpy as np
import math,json
import csv,pandas as pd
from multiprocessing import Pool
from functools import partial
import gc
import ast
import operator

from elasticsearch import helpers, Elasticsearch
import csv,pandas as pd
import datetime,re

import time,sys,os
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

app = Flask(__name__,
            static_url_path='', 
            static_folder='web/static',
            template_folder='web/templates')
CORS(app)


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
 
from multiprocessing import Process,Manager

m = Manager()
master_locations = m.dict()


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
#             print(e)
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
                print(location)
                print(e)
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
#             print(e)
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
    result= defaultdict(dict)
    output_thread = post['thread']
    result['id'] = output_thread['uuid']
    result['source_category'] = output_thread['site_type']
    result['domain'] = str(output_thread['site']).lower()
    result['domain_full'] = str(output_thread['site_full']).lower()
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





def get_tweet_info(tweet):

    selected_info = defaultdict(dict)
    selected_info['id'] = str(tweet['id'])
    selected_info['source_category'] = 'twitter'
    selected_info['domain'] = [tweet['entities']['urls'][0]['display_url'].split('/')[0].lower() if len(tweet['entities']['urls'])>0 else None][0]
    selected_info['domain_full'] = selected_info['domain']
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





def get_latest_crawl_parameter(keyword,media='blogs'):
    try:
        if not os.path.exists(path):
            print('No keyword folder found!! Exiting')
            return None
        else:
            folders = os.listdir(path)

            files=[]
            for file in folders:
                if '.json' in file:
                    if media in file:
                        if os.path.getsize(path+'/'+file)>0:
                            name = int(file.split('_')[-1].strip('.json'))
                            files.append(name)

            latest_file = str(sorted(files)[-1])

            if "blogs" in media:
                latest_file_path = path+'/'+keyword+"_blogs_news_discussions_"+str(date_today) +'_'+latest_file+'.json'
                data= []
                with open(latest_file_path) as fin:
                    for line in fin:
                        line = json.loads(line)
                        data.append(line)
                    
                latest_crawl_since = datetime.datetime.timestamp(datetime.datetime.strptime(data[-1]['crawled'].split('.')[0],'%Y-%m-%dT%H:%M:%S'))
            elif 'twitter' in media:
                latest_file_path = path+'/'+keyword+"_twitter_"+str(date_today) +'_'+latest_file+'.json'
                data= []
                with open(latest_file_path) as fin:
                    for line in fin:
                        line = json.loads(line)
                        data.append(line)
                        
                latest_crawl_since = data[0][0]['id']
            return latest_crawl_since
        
    except Exception as e:
        print(e)
        print("Couldn't get latest crawl parameter!!", e)
        return None





def get_blogs_news(keyword,streaming=True):


    if not os.path.exists(path):
        os.makedirs(path)

    webhoseio.config(token=blogs_key)

    if streaming:
        latest_crawl_since = get_latest_crawl_parameter(keyword,media='blogs')
    else:
        latest_crawl_since = None

    prvs_crawl_since = -1

    count=0
    results=[]    
    with open(path+'/'+keyword+"_blogs_news_discussions_"+str(date_today) +'_'+ str(date_timestamp)+".json",'w') as fin:
        while True:
            try:
                print(count)
                print(latest_crawl_since)
                query_params = {
                "q": 'thread.title: '+ keyword+ ', language:english',
                "sort": "crawled",
                "size" : 100,
                "ts": latest_crawl_since,
                }            
                if prvs_crawl_since==latest_crawl_since:
                    break
                else:
                    if count>5:
                        break
                    output = webhoseio.query("filterWebContent", query_params)
                    prvs_crawl_since = latest_crawl_since
                    if len(output['posts'])>0:
                        count+=1
                        print(len(output['posts']))
                        for post in output['posts']:
                            try:
                                json.dump(post,fin)
                                fin.write('\n')
                                results.append(post)
                            except Exception as e:
                                print(e)
                        latest_crawl_since = datetime.datetime.timestamp(datetime.datetime.strptime(results[-1]['crawled'].split('.')[0],'%Y-%m-%dT%H:%M:%S'))
                    else:
                        break
            except Exception as e:
                print(e)
                break
    return results


def get_twitter(keyword,streaming=True):


    if not os.path.exists(path):
        os.makedirs(path)

    date_timestamp = str(datetime.datetime.today().timestamp()).split('.')[0]
    
    if streaming:
        latest_crawl_since = get_latest_crawl_parameter(keyword, media='twitter')
    else:
        latest_crawl_since=None        
        
    master_results = []
    prvs_crawl_since = -1
    with open(path+'/'+keyword+"_twitter_"+str(date_today)+'_'+str(date_timestamp)+".json",'w') as fin:

        if not latest_crawl_since:
            for i in range(10):
                print('getting ', (i+1)*100)
                if prvs_crawl_since==latest_crawl_since:
                    return master_results
                else:
                    results = api.GetSearch(keyword,count=100,result_type='recent',return_json=True,max_id=latest_crawl_since)
                    if len(results['statuses'])>0:
                        json.dump(results['statuses'],fin)
                        master_results.extend(results['statuses'])
                        fin.write('\n')
                        prvs_crawl_since = latest_crawl_since
                        latest_crawl_since = results['statuses'][-1]['id']
                    else:
                        break

        else:        
            results = api.GetSearch(keyword,count=100,result_type='recent',return_json=True,since_id=latest_crawl_since)
            if len(results['statuses'])>0:
                json.dump(results['statuses'],fin)
                master_results.extend(results['statuses'])
                fin.write('\n')

                for i in range(10):
                    try:
                        print('2 getting ', (i+1)*100)
                        results = api.GetSearch(keyword,count=100,result_type='recent',return_json=True,since_id=results['statuses'][0]['id'])
                        if len(results['statuses'])>0:
                            json.dump(results['statuses'],fin)
                            master_results.extend(results['statuses'])
                            fin.write('\n')
                        else:
                            break
                    except Exception as e:
                        print(e)
                        if 'out of range' in str(e).lower():
                            return master_results
                        else:
                            print(e)
                            pass
    return master_results




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

def Master_blogs_function(keyword):

    tmp_blogs = get_blogs_news(keyword)

    try:
        blogs_done_files=[]
        with open(path+"/blogs_done_files_"+str(date_today)+'.txt') as fin:
            for line in fin:
                blogs_done_files.append(line.strip("\n"))
    except Exception as e:
        blogs_done_files = []

    #Load Blogs
    if os.path.exists(path):
        remaining_files= list(set(os.listdir(path)) - set(blogs_done_files))
        if len(remaining_files)>0:
            blogs_news=[]
            for file in remaining_files:
                if 'blogs_news' in file:
                    with open(path+'/'+file) as fin:
                        for line in fin:
                            line= json.loads(line)
                            blogs_news.append(line)


    blogs_done_files = remaining_files

    with open(path+"/blogs_done_files_"+str(date_today)+'.txt','a') as fin:
        for line in blogs_done_files:
            fin.write(line+'\n')

    # Process Blogs

    t1 = time.time()
    cleaned_results = []
    blogs_multiprocess_pool = [p.apply_async(get_post_info, [i]) for i in blogs_news]
    for i,result in enumerate(blogs_multiprocess_pool):
        try:
            output = result.get()
            output['keyword'] = keyword
            cleaned_results.append(output)
        except Exception as e:
            print("BLOGS EXCEPTION!!",e)
            print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
            pass

    return cleaned_results


def Master_twitter_function(keyword):
#### Twitter

    results = get_twitter(keyword)

    try:
        twitter_done_files=[]
        with open(path+"/twitter_done_files_"+str(date_today)+'.txt') as fin:
            for line in fin:
                twitter_done_files.append(line.strip("\n"))
    except Exception as e:
        twitter_done_files = []


    # Load Tweets

    if os.path.exists(path):
        remaining_twitter_files= list(set(os.listdir(path)) - set(twitter_done_files))
        if len(remaining_twitter_files)>0:
            twitter=[]
            for file in remaining_twitter_files:
                if 'twitter' in file and '.json' in file:
                    with open(path+'/'+file) as fin:
                        for line in fin:
                            line= json.loads(line)
                            twitter.append(line)


    twitter_done_files = remaining_twitter_files


    with open(path+"/twitter_done_files_"+str(date_today)+'.txt','a') as fin:
        for line in twitter_done_files:
            fin.write(line+'\n')

    # Process tweets

    t1 = time.time()
    cleaned_twitter = []
    tweets = [tweet for tweet in sum(twitter,[])]


    tweets_multiprocess_pool = [p.apply_async(get_tweet_info, [i]) for i in tweets]

    for i,result in enumerate(tweets_multiprocess_pool):
        try:
            output = result.get()
            output['keyword'] = keyword
            cleaned_twitter.append(output)
        except Exception as e:
            print("TWITTER EXCEPTION!!",e)
            print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
            pass

    return cleaned_twitter



def Upload_to_kibana(data):

    es = Elasticsearch(host)

    # try :
    #    es.indices.delete(index=index_name, ignore=[400, 404])
    #    print('index deleted')
    # except Exception as e:
    #    print(e)
    #    pass

    records = data

    actions = [{
        "_index": index_name,
        "_type": doc_type,
        "_id": keyword+'_'+str(date_today)+'_'+str(i),
        "_source": j} for i,j in enumerate(records)]

    return helpers.bulk(es, actions=actions)


num_processes = 6
def pool_init():  
    gc.collect()

p = Pool(initializer=pool_init, processes = num_processes)



@app.route('/run_social_listening', methods=['POST'])
def run_social_listening():

    global keyword

    try:
        _input = request.get_json(force=True)
        print(_input)
        keyword = _input['keyword']
    except Exception as e:
        print(e)
        return handle_exceptions(message='Keyword not given, EXITING !!',response_code=421)


    global date_today
    global date_timestamp
    global path
    global master_locations_coords
    global master_locations

    date_today = datetime.datetime.today().date()
    date_timestamp = str(datetime.datetime.today().timestamp()).split('.')[0]

    path = "data/" +keyword+'/'+str(date_today)

    cleaned_results = Master_blogs_function(keyword)
    cleaned_twitter = Master_twitter_function(keyword)

    final_result = cleaned_results+cleaned_twitter
    final_df = pd.DataFrame(final_result)

    final_df.to_csv(path+'/'+keyword+"_all_social_media_"+str(date_today)+".csv",index=False,mode='a')


    # master_location_coords_dict = dict(master_locations)

    # for i in master_location_coords_dict:
    #     master_location_coords.insert({"location":i,"lat":master_location_coords_dict[i][0],"lng":master_location_coords_dict[i][1]})


    # with open("./master_location_coords.json",'a') as fin:
    #     for line in master_location_coords:
    #         json.dump((line,master_location_coords[line]),fin)
    #         fin.write("\n")


    print("uploading {} articles to kibana".format(int(final_df.shape[0])))

    try:
        Upload_to_kibana(final_result)
    except Exception as e:
        print(e)
        return handle_exceptions(message="Problem in Building Dashboard",response_code=430)

    return jsonify(response="Dashboard Built", url = "http://185.90.51.142:5601/app/kibana#/dashboard/0ac89420-5287-11e8-8ab0-3f731bc5c361?_g=(refreshInterval:(display:Off,pause:!f,value:0),time:(from:now-30d,mode:quick,to:now))&_a=(description:'',filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'663c8a20-8115-11e8-ba2e-69a0a3013ee4',key:keyword.keyword,negate:!f,params:(query:{},type:phrase),type:phrase,value:{}),query:(match:(keyword.keyword:(query:{},type:phrase))))),fullScreenMode:!f,options:(darkTheme:!f,hidePanelTitles:!f,useMargins:!t),panels:!((gridData:(h:20,i:'1',w:48,x:0,y:24),id:'07b340d0-5266-11e8-bada-23eb8c6d65ff',panelIndex:'1',type:visualization,version:'6.3.0'),(gridData:(h:8,i:'2',w:48,x:0,y:16),id:c2c82ac0-5266-11e8-bada-23eb8c6d65ff,panelIndex:'2',type:visualization,version:'6.3.0'),(gridData:(h:23,i:'5',w:29,x:0,y:74),id:e191b1f0-5285-11e8-8ab0-3f731bc5c361,panelIndex:'5',type:visualization,version:'6.3.0'),(embeddableConfig:(vis:(legendOpen:!f)),gridData:(h:15,i:'6',w:32,x:0,y:59),id:'35836920-5286-11e8-8ab0-3f731bc5c361',panelIndex:'6',type:visualization,version:'6.3.0'),(gridData:(h:19,i:'7',w:29,x:0,y:97),id:b9233360-5285-11e8-8ab0-3f731bc5c361,panelIndex:'7',type:visualization,version:'6.3.0'),(gridData:(h:15,i:'8',w:16,x:32,y:59),id:'74e71770-5285-11e8-8ab0-3f731bc5c361',panelIndex:'8',type:visualization,version:'6.3.0'),(embeddableConfig:(vis:(legendOpen:!f)),gridData:(h:23,i:'9',w:19,x:29,y:74),id:'628886d0-5286-11e8-8ab0-3f731bc5c361',panelIndex:'9',type:visualization,version:'6.3.0'),(embeddableConfig:(vis:(legendOpen:!f)),gridData:(h:19,i:'10',w:19,x:29,y:97),id:'0d23deb0-5286-11e8-8ab0-3f731bc5c361',panelIndex:'10',type:visualization,version:'6.3.0'),(gridData:(h:7,i:'11',w:48,x:0,y:0),id:'783916b0-5287-11e8-8ab0-3f731bc5c361',panelIndex:'11',type:visualization,version:'6.3.0'),(embeddableConfig:(vis:(params:(sort:(columnIndex:0,direction:desc)))),gridData:(h:15,i:'12',w:48,x:0,y:44),id:'17ba7cb0-85e6-11e8-ba2e-69a0a3013ee4',panelIndex:'12',type:visualization,version:'6.3.0'),(embeddableConfig:(),gridData:(h:9,i:'13',w:48,x:0,y:7),id:'871fdd10-8407-11e8-ba2e-69a0a3013ee4',panelIndex:'13',type:visualization,version:'6.3.0')),query:(language:lucene,query:''),timeRestore:!t,title:social_media_analysis,viewMode:view)".format(keyword,keyword,keyword))

@app.route('/subscribe_alerts', methods=['POST'])
def subscribe_alerts():

    try:


        global keyword
        
        try:
            _input = request.get_json(force=True)
            print(_input)
            keyword = _input['keyword']
        except Exception as e:
            return handle_exceptions(message='Keyword not given, EXITING !!',response_code=421)
            
        global date_today
        global date_timestamp
        global path


        date_today = datetime.datetime.today().date()
        date_timestamp = str(datetime.datetime.today().timestamp()).split('.')[0]

        path = "data/" +keyword+'/'+str(date_today)


        if 'alert_type' in _input:
            alert_type = str(_input['alert_type']).lower()

            if 'media_type' in _input:
                media_type = str(_input['media_type']).lower()
            else:
                return handle_exceptions(message='Please mention Media Type (Blogs/News/Discussions/Twitter)',response_code=422)

            if 'alert_keyword' in _input:
                alert_keywords = str(_input['alert_keyword']).lower().split(',')
            else:
                return handle_exceptions(message='Please mention Keyword for Alerts',response_code=422)

            if 'recipient' in _input:
                recipient = str(_input['recipient']).lower()
            else:
                return handle_exceptions(message='Please mention recipient to send Email Alerts',response_code=423)

    except Exception as e:
        return handle_exceptions(message=e,response_code=424)


    try:
        final_df = pd.read_csv(path+'/'+keyword+"_all_social_media_"+str(date_today)+".csv")


        final_df['text'] = final_df['text'].apply(lambda x: ast.literal_eval(x))
        final_df['entities'] = final_df['entities'].apply(lambda x: ast.literal_eval(x))
        final_df['location'] = final_df['location'].apply(lambda x: ast.literal_eval(x))
        final_df['post_metrics'] = final_df['post_metrics'].apply(lambda x: ast.literal_eval(x))
        final_df['user'] = final_df['user'].apply(lambda x: ast.literal_eval(x))

    except Exception as e:
        print(e)
        return handle_exceptions(message="Keyword Not monitored. Add keyword to monitor first",response_code=425)


    alert_dict = get_subscribed_keyword_posts(alert_type,alert_keywords,final_df)

    for alert_keyword in alert_keywords:
        top_n = get_top_n(alert_dict[alert_keyword],n=5)
        try:
            html = get_html(alert_keyword, media_type ,alert_type,get_article(0, alert_keyword,media_type,top_n),get_article(1,alert_keyword,media_type,top_n))
            send_email(recipient=recipient,sender=alerts_sender,password=alerts_password,html=html,keyword=alert_keyword)

        except Exception as e:
            print(e)
            return handle_exceptions(message="Couldn't Send Alerts for {}, No Matching Article Found".format(alert_keyword),response_code=426)

    return jsonify(response="Successfully subscribed to alerts")


@app.errorhandler(Exception)
def default_exception_handle(error):
    response = jsonify(message=str(error))
    response.status_code = 420
    return response

def handle_exceptions(message,response_code):
    response = jsonify(response=message)
    response.status_code = response_code
    return response

if __name__ == '__main__':

    app.run(debug=True,port=9001,host="0.0.0.0")

