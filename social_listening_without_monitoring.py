
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


global date_today
global date_timestamp

api = twitter.Api(consumer_key=twitter_app_key,
                  consumer_secret=twitter_app_secret,
                  access_token_key=twitter_access_token,
                  access_token_secret=twitter_access_secret)


gmaps = googlemaps.Client(key=google_key)

app = Flask(__name__,
            static_url_path='', 
            static_folder='web/static',
            template_folder='web/templates')
CORS(app)

date_today = datetime.datetime.today().date()
date_timestamp = str(datetime.datetime.today().timestamp()).split('.')[0]


language_dict = dict(languages_countries_dict.languages)
countries_dict = dict(languages_countries_dict.countries)

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
    result['domain'] = output_thread['site'].lower()
    result['domain_full'] = output_thread['site_full'].lower()
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
    selected_info['user']['id'] = str(tweet['user']['id'])

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
                if ".json" in file:
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
#    try:
#        es.indices.delete(index=index_name_tmp, ignore=[400, 404])
#        print('index deleted')
#    except Exception as e:
#        print(e)
#        pass

    records = data

    actions = [{
        "_index": index_name_tmp,
        "_type": doc_type_tmp,
        "_id": 'data_'+str(i),
        "_source": j} for i,j in enumerate(records)]

    return helpers.bulk(es, actions=actions)


num_processes = 6
def pool_init():  
    gc.collect()

p = Pool(initializer=pool_init, processes = num_processes)



@app.route('/run_social_listening_without_monitoring', methods=['POST'])
def run_social_listening_without_monitoring():

    try:
        _input = request.get_json(force=True)
        print(_input)
        keyword = _input['keyword']
    except Exception as e:
        print(e)
        return handle_exceptions(message='Keyword not given, EXITING !!',response_code=421)


    global path

    path = "tmp/"+keyword+'/'+str(date_today)


    cleaned_results = Master_blogs_function(keyword)
    cleaned_twitter = Master_twitter_function(keyword)

    final_result = cleaned_results+cleaned_twitter
    final_df = pd.DataFrame(final_result)

    final_df.to_csv(path+'/'+keyword+"_all_social_media_"+str(date_today)+".csv",index=False,mode='a')
    print("uploading {} articles to kibana".format(int(final_df.shape[0])))

    try:
        Upload_to_kibana(final_result)
    except Exception as e:
        print(e)
        return handle_exceptions(message="Problem in Building Dashboard",response_code=430)

    return jsonify(response="Dashboard Built", url = "http://185.90.51.142:5601/app/kibana#/dashboard/da377110-85ed-11e8-90af-4bd679e81972?_g=(refreshInterval:(display:Off,pause:!f,value:0),time:(from:now-30d,mode:quick,to:now))&_a=(description:'',filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:social_listening_tmp_v1,key:keyword.keyword,negate:!f,params:(query:{},type:phrase),type:phrase,value:{}),query:(match:(keyword.keyword:(query:{},type:phrase))))),fullScreenMode:!f,options:(darkTheme:!f,hidePanelTitles:!f,useMargins:!t),panels:!((embeddableConfig:(),gridData:(h:19,i:'2',w:24,x:24,y:79),id:e683db90-85eb-11e8-90af-4bd679e81972,panelIndex:'2',type:visualization,version:'6.3.0'),(embeddableConfig:(),gridData:(h:9,i:'3',w:48,x:0,y:7),id:add29cf0-85eb-11e8-90af-4bd679e81972,panelIndex:'3',type:visualization,version:'6.3.0'),(embeddableConfig:(vis:(legendOpen:!f)),gridData:(h:19,i:'4',w:24,x:0,y:79),id:'91dd0530-85eb-11e8-90af-4bd679e81972',panelIndex:'4',type:visualization,version:'6.3.0'),(embeddableConfig:(vis:(legendOpen:!t)),gridData:(h:15,i:'5',w:17,x:31,y:24),id:'3fd5f510-85ed-11e8-90af-4bd679e81972',panelIndex:'5',type:visualization,version:'6.3.0'),(embeddableConfig:(),gridData:(h:8,i:'6',w:48,x:0,y:16),id:'3fb03a10-85ec-11e8-90af-4bd679e81972',panelIndex:'6',type:visualization,version:'6.3.0'),(embeddableConfig:(),gridData:(h:21,i:'7',w:24,x:24,y:98),id:'006ba1f0-85ec-11e8-90af-4bd679e81972',panelIndex:'7',type:visualization,version:'6.3.0'),(embeddableConfig:(),gridData:(h:15,i:'8',w:48,x:0,y:119),id:'1b9d9b90-85ec-11e8-90af-4bd679e81972',panelIndex:'8',type:visualization,version:'6.3.0'),(embeddableConfig:(vis:(legendOpen:!f)),gridData:(h:15,i:'9',w:31,x:0,y:24),id:cb9d1bc0-85eb-11e8-90af-4bd679e81972,panelIndex:'9',type:visualization,version:'6.3.0'),(embeddableConfig:(mapCenter:!(38.82259097617713,14.062500000000002),mapZoom:2),gridData:(h:22,i:'10',w:48,x:0,y:57),id:cb69c3d0-85f8-11e8-90af-4bd679e81972,panelIndex:'10',type:visualization,version:'6.3.0'),(embeddableConfig:(vis:(legendOpen:!f)),gridData:(h:21,i:'11',w:24,x:0,y:98),id:cc121710-845e-11e8-ba2e-69a0a3013ee4,panelIndex:'11',type:visualization,version:'6.3.0'),(embeddableConfig:(),gridData:(h:18,i:'12',w:48,x:0,y:39),id:b4c6e490-89f0-11e8-90af-4bd679e81972,panelIndex:'12',type:visualization,version:'6.3.0'),(embeddableConfig:(),gridData:(h:7,i:'13',w:48,x:0,y:0),id:'783916b0-5287-11e8-8ab0-3f731bc5c361',panelIndex:'13',type:visualization,version:'6.3.0')),query:(language:lucene,query:''),timeRestore:!t,title:social_media_tmp,viewMode:view)".format(keyword,keyword,keyword))


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
    app.run(debug=True,port=9000,host="0.0.0.0")

