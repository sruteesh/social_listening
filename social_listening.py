
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

master_location_coords=defaultdict(list)
def get_location_coords(location):
    if location in master_location_coords:
        return master_location_coords[location]
    elif location is not None:
        try:
            geocode_result = gmaps.geocode(location)
            _dict = geocode_result[0]['geometry']['location']
            master_location_coords[location] = (_dict['lat'],_dict['lng'])
            return (_dict['lat'],_dict['lng'])
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
    selected_info['id'] = tweet['id']
    selected_info['source_category'] = 'twitter'
    selected_info['domain'] = [tweet['entities']['urls'][0]['display_url'].split('/')[0].lower() if len(tweet['entities']['urls'])>0 else None][0]
    selected_info['domain_full'] = selected_info['domain']
    selected_info['source_url'] = "https://twitter.com/i/web/status/"+tweet['id_str']
    selected_info['title'] = None
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
        
        
    with open(path+'/'+keyword+"_twitter_"+str(date_today)+'_'+str(date_timestamp)+".json",'w') as fin:
        results = api.GetSearch(keyword,count=100,result_type='recent',return_json=True,since_id=latest_crawl_since)
        if len(results['statuses'])>0:
            json.dump(results['statuses'],fin)
            fin.write('\n')
        for i in range(4):
            try:
                print('getting ', (i+1)*100)
                results = api.GetSearch(keyword,count=100,result_type='recent',return_json=True,since_id=results['statuses'][0]['id'])
                if len(results['statuses'])>0:
                    json.dump(results['statuses'],fin)
                    fin.write('\n')
            except Exception as e:
                print(e)
                pass
    return results


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
    for result in blogs_multiprocess_pool:
        cleaned_results.append(result.get())

    return cleaned_results



# t2 = time.time()
# cleaned_results=[]
# for i in blogs_news:
#     cleaned_results.append(get_post_info(i))
# time.time()-t2



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
    blogs_multiprocess_pool = [p.apply_async(get_tweet_info, [i]) for i in tweets]
    for result in blogs_multiprocess_pool:
        cleaned_twitter.append(result.get())

    return cleaned_twitter


# cleaned_twitter=[]
# for twitter_list in twitter:
#     try:
#         for tweet in twitter_list:
#             cleaned_twitter.append(get_tweet_info(tweet))
#     except Exception as e:
#         print(e)
#         print(twitter_list)



def Upload_to_kibana(data):

    es = Elasticsearch(host)
    # try :
    #     es.indices.delete(index=index_name, ignore=[400, 404])
    #     print('index deleted')
    # except Exception as e:
    #     print(e)
    #     pass

    # master_agro_df2['upload_time'] = datetime.datetime.now().isoformat()
    # master_agro_df2['post_id'] = list(range(len(final_result)))
    # records=master_agro_df2.where(pd.notnull(master_agro_df2), None).T.to_dict()
    # list_records=[records[it] for it in records]

    records = data

    actions = [{
        "_index": index_name,
        "_type": doc_type,
        "_id": 'data_'+str(date_today)+'_'+str(i),
        "_source": j} for i,j in enumerate(records)]

    return helpers.bulk(es, actions=actions)


num_processes = 6
def pool_init():  
    gc.collect()

p = Pool(initializer=pool_init, processes = num_processes)




for i in master_location_coords:
    val = master_location_coords[i]
    master_location_coords[i] = (val[1],val[0])



@app.route('/run_social_listening', methods=['POST'])
def run_social_listening():

    try:
        _input = request.get_json(force=True)
        print(_input)
        keyword = _input['keyword']
    except Exception as e:
        print(e)
        print('Keyword not given, EXITING !!')
        sys.exit()

    global date_today
    global date_timestamp
    global path

    date_today = datetime.datetime.today().date()
    date_timestamp = str(datetime.datetime.today().timestamp()).split('.')[0]

    path = "data/" +keyword+'/'+str(date_today)

    cleaned_results = Master_blogs_function(keyword)
    cleaned_twitter = Master_twitter_function(keyword)

    final_result = cleaned_results+cleaned_twitter
    final_df = pd.DataFrame(final_result)

    final_df.to_csv(path+'/'+keyword+"_all_social_media_"+str(date_today)+".csv",index=False,mode='a')

    print("uploading {} articles to kibana".format(int(final_df.shape[0])))
    
    try:
        Upload_to_kibana(final_result)
    except Exception as e:
        return jsonify(response=e)

    return jsonify(response="Dashboard Built")


@app.route('/subscribe_alerts', methods=['POST'])
def subscribe_alerts():

    global date_today
    global date_timestamp
    global path

    date_today = datetime.datetime.today().date()
    date_timestamp = str(datetime.datetime.today().timestamp()).split('.')[0]

    path = "data/" +keyword+'/'+str(date_today)

    try:
        _input = request.get_json(force=True)
        print(_input)

        keyword = _input['keyword']

        if 'alert_type' in _input:
            alert_type = str(_input['alert_type']).lower()

            if 'media_type' in _input:
                media_type = str(_input['media_type']).lower()
            else:
                print("Please mention Media Type (Blogs/News/Discussions/Twitter)")
                return jsonify(response="Please mention Media Type (Blogs/News/Discussions/Twitter)")
                sys.exit()

            if 'alert_keyword' in _input:
                alert_keywords = str(_input['alert_keyword']).lower().split(',')
            else:
                print("Please mention Keyword for Alerts")
                return jsonify(response="Please mention Keyword for Alerts")
                sys.exit()

            if 'recipient' in _input:
                recipient = str(_input['recipient']).lower()
            else:
                print("Please mention recipient to send Email Alerts")
                return jsonify(response="Please mention recipient to send Email Alerts")
                sys.exit()

    except Exception as e:
        print(e)


    final_df = pd.read_csv(path+'/'+keyword+"_all_social_media_"+str(date_today)+".csv")


    final_df['text'] = final_df['text'].apply(lambda x: ast.literal_eval(x))
    final_df['entities'] = final_df['entities'].apply(lambda x: ast.literal_eval(x))
    final_df['location'] = final_df['location'].apply(lambda x: ast.literal_eval(x))
    final_df['post_metrics'] = final_df['post_metrics'].apply(lambda x: ast.literal_eval(x))
    final_df['user'] = final_df['user'].apply(lambda x: ast.literal_eval(x))


    alert_dict = get_subscribed_keyword_posts(alert_type,alert_keywords,final_df)

    for alert_keyword in alert_keywords:
        top_n = get_top_n(alert_dict[alert_keyword],n=5)
        try:
            html = get_html(alert_keyword, media_type ,alert_type,get_article(0, alert_keyword,media_type,top_n),get_article(1,alert_keyword,media_type,top_n))
            send_email(recipient=recipient,sender=alerts_sender,password=alerts_password,html=html,keyword=alert_keyword)

        except Exception as e:
            print(e)
            return jsonify(response="Couldn't Send Alerts for {}, No Matching Article Found".format(alert_keyword))

    return jsonify(response="Successfully subscribed to alerts")




if __name__ == '__main__':
    app.run(debug=True,port=9001,host="0.0.0.0")

