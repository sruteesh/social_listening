from social_listening_functions import *

import logging


logging.basicConfig(filename="./logs/"+str(datetime.date.today()) + '_daily.log',
                    format='%(asctime)-20s - %(name)-10s : %(levelname)-8s: %(message)s',
                    datefmt='%m/%d/%Y %H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


app = Flask(__name__,
            static_url_path='', 
            static_folder='web/static',
            template_folder='web/templates')
CORS(app)

num_processes = 6
def pool_init():  
    gc.collect()

p = Pool(initializer=pool_init, processes = num_processes)




def get_latest_crawl_parameter(keyword,media='blogs'):
    try:
        if not os.path.exists(path):
            logger.warning('No keyword folder found!!')
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
        logger.warning("Couldn't get latest crawl parameter!! {}".format(e))
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
                        logger.debug("Getting {} {}".format(count,len(output['posts'])))
                        count+=1
                        for post in output['posts']:
                            try:
                                json.dump(post,fin)
                                fin.write('\n')
                                results.append(post)
                            except Exception as e:
                                logger.warning("DUMPING TO FILE FAILED!! {} ".format(e))
                        latest_crawl_since = datetime.datetime.timestamp(datetime.datetime.strptime(results[-1]['crawled'].split('.')[0],'%Y-%m-%dT%H:%M:%S'))
                    else:
                        break
            except Exception as e:
                logger.warning("GETTING BLOGS FAILED!! {} ".format(e))
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
                logger.debug('getting {}'.format((i+1)*100))
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
                        logger.debug('2 getting {}'.format((i+1)*100))
                        results = api.GetSearch(keyword,count=100,result_type='recent',return_json=True,since_id=results['statuses'][0]['id'])
                        if len(results['statuses'])>0:
                            json.dump(results['statuses'],fin)
                            master_results.extend(results['statuses'])
                            fin.write('\n')
                        else:
                            break
                    except Exception as e:
                        if 'out of range' in str(e).lower():
                            return master_results
                        else:
                            logger.warning("GETTING TWEETS FAILED!! RETRYING {} ".format(e))
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
    logger.info("PROCESSING BLOGS....")
    for i,result in enumerate(blogs_multiprocess_pool):
        try:
            output = result.get()
            output['keyword'] = keyword
            cleaned_results.append(output)
        except Exception as e:
            logger.warning("BLOGS EXCEPTION!! {}".format(e))
            logger.warning('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
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


    logger.info("PROCESSING TWEETS....")
    tweets_multiprocess_pool = [p.apply_async(get_tweet_info, [i]) for i in tweets]

    for i,result in enumerate(tweets_multiprocess_pool):
        try:
            output = result.get()
            output['keyword'] = keyword
            cleaned_twitter.append(output)
        except Exception as e:
            logger.warning("TWITTER EXCEPTION!! {} ".format(e))
            logger.warning('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
            pass

    return cleaned_twitter




def Upload_to_kibana(data):

    es = Elasticsearch(host)

    # try :
    #    es.indices.delete(index=index_name, ignore=[400, 404])
    #    print('index deleted')
    # except Exception as e:
    #    logger.warning(e)
    #    pass

    records = data

    actions = [{
        "_index": index_name,
        "_type": doc_type,
        "_id": keyword+'_'+str(date_today)+'_'+str(i),
        "_source": j} for i,j in enumerate(records)]

    return helpers.bulk(es, actions=actions)


# num_processes = 6
# def pool_init():  
#     gc.collect()

# p = Pool(initializer=pool_init, processes = num_processes)


@app.route('/run_social_listening', methods=['POST'])
def run_social_listening():

    global keyword

    try:
        _input = request.get_json(force=True)
        logger.debug("INPUT {}".format(_input))
        keyword = _input['keyword']
    except Exception as e:
        logger.warning("NO KEYWORD GIVEN !! {}".format(e))
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


    logger.info("UPLOADING {} ARTICLES TO KIBANA".format(int(final_df.shape[0])))

    try:
        Upload_to_kibana(final_result)
    except Exception as e:
        logger.error("UPLOAD TO KIBANA FAILED!! {}".format(e))
        return handle_exceptions(message="Problem in Building Dashboard",response_code=430)

    keywords_df = pd.DataFrame()
    keywords_df['keyword'] = keyword
    keywords_df['added_at'] = str(datetime.datetime.today())
    keywords_df.to_csv("./data/keywords_to_monitor.csv",index=False)

    return jsonify(response="Dashboard Built", url = "http://185.90.51.142:5601/app/kibana#/dashboard/0ac89420-5287-11e8-8ab0-3f731bc5c361?_g=(refreshInterval:(display:Off,pause:!f,value:0),time:(from:now-30d,mode:quick,to:now))&_a=(description:'',filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'663c8a20-8115-11e8-ba2e-69a0a3013ee4',key:keyword.keyword,negate:!f,params:(query:{},type:phrase),type:phrase,value:{}),query:(match:(keyword.keyword:(query:{},type:phrase))))),fullScreenMode:!f,options:(darkTheme:!f,hidePanelTitles:!f,useMargins:!t),panels:!((gridData:(h:20,i:'1',w:48,x:0,y:24),id:'07b340d0-5266-11e8-bada-23eb8c6d65ff',panelIndex:'1',type:visualization,version:'6.3.0'),(gridData:(h:8,i:'2',w:48,x:0,y:16),id:c2c82ac0-5266-11e8-bada-23eb8c6d65ff,panelIndex:'2',type:visualization,version:'6.3.0'),(gridData:(h:23,i:'5',w:29,x:0,y:74),id:e191b1f0-5285-11e8-8ab0-3f731bc5c361,panelIndex:'5',type:visualization,version:'6.3.0'),(embeddableConfig:(vis:(legendOpen:!f)),gridData:(h:15,i:'6',w:32,x:0,y:59),id:'35836920-5286-11e8-8ab0-3f731bc5c361',panelIndex:'6',type:visualization,version:'6.3.0'),(gridData:(h:19,i:'7',w:29,x:0,y:97),id:b9233360-5285-11e8-8ab0-3f731bc5c361,panelIndex:'7',type:visualization,version:'6.3.0'),(gridData:(h:15,i:'8',w:16,x:32,y:59),id:'74e71770-5285-11e8-8ab0-3f731bc5c361',panelIndex:'8',type:visualization,version:'6.3.0'),(embeddableConfig:(vis:(legendOpen:!f)),gridData:(h:23,i:'9',w:19,x:29,y:74),id:'628886d0-5286-11e8-8ab0-3f731bc5c361',panelIndex:'9',type:visualization,version:'6.3.0'),(embeddableConfig:(vis:(legendOpen:!f)),gridData:(h:19,i:'10',w:19,x:29,y:97),id:'0d23deb0-5286-11e8-8ab0-3f731bc5c361',panelIndex:'10',type:visualization,version:'6.3.0'),(gridData:(h:7,i:'11',w:48,x:0,y:0),id:'783916b0-5287-11e8-8ab0-3f731bc5c361',panelIndex:'11',type:visualization,version:'6.3.0'),(embeddableConfig:(vis:(params:(sort:(columnIndex:0,direction:desc)))),gridData:(h:15,i:'12',w:48,x:0,y:44),id:'17ba7cb0-85e6-11e8-ba2e-69a0a3013ee4',panelIndex:'12',type:visualization,version:'6.3.0'),(embeddableConfig:(),gridData:(h:9,i:'13',w:48,x:0,y:7),id:'871fdd10-8407-11e8-ba2e-69a0a3013ee4',panelIndex:'13',type:visualization,version:'6.3.0')),query:(language:lucene,query:''),timeRestore:!t,title:social_media_analysis,viewMode:view)".format(keyword,keyword,keyword))

@app.route('/subscribe_alerts', methods=['POST'])
def subscribe_alerts():

    try:
        global keyword
        
        try:
            _input = request.get_json(force=True)
            logger.info("INPUT {}".format(_input))
            keyword = _input['keyword']
        except Exception as e:
            logger.warning("NO KEYWORD GIVEN !! {}".format(e))
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
        logger.warning("KEYWORD NOT MONITORED !! {}".format(e))
        return handle_exceptions(message="Keyword Not monitored. Add keyword to monitor first",response_code=425)


    alert_dict = get_subscribed_keyword_posts(alert_type,alert_keywords,final_df)

    for alert_keyword in alert_keywords:
        top_n = get_top_n(alert_dict[alert_keyword],n=5)
        try:
            html = get_html(alert_keyword, media_type ,alert_type,get_article(0, alert_keyword,media_type,top_n),get_article(1,alert_keyword,media_type,top_n))
            send_email(recipient=recipient,sender=alerts_sender,password=alerts_password,html=html,keyword=alert_keyword)

        except Exception as e:
            logger.warning("SENDING EMAIL FAILED !!".format(e))
            return handle_exceptions(message="Couldn't Send Alerts for {}, No Matching Article Found".format(alert_keyword),response_code=426)

    return jsonify(response="Successfully subscribed to alerts")

@app.after_request
def after(response):
  # todo with response
  logger.info("{} {} {}".format(response.status,response.headers,response.get_data()))
  return response

@app.before_request
def before():
    # todo with request
    logger.info('===============================================================================')
    logger.info(request.host)

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

