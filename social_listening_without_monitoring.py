
# coding: utf-8
from social_listening_functions import *
from multiprocessing import Pool
from functools import partial
import gc,ast,operator


import logging

logging.basicConfig(filename="./logs/"+str(datetime.date.today()) + '_no_monitor_daily.log',
                    format='%(asctime)-20s - %(name)-10s : %(levelname)-8s: %(message)s',
                    datefmt='%m/%d/%Y %H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)



app = Flask(__name__,
            static_url_path='', 
            static_folder='web/static',
            template_folder='web/templates')
CORS(app)

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


def get_latest_crawl_parameter(keyword,media='blogs'):
    try:
        if not os.path.exists(path):
            print('No keyword folder found!!')
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
        print("Couldn't get latest crawl parameter!! {}".format(e))
        return None





def get_blogs_news(keyword,streaming=True):

    try:
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
                            print("Getting {} {}".format(count,len(output['posts'])))
                            count+=1
                            for post in output['posts']:
                                try:
                                    json.dump(post,fin)
                                    fin.write('\n')
                                    results.append(post)
                                except Exception as e:
                                    print("DUMPING TO FILE FAILED!! {} ".format(e))
                            latest_crawl_since = datetime.datetime.timestamp(datetime.datetime.strptime(results[-1]['crawled'].split('.')[0],'%Y-%m-%dT%H:%M:%S'))
                        else:
                            break
                except Exception as e:
                    print("GETTING BLOGS FAILED!! {} ".format(e))
                    break
        return results
    except Exception as e:
        print("GET BLOGS EXCEPTION !!",e)
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
        return


def get_twitter(keyword,streaming=True):

    try:

#         if not os.path.exists(path):
#             os.makedirs(path)

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
                    print('getting {}'.format((i+1)*100))
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
                            print('2 getting {}'.format((i+1)*100))
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
                                print("GETTING TWEETS FAILED!! RETRYING {} ".format(e))
                                pass
        return master_results
    except Exception as e:
        print("GET TWITTER EXCEPTION !!",e)
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
        return


def get_articles(keyword,source,page_limit=4):
    service = getService()
    startIndex = 1
    response = []
    q = '"'+keyword+'"'
    
    if source=='google':
        site = ''
    else:
        site = source+".com"
    
    with open(path+'/'+keyword+"_"+source+'_'+str(date_today)+".json",'w') as fin:

        for nPage in range(0, page_limit):
            try:
                print ("Reading page number {} for {}:".format(nPage+1,source))

                result = service.cse().list(
                    q=q, #Search words
                    cx='001132580745589424302:jbscnf14_dw',  #CSE Key
                    lr='lang_en', #Search language
                    siteSearch=site,
                    start=startIndex,
                    sort = 'date'
                ).execute()
                
                startIndex = result.get("queries").get("nextPage")[0].get("startIndex")
                json.dump(result['items'],fin)
                fin.write('\n')
                
            except Exception as e:
                print(e)
                return
    return

def Master_blogs_function(keyword):

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
    print("PROCESSING BLOGS....")
    for i,result in enumerate(blogs_multiprocess_pool):
        try:
            output = result.get()
            output['keyword'] = keyword
            cleaned_results.append(output)
        except Exception as e:
            print("BLOGS EXCEPTION!! {}".format(e))
            print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
            pass

    return cleaned_results


def Master_twitter_function(keyword):
#### Twitter

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


    print("PROCESSING TWEETS....")
    tweets_multiprocess_pool = [p.apply_async(get_tweet_info, [i]) for i in tweets]

    for i,result in enumerate(tweets_multiprocess_pool):
        try:
            output = result.get()
            output['keyword'] = keyword
            cleaned_twitter.append(output)
        except Exception as e:
            print("TWITTER EXCEPTION!! {} ".format(e))
            print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
            pass

    return cleaned_twitter


def Master_google_function(keyword,source):


    goog_yout_pint=[]
    with open(path+'/'+keyword+"_"+source+'_'+str(date_today)+".json") as fin:
        for line in fin:
            goog_yout_pint.extend(json.loads(line))



    # Process Google, Youtube, Pinterest
    t1 = time.time()
    cleaned_results = []
    goog_yout_pint_multiprocess_pool = [p.apply_async(get_articles_info, [(i,source)]) for i in goog_yout_pint]
    
    
    print("PROCESSING {}....".format(source))
    
    for i,result in enumerate(goog_yout_pint_multiprocess_pool):
        try:
            output = result.get()
            output['keyword'] = keyword
            cleaned_results.append(output)
        except Exception as e:
            print("{} EXCEPTION!! {}".format(source, e))
            print('MAIN ERROR on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
            pass

    return cleaned_results


def get_social_search_results(keyword_tuple):
    
    global path
    global date_today
    global date_timestamp

    keyword,source = keyword_tuple[0]
    path = keyword_tuple[1]
    date_today = keyword_tuple[2]
    date_timestamp = keyword_tuple[3]

    if source=='blogs':
        tmp_articles = get_blogs_news(keyword)
    elif source=='twitter':
        tmp_articles = get_twitter(keyword)
    else:
        tmp_articles = get_articles(keyword,source)
        
    return

def search_social_media(search_inputs):

    social_media_multiprocess_pool = [p.apply_async(get_social_search_results, [(i,path,date_today,date_timestamp)]) for i in search_inputs]
    for i,result in enumerate(social_media_multiprocess_pool):
        try:
            output = result.get()
        except Exception as e:
            print("SEARCH EXCEPTION!! {}".format(e))
            pass
    return None


num_processes = 6
def pool_init():  
    gc.collect()

p = Pool(initializer=pool_init, processes = num_processes)



@app.route('/run_social_listening_without_monitoring', methods=['POST'])
def run_social_listening_without_monitoring():


    global keyword

    try:
        _input = request.get_json(force=True)
        logger.debug(_input)
        keyword = _input['keyword']
    except Exception as e:
        logger.warning(e)
        return handle_exceptions(message='Keyword not given, EXITING !!',response_code=421)


    global path
    global date_today
    global date_timestamp


    date_today = datetime.datetime.today().date()
    date_timestamp = str(datetime.datetime.today().timestamp()).split('.')[0]

    path = "tmp/"+keyword+'/'+str(date_today)

    search_inputs = [(keyword,'blogs'),(keyword,'twitter'),(keyword,'youtube'),(keyword,'pinterest'),(keyword,'reddit'),(keyword,'google')]

    search_social_media(search_inputs)
    # for i in search_inputs:
    #     get_social_search_results(i)

    cleaned_blogs = Master_blogs_function(keyword)
    cleaned_twitter = Master_twitter_function(keyword)
    cleaned_pinterest = Master_google_function(keyword,'pinterest')
    cleaned_youtube = Master_google_function(keyword,'youtube')
    cleaned_reddit = Master_google_function(keyword,'reddit')
    cleaned_google = Master_google_function(keyword,'google')

    final_result = cleaned_blogs+cleaned_twitter+cleaned_pinterest+cleaned_youtube+cleaned_reddit+cleaned_google
    final_df = pd.DataFrame(final_result)

    final_df.to_csv(path+'/'+keyword+"_all_social_media_"+str(date_today)+".csv",index=False,mode='a')
    print("uploading {} articles to kibana".format(int(final_df.shape[0])))

    try:
        Upload_to_kibana(final_result)
    except Exception as e:
        print(e)
        return handle_exceptions(message="Problem in Building Dashboard",response_code=430)

    dashboard_url = """
<iframe src="http://185.90.51.142:5601/app/kibana#/dashboard/da377110-85ed-11e8-90af-4bd679e81972?embed=true&_g=(refreshInterval:(display:Off,pause:!f,value:0),time:(from:now-30d,mode:quick,to:now))&_a=(description:'',filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:social_listening_tmp_v1,key:keyword.keyword,negate:!f,params:(query:{},type:phrase),type:phrase,value:{}),query:(match:(keyword.keyword:(query:{},type:phrase))))),fullScreenMode:!t,options:(darkTheme:!f,hidePanelTitles:!f,useMargins:!t),panels:!((embeddableConfig:(),gridData:(h:19,i:'2',w:24,x:24,y:79),id:e683db90-85eb-11e8-90af-4bd679e81972,panelIndex:'2',type:visualization,version:'6.3.0'),(embeddableConfig:(),gridData:(h:9,i:'3',w:48,x:0,y:7),id:add29cf0-85eb-11e8-90af-4bd679e81972,panelIndex:'3',type:visualization,version:'6.3.0'),(embeddableConfig:(vis:(legendOpen:!f)),gridData:(h:19,i:'4',w:24,x:0,y:79),id:'91dd0530-85eb-11e8-90af-4bd679e81972',panelIndex:'4',type:visualization,version:'6.3.0'),(embeddableConfig:(vis:(legendOpen:!t)),gridData:(h:15,i:'5',w:17,x:31,y:24),id:'3fd5f510-85ed-11e8-90af-4bd679e81972',panelIndex:'5',type:visualization,version:'6.3.0'),(embeddableConfig:(),gridData:(h:8,i:'6',w:48,x:0,y:16),id:'3fb03a10-85ec-11e8-90af-4bd679e81972',panelIndex:'6',type:visualization,version:'6.3.0'),(embeddableConfig:(),gridData:(h:21,i:'7',w:24,x:24,y:98),id:'006ba1f0-85ec-11e8-90af-4bd679e81972',panelIndex:'7',type:visualization,version:'6.3.0'),(embeddableConfig:(),gridData:(h:15,i:'8',w:48,x:0,y:119),id:'1b9d9b90-85ec-11e8-90af-4bd679e81972',panelIndex:'8',type:visualization,version:'6.3.0'),(embeddableConfig:(vis:(legendOpen:!f)),gridData:(h:15,i:'9',w:31,x:0,y:24),id:cb9d1bc0-85eb-11e8-90af-4bd679e81972,panelIndex:'9',type:visualization,version:'6.3.0'),(embeddableConfig:(mapCenter:!(38.82259097617713,14.062500000000002),mapZoom:2),gridData:(h:22,i:'10',w:48,x:0,y:57),id:cb69c3d0-85f8-11e8-90af-4bd679e81972,panelIndex:'10',type:visualization,version:'6.3.0'),(embeddableConfig:(vis:(legendOpen:!f)),gridData:(h:21,i:'11',w:24,x:0,y:98),id:cc121710-845e-11e8-ba2e-69a0a3013ee4,panelIndex:'11',type:visualization,version:'6.3.0'),(embeddableConfig:(),gridData:(h:18,i:'12',w:48,x:0,y:39),id:b4c6e490-89f0-11e8-90af-4bd679e81972,panelIndex:'12',type:visualization,version:'6.3.0'),(embeddableConfig:(),gridData:(h:7,i:'13',w:48,x:0,y:0),id:'783916b0-5287-11e8-8ab0-3f731bc5c361',panelIndex:'13',type:visualization,version:'6.3.0')),query:(language:lucene,query:''),timeRestore:!t,title:social_media_tmp,viewMode:view)"></iframe>
""".format(keyword,keyword,keyword)
    
    lines = [
'<!DOCTYPE html>',
'<html>',
'<style type="text/css">',
'html, body { margin: 0; padding 0; width: 100%; height: 100%;}',
'iframe { border: 0; width: 100%; height: 99%; }'
'</style>',
'<body>',
dashboard_url,
'</body>',
'</html>']
    
    with open("/home/kritonis/public_html/social_listening_dashboard/dashboard/dashboard.html",'w') as fin:
        for line in lines:
            fin.write(line+'\n')
    
    return jsonify(response="Dashboard Built", url = "http://kritonis.com/social_listening_dashboard/dashboard/dashboard.html")


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
    app.run(debug=True,port=9000,host="0.0.0.0")

