import time,pymysql,scrapy

class Zghy_Spider(scrapy.Spider):
    name = 'hy_zghy'
    allowed_domains = ['zghy.org.cn']
    headers={'referer':'https://www.zghy.org.cn/ch/tech',
             'accept':'*/*',
             'x-requested-with':'XMLHttpRequest'}
    page = 0
    #更新设置--------------------------------------------------
    # start_time = time.time()
    # up_time = 600  # 更新 秒
    custom_settings = {
                       #设置爬取算法模式
                       # 'SCHEDULER_DISK_QUEUE' : 'scrapy.squeues.PickleFifoDiskQueue',
                       # 'SCHEDULER_MEMORY_QUEUE' : 'scrapy.squeues.FifoMemoryQueue',
                       # 'DEPTH_PRIORITY': 1, #0表示深度优先,1表示广度优先
                       # 'DEPTH_LIMIT' : 5    #最大深度值
                        'CONCURRENT_REQUESTS':32,
                        'DOWNLOAD_DELAY':0.5,
                        'CONCURRENT_REQUESTS_PER_DOMAIN' :32
                       #'LOG_LEVEL': 'DEBUG'
                        }
    #-默认入库,入FTP,入分类设置,更新时长---------------------------
    table_name = 'hy_cn_tech'   #mysql表名
    default_category='tech'          #默认分类
    #--------------------------------------------------------
    def __init__(self):
        super(Zghy_Spider, self).__init__(name='hy_zghy')
        # mysql------------------------------------------
        self.conn = pymysql.Connect(
            host='154.212.112.247',
            port=13006,
            # 数据库名：
            db='seo-hy',
            user="root",
            passwd='itfkgsbxf3nyw6s1',
            charset='utf8')
        self.cur = self.conn.cursor()
    def start_requests(self):
        for num in range(500,1011):#1011
            yield scrapy.Request(url='https://www.zghy.org.cn/load/news?page={}&type=0&sign=tech&last_id='.format(num),callback=self.parse,headers=self.headers)
            print('----------------------------开始获取第{}页----------------------------------'.format(num))

    def parse(self,response):
        if response.status <400:
            for date in response.json()['list']:
                yield scrapy.Request('https://www.zghy.org.cn/item/{}'.format(date['id']),headers=self.headers,callback=self.parse_item)
        else:
            print('error:请求失败:{} {}'.format(response.status,response.url))
    def parse_item(self, response):
        # 后续更新:启动10分钟后关闭
        # if time.time() - self.start_time >= self.up_time:
        #     self.crawler.engine.close_spider(self, "更新10分钟完成!!")
        item = {}
        url=response.url
        item['url'] = url
        # 标题
        try:
            item['title'] = response.xpath("//h1[@class='article-title']/text()").extract_first()
            if item['title'] == None:
                item['title'] = ''
        except Exception:
            item['title'] = ''
        # 关键字
        try:
            item['keyword'] = response.xpath("//meta[@name='keywords']/@content").extract_first().replace('中国科技新闻网','')
            if item['keyword'] ==None or len(item['keyword'])<1:
                item['keyword'] = item['title'].replace(' ',',')
        except Exception:
            item['keyword'] = item['title'].replace(' ',',')
        # 摘要
        try:
            item['description'] = response.xpath("//meta[@name='description']/@content").extract_first()
            if item['description'] == None or item['description']=='中国科技新闻网为中国科技新闻学会主办的网站，我们将聚合全国全官媒新闻与信息，把中国科技新闻网打造成国家级权威新闻传播、存储、交流的云平台，并实现全网络新闻资讯、客户资源共享，进而达到信息快速、联动、广泛、高效的传播与推广。':
                item['description'] = item['title']
        except Exception:
            item['description'] = item['title']
        # # 图片
        try:
            img_url=response.xpath("//div[@class='article-content']//img/@src").extract_first()
            if 'brand' in img_url or img_url == None:
                item['img_src'] = ''
            else:item['img_src']=img_url
        except Exception:
            item['img_src'] = ''
        # 正文
        try:
            item['content'] ='\n'.join(response.xpath("//div[@class='article_main_content']//p/text()").extract()).replace('科技日报记者 ','')
        except Exception:
            item['content'] = ''
        # 作者
        item['author'] = '马铃薯科技'
        # 发行时间
        try:
            item['release_time'] = response.xpath("//div[@class='article-sub']/span/text()").extract_first().strip()
        except Exception:
            item['release_time'] = time.strftime('%Y-%m-%d %H:%M:%S')
        # 分类
        item['category'] = 'tech'
        # 来源
        item['be_from'] = 'www.zghy.org.cn'

        #生成器返回:
        if len(item['title']) >=1 and len(item['content']) >= 50 and item['category'] != '':
            #-----------入库去重--------------------------------------
            find_ex = "select id,url from {} where title= %s ".format(self.table_name)
            self.cur.execute(find_ex, (item["title"]))
            if not self.cur.fetchone():
                news = "insert into {}(title,content,author,release_time,keyword,description,url,img_src,category,create_time,be_from) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(self.table_name)
                self.cur.execute(news, (item['title'], item['content'], item['author'], item['release_time'], item['keyword'],item['description'], item['url'],item['img_src'], item['category'], time.time(),item['be_from']))
                self.conn.commit()
                self.page += 1
                print(time.strftime('%Y.%m.%d-%H:%M:%S'), '第', self.page, '条抓取成功:', item['url'])
            else:print('**数据重复:',item['url'])
        else:
            print('##数据不匹配:标题长度:',len(item['title']),'文本长度:',len(item['content']),'category:',item['category'],response.url)


    def close(self, reason):
        print(reason,'共抓取:',self.page,'条数据')
        self.conn.close()
        ##self.crawler.engine.close_spider(self, "关闭spider")
        #scrapy crawl hy_zghy
