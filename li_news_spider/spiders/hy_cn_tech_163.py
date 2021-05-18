from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
import time,pymysql

class Wy_Spider(CrawlSpider):
    name = 'hy_163'
    allowed_domains = ['163.com']
    start_urls = ['https://tech.163.com/']
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
                       #  'CONCURRENT_REQUESTS':32,
                       #  'DOWNLOAD_DELAY':1,
                       #  'CONCURRENT_REQUESTS_PER_DOMAIN' :32,
                       #'LOG_LEVEL':'DEBUG'
                        }
    #-默认入库,入FTP,入分类设置,更新时长---------------------------
    table_name = 'hy_cn_tech'   #mysql表名
    default_category='tech'          #默认分类
    #--------------------------------------------------------
    rules = (Rule(LinkExtractor(allow=r'^https://www.163.com/(?!keywords)(?!photoview).*html$'), callback='parse_item', follow=False),
             Rule(LinkExtractor(allow=r'https://tech.163.com/(?!keywords).*'),follow=True),)

    def __init__(self):
        super(Wy_Spider, self).__init__(name='hy_163')
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

    def parse_item(self, response):
        # 后续更新:启动10分钟后关闭
        # if time.time() - self.start_time >= self.up_time:
        #     self.crawler.engine.close_spider(self, "更新10分钟完成!!")
        item = {}
        url=response.url
        item['url'] = url
        # 标题
        try:
            item['title'] = response.xpath("//meta[@property='og:title']/@content").extract_first()
            if item['title'] == None:
                item['title'] = ''
        except Exception:
            item['title'] = ''
        # 关键字
        try:
            item['keyword'] = response.xpath("//meta[@name='keywords']/@content").extract_first()
            if item['keyword'] ==None or len(item['keyword'])<1:
                item['keyword'] = item['title'].replace(' ',',')
        except Exception:
            item['keyword'] = item['title'].replace(' ',',')
        # 摘要
        try:
            item['description'] = response.xpath("//meta[@name='description']/@content").extract_first()
            if item['description'] == None:
                item['description'] = response.xpath("//meta[@property='og:description']/@content").extract_first()
            if item['description'] == None:
                item['description'] = item['title']
        except Exception:
            item['description'] = item['title']
        # 图片
        try:
            img_url=response.xpath("//div[@class='post_body']//img/@src").extract_first()
            if 'logo' in img_url or img_url == None:
                item['img_src'] = ''
            else:item['img_src']=img_url
        except Exception:
            item['img_src'] = ''
        # 正文
        try:
            item['content'] ='\n'.join(response.xpath("//div[@class='post_body']//p/text() | //div[@class='post_body']//p/a/text()").extract())
        except Exception:
            item['content'] = ''

        # 作者
        item['author'] = '马铃薯科技'

        # 发行时间
        try:
            item['release_time'] = response.xpath("//div[@class='post_info']/text()").extract_first().replace('\n','').strip()[0:19]
        except Exception:
            item['release_time'] = time.strftime('%Y-%m-%d %H:%M:%S')
        #分类
        try:
            item['category']='tech'
        except Exception:
            item['category']='tech'
        # 来源
        item['be_from'] ='tech.163.com'


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
        #scrapy crawl hy_163
