from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from ..items import LiNewsSpiderItem
#from ftplib import FTP
#from io import BytesIO
import time,pymysql,scrapy#,hashlib,emoji

class Businesstoday_Spider(CrawlSpider):
    name = 'businesstoday'
    allowed_domains = ['businesstoday.in']
    start_urls = ['https://www.businesstoday.in/']
    page = 0
    # 更新设置--------------------------------------------------
    # start_time = time.time()
    # up_time = 600  # 更新 秒
    # custom_settings = {
    #     # 设置爬取算法模式
    #     'SCHEDULER_DISK_QUEUE': 'scrapy.squeues.PickleFifoDiskQueue',
    #     'SCHEDULER_MEMORY_QUEUE': 'scrapy.squeues.FifoMemoryQueue',
    #     'DEPTH_PRIORITY': 1,  # 0表示深度优先,1表示广度优先
    #     'DEPTH_LIMIT': 5}  # 最大深度值
    #-默认入库,入FTP,入分类设置---------------------------
    table_name = 'Data_Content_669'   #mysql表名
    default_category='other'          #默认分类
    rules = (Rule(LinkExtractor(allow=r'https://www.businesstoday.in/.*/.*-.*-.*-.*-.*-.*/story/.*'), callback='parse_item', follow=True),
             #Rule(LinkExtractor(allow=r'https://www.businesstoday.in/latest/.*'), follow=True),
             Rule(LinkExtractor(allow=r'https://www.businesstoday.in/.*'),follow=True)
             )
    # mysql------------------------------------------
    conn = pymysql.Connect(
        host='154.212.112.247',
        port=13006,
        # 数据库名：
        db='test',
        user="root",
        passwd='itfkgsbxf3nyw6s1',
        charset='utf8mb4')
    cur = conn.cursor()

    def parse_item(self, response):
        # 后续更新:启动10分钟后关闭
        # if time.time() - self.start_time >= self.up_time:
        #     self.crawler.engine.close_spider(self, "更新10分钟完成!!")
        item = {}
        url=response.url
        item['url'] = url
        # 标题
        try:
            item['title'] = response.xpath("//title/text()").extract_first().replace('\n','')
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
                item['description'] = item['title']
        except Exception:
            item['description'] = item['title']
        # 图片
        try:
            img_url=response.xpath("//meta[@name='twitter:image']/@content").extract_first()
            if 'default' in img_url or img_url == None:
                print('***没有图片:',item['url'])
                item['img_src'] = '<img src="https://img.ksyoume.cn/img_upload/b45c9f4afad48af33b47a52b08ac5c85.jpg" alt="{}" />'.format(item['keyword'])
            else:item['img_src']= '<img src="'+img_url+'" alt="{}" />'.format(item['keyword'])
        except Exception:
            item['img_src'] = '<img src="https://img.ksyoume.cn/img_upload/b45c9f4afad48af33b47a52b08ac5c85.jpg" alt="{}" />'.format(item['keyword'])
        # 正文
        try:
            item['content'] ='\n'.join(response.xpath("//div[contains(@class,'story-right relatedstory paywall')]/p//text() | //div[@class='storyBody paywall']/p//text() | //div[@class='story-right']//text()").extract())
        except Exception :
            item['content'] =''
        # 作者
        try:
            item['author'] = response.xpath("//div[@class='story-details']/a/text()").extract_first()
            if item['author'] == None:
                item['author'] = 'ONION INDIA NEWS'
        except Exception:
            item['author'] = 'ONION INDIA NEWS'
        # 发行时间
        try:
            item['release_time'] = response.xpath("//div[@class='story-details']/text()").extract_first().replace('| ','').replace('Updated ','').replace('Print Edition:','').strip()
            if item['release_time'] == None or len(item['release_time'])<8:
                item['release_time'] = time.ctime(time.time()-86400)
        except Exception:
            item['release_time'] = time.ctime(time.time()-86400)
        #分类
        try:
            cate = '/'.join(response.xpath("//div[@id='pathway']//text() | //ol[@class='breadcrumb']//text()").extract())
            item['category']=self.cate(item_txt=cate)
        except Exception:
            item['category']=self.default_category
        #来源
        item['be_from'] ='www.businesstoday.in'
        # #验证图片请求速度
        # item['img_time']=time.time()
        #生成器返回:
        if len(item['title']) >=1 and len(item['content']) >= 50 and item['category'] != '':
            #-----------入库去重--------------------------------------
            find_ex = "select id,PageUrl from {} where title= %s ".format(self.table_name)
            self.cur.execute(find_ex, (item["title"]))
            if not self.cur.fetchone():
                news = "insert into {}(title,content,author,time,keywords,description,tag,PageUrl,thumbid,category,create_time,be_from) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(self.table_name)
                self.cur.execute(news, (item['title'], item['img_src'] + '\n' + item['content'], item['author'], item['release_time'], item['keyword'],item['description'], item['keyword'], item['url'], item['img_src'], item['category'], time.time(),item['be_from']))
                self.page += 1
                print(time.strftime('%Y.%m.%d-%H:%M:%S'), '第', self.page, '条抓取成功:',item['category'], item['url'])
            else:print('**数据重复:',item['url'])
        else:
            print('##数据不匹配:标题长度:',len(item['title']),'文本长度:',len(item['content']),'category:',item['category'],response.url)

    #分类区分
    def cate(self,item_txt):
        if 'Economy' in item_txt:
            return 'Economy'
        elif 'Corporate' in item_txt:
            return 'Corporate'
        elif 'MARKETS' in item_txt:
            return 'Markets'
        elif 'money' in item_txt:
            return 'Money'
        else:
            return self.default_category
    def close(self, reason):
        print(reason,'共抓取:',self.page,'条数据')
        self.conn.close()
        ##self.crawler.engine.close_spider(self, "关闭spider")
        #scrapy crawl businesstoday

