from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from ..items import LiNewsSpiderItem
from ftplib import FTP
from io import BytesIO
import time,pymysql,scrapy,hashlib

class Greatist_Spider(CrawlSpider):

    name = 'wellandgood'
    allowed_domains = ['wellandgood.com']
    start_urls = ['https://www.wellandgood.com/']
    page = 0
    #更新设置--------------------------------------------------
    start_time = time.time()
    up_time = 600  # 更新 秒
    custom_settings = {
                       #设置爬取算法模式
                       'SCHEDULER_DISK_QUEUE' : 'scrapy.squeues.PickleFifoDiskQueue',
                       'SCHEDULER_MEMORY_QUEUE' : 'scrapy.squeues.FifoMemoryQueue',
                       'DEPTH_PRIORITY': 1, #0表示深度优先,1表示广度优先
                       'DEPTH_LIMIT' : 5    #最大深度值
                        }
    #-默认入库,入FTP,入分类设置,更新时长---------------------------
    table_name = 'Data_Content_673'   #mysql表名
    ftp_name = ''                 #FTP文件名,只要名为:test则为测试!
    default_category='other'          #默认分类
    default_img = 'life.jpg'  # 默认图片
    #--------------------------------------------------------
    rules = (Rule(LinkExtractor(allow=r'https://www.wellandgood.com/.*-.*-.*/'), callback='parse_item', follow=True),
             Rule(LinkExtractor(allow=r'https://www.wellandgood.com/.*/'),follow=True),)
    def __init__(self):
        super(Greatist_Spider, self).__init__(name='wellandgood')
        # mysql------------------------------------------
        self.conn = pymysql.Connect(
            host='154.212.112.247',
            port=13006,
            # 数据库名：
            db='test',
            user="root",
            passwd='itfkgsbxf3nyw6s1',
            charset='utf8')
        self.cur = self.conn.cursor()

        # ftp---------------------------------------------
        self.ftp = FTP()
        self.ftp.connect('154.86.175.226', 21)
        self.ftp.login(user='img', passwd='W2BpLPnyXbdmWCNd')
        self.ftp.set_pasv(False)
        self.ftp.encoding = 'utf-8'

    def parse_item(self, response):
        # 后续更新:启动10分钟后关闭
        if time.time() - self.start_time >= self.up_time:
            self.crawler.engine.close_spider(self, "更新10分钟完成!!")
        item = {}
        url=response.url
        item['url'] = url
        # # 标题
        try:
            item['title'] = response.xpath("//meta[@name='twitter:title']/@content").extract_first()
            if item['title'] == None:
                item['title'] = response.xpath("//h1//text()").extract_first().strip()
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
            item['description'] = response.xpath("//meta[@name='twitter:description']/@content").extract_first()
            if item['description'] == None:
                item['description'] = response.xpath("//meta[@property='og:description']/@content").extract_first()
            if item['description'] == None:
                item['description'] = item['title']
        except Exception:
            item['description'] = item['title']
        # 图片
        try:
            img_url=response.xpath("//meta[@name='twitter:image']/@content").extract_first()
            if 'default' in img_url or img_url == None:
                item['img_src'] = 'https://www.baidu.com/'
            else:item['img_src']=img_url
        except Exception:
            item['img_src'] = 'https://www.baidu.com/'
        # 正文
        try:
            item['content'] =''.join(response.xpath("//div[@class='editor-content']/div[@class='drop-cap text-big text-gray']//text() | //div[@class='editor-content']/p//text()").extract()).replace('.','.\n')
            if item['content'] == None or len(item['content']) < 1:
                item['content'] = ''.join(response.xpath("//main[@class='page__main']//text()").extract()).replace('.', '.\n')
        except Exception:
            item['content'] = ''
        # 作者
        try:
            item['author'] = response.xpath("//meta[@name='twitter:data1']/@content").extract_first()
            if item['author'] == None:
                item['author'] = 'Guide to life in India'
        except Exception:
            item['author'] = 'Guide to life in India'
        # 发行时间
        try:
            item['release_time'] = response.xpath("//meta[@property='article:published_time']/@content").extract_first().strip()[0:10]
        except Exception:
            item['release_time'] = time.ctime(time.time()-86400)
        #分类
        try:
            item['category']='Life Health'
        except Exception:
            item['category']='Life Health'
        # 来源
        item['be_from'] ='www.wellandgood.com'
        #生成器返回:
        if len(item['title']) >=1 and len(item['content']) >= 50 and item['category'] != '':
            #-----------入库去重--------------------------------------
            find_ex = "select id,PageUrl from {} where title= %s ".format(self.table_name)
            self.cur.execute(find_ex, (item["title"]))
            if not self.cur.fetchone():
                #封面图片
                #if item['img_src'] != '':
                yield scrapy.Request(url=item['img_src'], callback=self.img_parse, meta={'item': item},dont_filter=True)
                # else:
                #     #self.img_parse(response=scrapy.http.HtmlResponse(url='',meta={'item':item},body=))
                #     yield scrapy.Request(url='https://www.baidu.com/', callback=self.img_parse, meta={'item': item},dont_filter=True)
            else:print('**数据重复:',item['url'])
        else:
            print('##数据不匹配--状态码:',response.status,'标题长度:',len(item['title']),'文本长度:',len(item['content']),'category:',item['category'],response.url)

    def img_parse(self,response):
        item = response.meta['item']
        if 'www.baidu.com' in response.url or response.status >=400:
            img_path='<img src="https://img.ksyoume.cn/img_upload/{}" alt="{}" />'.format(self.default_img,item['keyword'])
        else:
            if self.ftp_name != 'test':
                time_name = time.strftime("%Y-%m-%d", time.localtime(time.time())).replace('-', '')
                try:self.ftp.mkd(time_name)
                except Exception:pass
            else:
                time_name ='test'
            m = hashlib.md5()
            m.update(response.url.encode('utf-8'))
            base_txtname_md5 = m.hexdigest() + '.jpg'
            ftp_path = time_name + '/' + base_txtname_md5
            try:
                fp = BytesIO(response.body)
                self.ftp.storbinary(cmd="STOR %s" % ftp_path,fp=fp )  #上传文件
                img_path = '<img src="https://img.ksyoume.cn/img_upload/{}" alt="{}" />'.format(ftp_path,item['keyword'])
            except Exception as e:
                img_path = '<img src="https://img.ksyoume.cn/img_upload/{}" alt="{}" />'.format(self.default_img,item['keyword'])
                print(item['url'],'!!!图片上传失败:',e)
                self.ftp.close()
                time.sleep(2)
                self.ftp.connect('154.86.175.226', 21)
                self.ftp.login(user='img', passwd='W2BpLPnyXbdmWCNd')
                self.ftp.set_pasv(False)
                self.ftp.encoding = 'utf-8'
        news = "insert into {}(title,content,author,time,keywords,description,tag,PageUrl,thumbid,category,create_time,be_from) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(self.table_name)
        self.cur.execute(news, (item['title'], img_path + '\n' + item['content'], item['author'], item['release_time'], item['keyword'],item['description'], item['keyword'], item['url'], img_path, item['category'], time.time(),item['be_from']))
        self.page += 1
        print(time.strftime('%Y.%m.%d-%H:%M:%S'), '第', self.page, '条抓取成功:',item['url'])


    #分类区分
    def cate(self,item_txt):
        if ' network ' in item_txt or ' technology ' in item_txt or ' download ' in item_txt or ' Samsung ' in item_txt or ' PC ' in item_txt or ' Technology 'in item_txt or ' Internet ' in item_txt or ' camera 'in item_txt or ' TECH ' in item_txt :
            return 'other'
        elif ' phone 'in item_txt or ' Android 'in item_txt or ' app 'in item_txt or ' Skype ' in item_txt:
            return 'Mobile'
        elif ' TIKTOK 'in item_txt or ' Tik tok 'in item_txt or ' Tik Tok 'in item_txt or ' TIK TOK ' in item_txt:
            return 'TIKTOK'
        elif ' Microsoft 'in item_txt or ' Windows 'in item_txt or ' Office 'in item_txt or ' Outlook ' in item_txt:
            return 'Microsoft'
        elif ' Facebook ' in item_txt:
            return 'Facebook'
        elif ' Tesla ' in item_txt or ' Elon Musk ' in item_txt:
            return 'Tesla'
        elif ' Huawei ' in item_txt:
            return 'Huawei'
        elif ' Amazon 'in item_txt or ' Jeff Bezos ' in item_txt:
            return 'Amazon'
        elif ' AI ' in item_txt:
            return 'AI'
        elif ' Google 'in item_txt:
            return 'Google'
        elif ' iOS 'in item_txt or ' Apple 'in item_txt or ' iPhone 'in item_txt or ' iPad 'in item_txt or ' iMac 'in item_txt or ' MacBook 'in item_txt or ' Watch 'in item_txt or ' iCloud 'in item_txt or ' iTunes ' in item_txt:
            return 'Apple'
        else:
            return self.default_category
    def close(self, reason):
        print(reason,'共抓取:',self.page,'条数据')
        self.conn.close()
        self.ftp.close()
        ##self.crawler.engine.close_spider(self, "关闭spider")
        #scrapy crawl wellandgood

