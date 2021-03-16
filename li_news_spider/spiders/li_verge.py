from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from ..items import LiNewsSpiderItem
from ftplib import FTP
from io import BytesIO
import time,pymysql,scrapy,hashlib

class Verge_Spider(CrawlSpider):
    name = 'verge'
    allowed_domains = ['theverge.com','vox-cdn.com']
    start_urls = ['https://www.theverge.com/']
    #-默认入库,入FTP,入分类设置---------------------------
    table_name = 'Data_Content_665'   #mysql表名
    ftp_name = ''                 #FTP文件名,只要名为:test则为测试!
    default_category='other'          #默认分类
    # def __init__(self):
    #     super(Verge_Spider, self).__init__(name='verge')
    page=0
    rules = (Rule(LinkExtractor(allow=r'https://www.theverge.com/\d+/\d+/\d+/\d+/.*'), callback='parse_item', follow=True),
             Rule(LinkExtractor(allow=r'https://www.theverge.com/.*'),follow=True),)
    # mysql------------------------------------------
    conn = pymysql.Connect(
        host='154.212.112.247',
        port=13006,
        # 数据库名：
        db='test',
        user="root",
        passwd='itfkgsbxf3nyw6s1',
        charset='utf8')
    cur = conn.cursor()
    # ftp---------------------------------------------
    ftp = FTP()
    ftp.connect('154.86.175.226', 21)
    ftp.login(user='img', passwd='W2BpLPnyXbdmWCNd')
    ftp.set_pasv(False)
    ftp.encoding = 'utf-8'

    def parse_item(self, response):
        item = LiNewsSpiderItem()
        url=response.url
        item['url'] = url
        # 标题
        try:
            item['title'] = response.xpath("//h1[@class='c-page-title']/text()").extract_first().replace('\n','')
            if item['title'] == None:
                item['title'] = ''
        except Exception:
            item['title'] = ''
        # 关键字
        try:
            item['keyword'] = response.xpath("//meta[@name='parsely-tags']/@content").extract_first().replace('verge,front-page,','')
            if item['keyword'] ==None or item['keyword']=='verge,front-page' or len(item['keyword'])<1:
                item['keyword'] = item['title']
        except Exception:
            item['keyword'] = item['title']
        # 摘要
        try:
            item['description'] = response.xpath("//meta[@name='description']/@content").extract_first()
            if item['description'] == None:
                item['description'] = item['title']
        except Exception:
            item['description'] = item['title']
        # 图片
        try:
            img_url=response.xpath("//meta[@property='og:image']/@content").extract_first()
            if 'default' in img_url or img_url == None:
                item['img_src'] = ''
            else:item['img_src']=img_url
        except Exception:
            item['img_src'] = ''
        # 正文
        try:
            item['content'] =''.join(response.xpath("//div[@class='c-entry-content ']/p//text()").extract()).replace('.','.\n')
        except Exception:
            item['content'] = ''
        # 作者
        try:
            item['author'] = response.xpath("//meta[@name='parsely-author']/@content").extract_first()
            if item['author'] == None:
                item['author'] = 'POTATO TECHNOLOGY NEWS'
        except Exception:
            item['author'] = 'POTATO TECHNOLOGY NEWS'
        # 发行时间
        try:
            item['release_time'] = response.xpath('//time[@class="c-byline__item"]/text()').extract_first().replace('\n','').strip()
        except Exception:
            item['release_time'] = ''
        #分类
        try:
            item['category']=self.cate(item_txt=str(item['content']))
        except Exception:
            item['category']=''
        # 来源
        item['be_from'] ='www.theverge.com'

        #生成器返回:
        if len(item['title']) >=1 and len(item['content']) >= 50 and item['category'] != '':
            #-----------入库去重--------------------------------------
            find_ex = "select id,PageUrl from {} where title= %s ".format(self.table_name)
            self.cur.execute(find_ex, (item["title"]))
            if not self.cur.fetchone():
                #封面图片
                if item['img_src'] != '':
                    yield scrapy.Request(url=item['img_src'], callback=self.img_parse, meta={'item': item})
                # else:
                #     #self.img_parse(response=scrapy.http.HtmlResponse(url='',meta={'item':item},body=))
                #     yield scrapy.Request(url='https://www.baidu.com/', callback=self.img_parse, meta={'item': item},dont_filter=True)
            else:print('**数据重复:',item['url'])
        else:
            print('##数据不匹配:标题长度:',len(item['title']),'文本长度:',len(item['content']),'category:',item['category'],response.url)
    def img_parse(self,response):
        item = response.meta['item']
        if 'www.baidu.com' in response.url or response.status >302:
            img_path='<img src="https://img.ksyoume.cn/img_upload/95e3325e00471c3a0705d42e406d69a3.jpg" alt="" >'
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
                img_path= '<img src="https://img.ksyoume.cn/img_upload/' + ftp_path + '" alt="{}"'.format(item['keyword']) + ' />'
            except Exception as e:
                img_path = '<img src="https://img.ksyoume.cn/img_upload/95e3325e00471c3a0705d42e406d69a3.jpg" alt="" >'
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
        print('scrapy-businessinsider抓取完成,共抓取:',self.page,'条数据')
        self.conn.close()
        self.ftp.close()
        ##self.crawler.engine.close_spider(self, "关闭spider")
        #scrapy crawl verge



        # 文本内链接
        # try:
        #     txt_link_list = []
        #     label_a=response.xpath("//div[@class='Normal']//a")
        #     for a in label_a:
        #         txt=a.xpath("./text()").extract_first()
        #         if txt != None:
        #             txt_link = {}
        #             txt_link['txt']=txt
        #             txt_link['link']=a.xpath("./@href").extract_first()
        #             txt_link_list.append(txt_link)
        #     item['txt_link']=txt_link_list
        # except Exception:
        #     item['txt_link']=''
