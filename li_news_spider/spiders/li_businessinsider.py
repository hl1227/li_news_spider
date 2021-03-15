from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from ..items import LiNewsSpiderItem
import time

class Businessinsider_Spider(CrawlSpider):
    name = 'businessinsider'
    allowed_domains = ['businessinsider.in']
    start_urls = ['https://www.businessinsider.in/',]
    def __init__(self):
        super(Businessinsider_Spider, self).__init__(name='businessinsider')
        self.page=0
    #     self.start_time=time.time()
    rules = (
             Rule(LinkExtractor(allow=r'https://www.businessinsider.in/.*/articleshow/\d+.cms'),callback='parse_item', follow=True),
             Rule(LinkExtractor(allow=r'https://www.businessinsider.in/.*'), follow=True),
             )

    def parse_item(self, response):
        item = LiNewsSpiderItem()
        url=response.url
        item['url'] = url
        #item['status'] = 1
        #item["creat_time"] = time.time()
        # 标题
        try:
            item['title'] = response.xpath("//meta[@property='og:title']/@content").extract_first()
        except Exception:
            item['title'] = ''
        # 关键字
        try:
            item['keyword'] = response.xpath("//meta[@name='keywords']/@content").extract_first()
            if item['keyword'] ==None or item['keyword']=='abcnews':
                item['keyword'] = item['title']
        except Exception:
            item['keyword'] = item['title']
        # 摘要
        try:
            item['description'] = response.xpath("//meta[@name='description']/@content").extract_first()

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
            item['content'] ='\n&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;'.join(response.xpath("//div[@class='Normal']//text()").extract()).replace('Advertisement','').replace('\n','')
        except Exception:
            item['content'] = ''
        # 作者
        try:
            item['author'] = response.xpath("//div[@class='byline-cont']//text()").extract_first()
            if item['author'] == None:
                item['author'] = response.xpath("//a[@data-auth='author']/text()").extract_first()
        except Exception:
            item['author'] = ''
        # 发行时间
        try:
            item['release_time'] = response.xpath("//span[contains(text(),'IST')]//text()").extract_first().replace(' IST','')
        except Exception:
            item['release_time'] = ''
        #分类
        try:
            item['category']=self.cate(item_txt=str(item['content']))
        except Exception:
            item['category']=''
        # 来源
        item['be_from'] ='www.businessinsider.in'
        #生成器返回:
        if len(item['title']) >=1 and len(item['content']) >= 50 and item['category'] != '':
            self.page += 1
            print(time.strftime('%Y.%m.%d-%H:%M:%S'),'第',self.page,'条抓取成功 耗时:',round(time.time()-response.meta['start_time'],2),'秒',response.url,)
            yield item
        #else:print(time.strftime('%Y.%m.%d-%H:%M:%S'),'****分类不匹配或者抓取失败****:',response.url)
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
            return ''
    def close(spider, reason):
        print('scrapy-businessinsider抓取完成,共抓取:',spider.page,'条数据')
        ##self.crawler.engine.close_spider(self, "关闭spider")
        #scrapy crawl businessinsider



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
