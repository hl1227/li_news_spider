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
        self.start_time=time.time()
    rules = (
             Rule(LinkExtractor(allow=r'https://www.businessinsider.in/.*/articleshow/\d+.cms'),callback='parse_item', follow=True),
             Rule(LinkExtractor(allow=r'https://www.businessinsider.in/.*'), follow=True),
             )

    def parse_item(self, response):
        item = LiNewsSpiderItem()
        item['url'] = response.url
        #item['status'] = 1
        #item["creat_time"] = time.time()
        #分类
        try:
            #item['category']=response.url.split('/')[3]
            item['category']=response.xpath("//li[@itemprop='itemListElement'][2]/a//text()").extract_first()
        except Exception:
            item['category']=''
        #关键字
        try:
            item['keyword']=response.xpath("//meta[@name='keywords']/@content").extract_first()
        except Exception:
            item['keyword']=''
        #摘要
        try:
            item['description']=response.xpath("//meta[@name='description']/@content").extract_first()
        except Exception:
            item['description']=''
        #图片
        try:
            item['img_src']='<img src="'+response.xpath("//meta[@property='og:image']/@content").extract_first()+'" >'
        except Exception:
            item['img_src']=''
        # 正文
        try:
            item['content'] = item['img_src']+' '+''.join(response.xpath("//div[@class='Normal']//text()").extract()).replace('Advertisement','')
        except Exception:
            item['content'] = ''
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
        #作者
        try:
            item['author']=response.xpath("//div[@class='byline-cont']//text()").extract_first()
            if item['author'] ==None:
                item['author']=response.xpath("//a[@data-auth='author']/text()").extract_first()
        except Exception:
            item['author']=''
        # 发行时间
        try:
            item['release_time']=response.xpath("//span[contains(text(),'IST')]//text()").extract_first().replace(' IST','')
        except Exception:
            item['release_time']=''
        #标题
        try:
            item['title']=response.xpath("//meta[@property='og:title']/@content").extract_first()
        except Exception:
            item['title']=None
        if item['title'] is not None and len(item['content']) >= 50:
            self.page += 1
            #print(time.strftime('%Y.%m.%d-%H:%M:%S'),'第',self.page,'条抓取成功,url:', item['url'],'耗时:',time.time()-response.meta['start_time'],'秒')
            print(time.strftime('%Y.%m.%d-%H:%M:%S'),'第',self.page,'条抓取成功 耗时:',round(time.time()-response.meta['start_time'],2),'秒',response.url,)
            yield item

        else:print(time.strftime('%Y.%m.%d-%H:%M:%S'),'******抓取失败:',response.url)
    def close(spider, reason):
        print('scrapy-arstechnica抓取完成,共抓取:',spider.page,'条数据')
        ##self.crawler.engine.close_spider(self, "关闭spider")
        #scrapy crawl businessinsider
