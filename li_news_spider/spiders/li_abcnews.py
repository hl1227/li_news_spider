from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from ..items import LiNewsSpiderItem
import time

class AbcNews_Spider(CrawlSpider):
    name = 'abcnews'
    allowed_domains = ['abcnews.go.com']
    start_urls = ['https://abcnews.go.com/', 'https://abcnews.go.com/WN', 'https://abcnews.go.com/Nightline', 'https://abcnews.go.com/2020',
                  'https://abcnews.go.com/ThisWeek', 'https://abcnews.go.com/TheView', 'https://abcnews.go.com/Health/Coronavirus',
                  'https://abcnews.go.com/US', 'https://abcnews.go.com/Politics', 'https://abcnews.go.com/International',
                  'https://abcnews.go.com/Entertainment', 'https://abcnews.go.com/Business', 'https://abcnews.go.com/Technology',
                  'https://abcnews.go.com/Lifestyle', 'https://abcnews.go.com/Health', 'https://abcnews.go.com/VR', 'https://abcnews.go.com/Sports']
    page = 0
    start_time = time.time()
    rules = (
        Rule(LinkExtractor(allow=r'https://abcnews.go.com/.*/story\?id=\d+'),callback='parse_item', follow=True),
        Rule(LinkExtractor(allow=r'https://abcnews.go.com/.*/wireStory/.*-\d+'), callback='parse_item',follow=True),
        Rule(LinkExtractor(allow=r'https://abcnews.go.com/author/.*'), follow=True),
    )

    def parse_item(self, response):

        item = LiNewsSpiderItem()
        url = response.url
        #URL
        item['url'] = url
        #标题
        try:
            item['title']=response.xpath("//meta[@property='og:title']/@content").extract_first()
        except Exception:
            item['title']=''
        #关键字
        try:
            item['keyword']=response.xpath("//meta[@name='keywords']/@content").extract_first()
            if item['keyword'] ==None or item['keyword']=='abcnews':
                item['keyword'] = item['title']
        except Exception:
            item['keyword']=item['title']
        #摘要
        try:
            item['description']=response.xpath("//meta[@name='description']/@content").extract_first().replace('\n','')
            if item['description'] == None:
                item['description']=item['title']
        except Exception:
            item['description']=item['title']
        #图片
        try:
            img_url=response.xpath("//meta[@property='og:image']/@content").extract_first()
            if 'default' in img_url or img_url == None:
                item['img_src'] = ''
            else:
                item['img_src']=img_url
        except Exception:
            item['img_src']=''
        # 正文
        try:
            item['content'] = '\n&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;'.join(response.xpath("//section[@class='Article__Content story']/p[not(@id='ramplink_iPhone_' or @id='ramplink_Instagram_')]//text()").extract()).replace('ABC News','Potato Technology News').replace('\n','')
        except Exception:
            item['content'] = ''
        #作者
        try:
            author = response.xpath("//span[@class='Byline__Author ']//text()").extract()
            if len(author) >= 1:
                item['author'] = author[0].strip()
            else:
                item['author'] = response.xpath("//div[@class='Byline__Author']//text()").extract_first().strip()
        except Exception:
            item['author']=''
        # 发行时间
        try:
            item['release_time']=response.xpath("//div[@class='Byline__Meta Byline__Meta--publishDate']//text()").extract_first()
        except Exception:
            item['release_time']=''
        #分类
        try:
            item['category']=self.cate(item_txt=str(item['content']))
        except Exception:
            item['category']=''
        #来源
        item['be_from']='abcnews.go.com'
        if len(item['title']) >=1 and len(item['content']) >= 50 and item['category'] != '':
            self.page += 1
            print(time.strftime('%Y.%m.%d-%H:%M:%S'),'第',self.page,'条抓取成功 耗时:',round(time.time()-response.meta['start_time'],2),'秒',response.url,)
            yield item
        #else:print(time.strftime('%Y.%m.%d-%H:%M:%S'),'****分类不匹配或者抓取失败****:',response.url)
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
        #scrapy crawl abcnews



















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

        #分类
        # try:
        #     # if '/story?id=' in url:
        #     if '/GMA/' in url:
        #         category=url.split('/')[4]
        #     else:category=url.split('/')[3]
        #     # else:
        #     item['category']=category
        # except Exception:
        #     item['category']=''