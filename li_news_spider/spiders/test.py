import scrapy,re,time,random



class TestSpider(scrapy.Spider):
    name = 'test'
    allowed_domains = ['kingname.info']
    page=0
    start_urls = []
    for page in range(1, 1000):
        start_urls.append('https://httpbin.org/anything/{}'.format(page))

    def parse(self, response):
        print(response.text[-100:-1])
        print('='*40)
        self.page+=1

    def close(spider, reason):
        print('scrapy-arstechnica抓取完成,共抓取:',spider.page,'条数据')

    #scrapy crawl test
