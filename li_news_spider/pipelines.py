# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymysql


class LiNewsSpiderPipeline:
    def open_spider(self, spider):
        self.conn = pymysql.Connect(
            host='154.212.112.247',
            port=13006,
            # 数据库名：
            db='test',
            user="root",
            passwd='itfkgsbxf3nyw6s1',
            charset='utf8')
        self.cur = self.conn.cursor()
    def process_item(self, item, spider):
        news="insert into Data_Content_665(title,content,author,time,keywords,description,tag,PageUrl,thumbid,category) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        self.cur.execute(news,(item['title'],item['content'],item['author'],item['release_time'],item['keyword'],item['description'],item['keyword'],item['url'],item['img_src'],item['category']))
        self.conn.commit()
        return item


    def close_spider(self, spider):
        self.conn.close()