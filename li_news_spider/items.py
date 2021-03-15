# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class LiNewsSpiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()

    title = scrapy.Field()  # 标题
    author = scrapy.Field()  # 作者
    keyword = scrapy.Field()  # 关键字
    tag  = scrapy.Field()     # 关键字
    img_src = scrapy.Field()  # 图片
    content = scrapy.Field()  # 正文
    url = scrapy.Field()  # 网址
    # status = scrapy.Field()  # 状态
    # txt_link = scrapy.Field()  # 文本内链接
    category = scrapy.Field()  # 类别
    #creat_time = scrapy.Field()  # 创建时间
    description = scrapy.Field()  # 摘要
    release_time = scrapy.Field()  # 发行时间
    be_from=scrapy.Field() #来源
