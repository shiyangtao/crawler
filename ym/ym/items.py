# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class YmItem(scrapy.Item):
    seqid = scrapy.Field()
    phone = scrapy.Field()
    submitDate = scrapy.Field()
    receiveDate = scrapy.Field()
    state = scrapy.Field()
