# -*- coding: utf-8 -*-

import scrapy
from scrapy.exceptions import DropItem
from edp.items import TestItem


class TestSpider(scrapy.Spider):
    name = "test"
    allowed_domains = ["sina.com.cn"]
    start_urls = [
        "http://www.sina.com.cn/",
    ]

    def parse(self, response):
        for sel in response.css('.main-nav ul > li > a'):
            item = TestItem()
            item["name"] = sel.xpath("text()").extract_first()
            item["url"] = sel.xpath("@href").extract_first()
            if item["name"] is None or item["url"] is None:
                raise DropItem("Contains empty value: %s" % item)
            else:
                yield item
