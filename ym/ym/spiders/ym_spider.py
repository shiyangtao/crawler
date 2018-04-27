# -*- coding: utf-8 -*-

import re
import scrapy
from scrapy.exceptions import DropItem
from ym.items import YmItem


class YmSpider(scrapy.Spider):
    name = 'ym'
    start_urls = [
        # 'http://127.0.0.1/ym.raw.xml'
        'http://sdk4report.eucp.b2m.cn:8080/sdkproxy/getreport.action?cdkey=6SDK-EMY-6688-JBTSM&password=811096'
    ]

    def parse(self, response):

        for msg in response.xpath('//message'):
            item = YmItem()
            item['seqid'] = self.validStr(msg.xpath('seqid/text()').extract_first())
            item['phone'] = self.validStr(msg.xpath('srctermid/text()').extract_first())
            item['submitDate'] = self.validStr(msg.xpath('submitDate/text()').extract_first())
            if item['submitDate'] is None:
                item['submitDate'] = self.validStr(msg.xpath('submitdate/text()').extract_first())
            item['receiveDate'] = self.validStr(msg.xpath('receiveDate/text()').extract_first())
            if item['receiveDate'] is None:
                item['receiveDate'] = self.validStr(msg.xpath('receivedate/text()').extract_first())
            item['state'] = self.validStr(msg.xpath('state/text()').extract_first())

            if item['seqid'] is None or item['phone'] is None:
                raise DropItem("Dropped invalid record")

            yield item

    def validStr(self, str):
        if str is None:
            return None
        else:
            return str.strip()

    def validInt(self, str):
        result = 0
        if str is None:
            return 0
        else:
            str = str.split('.')[0]
            str = re.sub('[, ]', '', str)
            try:
                result = int(str)
            except:
                pass
            return result

    def validNumber(self, str):
        result = 0.0
        if str is None:
            return 0.0
        else:
            str = re.sub('[, ]', '', str)
            try:
                result = float(str)
            except:
                pass
            return result
