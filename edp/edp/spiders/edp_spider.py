# -*- coding: utf-8 -*-

import json
import re
import scrapy
from scrapy.exceptions import DropItem
from scrapy.exceptions import CloseSpider
from edp.items import EdpUserItem, EdpPointsAuditItem, EdpCouponAuditItem

cards_no = []
input_filename = '/data/logs/scrapyd/cards_no.txt'

login_url = 'https://e.dianping.com/shopaccount/login/ajaxLogin'
uid_url = 'https://e.dianping.com/pos/charge/checkUserLogin?actionId='
consume_url = 'https://e.dianping.com/pos/consume'
member_url = 'https://e.dianping.com/manage/member/list'

login_headers = {
    'Accept': 'application/json, text/javascript',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.6,en;q=0.4',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8;',
    'Host': 'e.dianping.com',
    'Origin': 'https://e.dianping.com',
    'Referer': 'https://e.dianping.com/?redir=https%3A%2F%2Fe.dianping.com%2Fmanage%2Fhome',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36',
    'X-Request': 'JSON',
    'X-Requested-With': 'XMLHttpRequest'
}

login_data = {
    'redir': 'https%3A%2F%2Fe.dianping.com%2Fmanage%2Fhome',
    'userName': '3513023',
    'password': '112358',
    'validate': '',
    'captchaSig': '',
    'keepLogin': 'on'
}


class EdpSpider(scrapy.Spider):
    name = "edp"
    allowed_domains = ["dianping.com"]
    start_urls = [
        "https://e.dianping.com/slogin?isrefer=false",
    ]

    def parse(self, response):
        return scrapy.FormRequest(
            login_url,
            method="POST",
            formdata=login_data,
            headers=login_headers,
            callback=self.after_login
        )

    def after_login(self, response):
        if response.status != 200:
            raise CloseSpider("login failed: %s" % response.body)

        json_resp = json.loads(response.body)
        if not json_resp["success"]:
            raise CloseSpider("login failed: %s" % response.body)

        with open(input_filename, 'r') as f:
            for line in f:
                line = line.strip()
                if line == '':
                    continue

                item = EdpUserItem()

                url = '%s?memberId=%s' % (member_url, line)
                yield scrapy.Request(
                    url,
                    method="GET",
                    headers=login_headers,
                    callback=self.parse_edp_account,
                    meta={'item': item, 'number': line}
                )

    def parse_edp_account(self, response):

        # print response.body

        item = response.meta['item']
        number = response.meta['number']

        member_profile = response.css('#uid')
        if member_profile is None:
            raise DropItem("Cannot found member-profile block, number=%s", number)

        item['uid'] = self.validStr(member_profile.xpath('@data-uid').extract_first())
        if item['uid'] is None:
            raise DropItem("Cannot found uid, number=%s", number)

        uidNum = response.css('#uidNum')
        bMode = False

        item['card_no'] = self.validStr(member_profile.xpath('@data-phone').extract_first())
        if item['card_no'] is None and uidNum is not None:
            item['card_no'] = self.validStr(uidNum.xpath('small/text()').re_first(r'\([^0-9]+(\d+)\)'))
        if item['card_no'] is None:
            bMode = True
            item['card_no'] = self.validStr(uidNum.xpath('b/text()').extract_first())

        item['phone'] = self.validStr(member_profile.xpath('@data-num').extract_first())
        if item['phone'] is None and uidNum is not None and not bMode:
            item['phone'] = self.validStr(uidNum.xpath('b/text()').extract_first())

        item['level'] = self.validInt(member_profile.xpath('@data-level').extract_first())
        if item['level'] is None:
            item['level'] = 1

        birthdaySpan = response.css('#uidNum + span')

        item['birthday'] = self.validStr(member_profile.xpath('@data-birthday').extract_first())
        if item['birthday'] is None and birthdaySpan is not None:
            item['birthday'] = self.validStr(birthdaySpan.xpath('text()').re_first(r'[^0-9-]+([0-9-]+)'))

        usName = response.css('#usName')

        if usName is not None:
            item['name'] = self.validStr(usName.xpath('strong/text()').extract_first())
            item['title'] = self.validStr(usName.xpath('text()').extract_first())

        if item['card_no'] is None:
            raise DropItem("Cannot found card_no, number=%s", number)

        member_prepaid_list = response.css('.member-prepaid')
        if member_prepaid_list is None:
            raise DropItem("Cannot found member-prepaid blocks, number=%s", number)

        item['reg_time'] = self.validStr(response.xpath('((//div[@class="member-prepaid"])[1]//dd)[1]/text()').extract_first())
        item['reg_warehouse'] = self.validStr(response.xpath('((//div[@class="member-prepaid"])[1]//dd)[2]/text()').extract_first())

        item['balance'] = self.validNumber(response.xpath('((//div[@class="member-prepaid"])[2]//dd)[1]/text()').extract_first())
        item['balance_in'] = self.validNumber(response.xpath('((//div[@class="member-prepaid"])[2]//dd)[2]/text()').extract_first())
        item['points'] = self.validNumber(response.xpath('((//div[@class="member-prepaid"])[2]//dd)[3]/text()').extract_first())
        item['points_in'] = self.validNumber(response.xpath('((//div[@class="member-prepaid"])[2]//dd)[4]/text()').extract_first())

        self.parse_edp_coupons(response)
        self.parse_edp_bill_balance(response)

        bill_req = response.css('#bill')
        if bill_req is not None:

            callback_list = [
                None,
                self.parse_edp_bill_points,
                self.parse_edp_bill_coupon
            ]

            i = 0
            for link in bill_req.xpath('a'):
                bill_url = 'https://e.dianping.com%s' % self.validStr(link.xpath('@href').extract_first())

                if callback_list[i] is not None:
                    yield scrapy.Request(
                        bill_url,
                        method="GET",
                        headers=login_headers,
                        callback=callback_list[i],
                        meta={'item': item}
                    )

                i += 1

        yield item

    def parse_edp_coupons(self, response):
        item = response.meta['item']

        item['coupons'] = []

        member_coupons = response.css('.member-coupon tbody > tr')
        if member_coupons is not None:

            for member_coupon in member_coupons:
                coupon = {}
                coupon['card_no'] = item['card_no']
                coupon['name'] = self.validStr(member_coupon.xpath('@data-name').extract_first())
                if coupon['name'] is None:
                    continue
                coupon['quantity'] = self.validInt(member_coupon.xpath('(td)[2]/text()').extract_first())
                coupon['sent_time'] = self.validStr(member_coupon.xpath('(td)[3]/text()').extract_first())
                coupon['expire_at'] = self.validStr(member_coupon.xpath('(td)[4]/text()').extract_first())
                item['coupons'].append(coupon)

        item['coupon_num'] = len(item['coupons'])
        return item

    def parse_edp_bill_balance(self, response):
        item = response.meta['item']

        item['balance_bills'] = []

        member_bills = response.css('#billList tbody > tr')
        if member_bills is not None:

            for member_bill in member_bills:
                bill = {}
                bill['card_no'] = item['card_no']
                bill['type'] = self.validStr(member_bill.xpath('(td)[1]/text()').extract_first())
                if bill['type'] is None:
                    bill['type'] = self.validStr(member_bill.xpath('(td)[1]/span/text()').extract_first())
                if bill['type'] is None:
                    continue
                bill['time'] = self.validStr(member_bill.xpath('(td)[2]/text()').extract_first())
                bill['warehouse'] = self.validStr(member_bill.xpath('(td)[4]/text()').extract_first())
                bill['amount'] = self.validNumber(member_bill.xpath('(td)[5]/text()').extract_first())
                bill['reward'] = self.validNumber(member_bill.xpath('(td)[6]/text()').extract_first())
                bill['operator'] = self.validStr(member_bill.xpath('(td)[8]/text()').extract_first())
                item['balance_bills'].append(bill)
        return item

    def parse_edp_bill_points(self, response):
        item = response.meta['item']

        member_bills = response.css('#billList tbody > tr')
        if member_bills is not None:
            for member_bill in member_bills:
                bill = EdpPointsAuditItem()
                bill['card_no'] = item['card_no']
                bill['type'] = self.validStr(member_bill.xpath('(td)[1]/span/text()').extract_first())
                if bill['type'] is None:
                    bill['type'] = self.validStr(member_bill.xpath('(td)[1]/text()').extract_first())
                if bill['type'] is None:
                    continue
                bill['time'] = self.validStr(member_bill.xpath('(td)[2]/text()').extract_first())
                bill['warehouse'] = self.validStr(member_bill.xpath('(td)[4]/text()').extract_first())
                bill['amount'] = self.validInt(member_bill.xpath('(td)[5]/text()').extract_first())
                bill['operator'] = self.validStr(member_bill.xpath('(td)[6]/text()').extract_first())
                yield bill

    def parse_edp_bill_coupon(self, response):
        item = response.meta['item']

        member_bills = response.css('#billList tbody > tr')
        if member_bills is not None:
            for member_bill in member_bills:
                bill = EdpCouponAuditItem()
                bill['card_no'] = item['card_no']
                bill['type'] = self.validStr(member_bill.xpath('(td)[1]/span/text()').extract_first())
                if bill['type'] is None:
                    bill['type'] = self.validStr(member_bill.xpath('(td)[1]/text()').extract_first())
                if bill['type'] is None:
                    continue
                bill['time'] = self.validStr(member_bill.xpath('(td)[2]/text()').extract_first())
                bill['warehouse'] = self.validStr(member_bill.xpath('(td)[4]/text()').extract_first())
                bill['coupon_name'] = self.validStr(member_bill.xpath('(td)[5]/text()').extract_first())
                bill['quantity'] = self.validInt(member_bill.xpath('(td)[6]/text()').extract_first())
                bill['operator'] = self.validStr(member_bill.xpath('(td)[7]/text()').extract_first())
                yield bill

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

    # def after_found_uid(self, response):
    #     if response.status != 200:
    #         raise DropItem("find uid wrong status: %s,%s,%s" % (response.status, response.meta['params'], response.body))

    #     json_resp = json.loads(response.body)
    #     if not json_resp["result"]["uid"]:
    #         raise DropItem("find uid failed: %s,%s" % (response.meta['params'], response.body))

    #     print "[uid] %s:%s" % (response.meta["params"]["number"], json_resp["result"]["uid"])

    #     item = EdpItem()
    #     item['uid'] = json_resp["result"]["uid"]

    #     url = "%s?uid=%s" % (consume_url, json_resp["result"]["uid"])
    #     yield scrapy.Request(
    #         url,
    #         method="GET",
    #         headers=login_headers,
    #         callback=self.parse_edp_account,
    #         meta={'item': item}
    #     )
