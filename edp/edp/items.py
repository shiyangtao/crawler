# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class TestItem(scrapy.Item):
    name = scrapy.Field()
    url = scrapy.Field()


class EdpUserItem(scrapy.Item):
    card_no = scrapy.Field()
    uid = scrapy.Field()
    name = scrapy.Field()
    title = scrapy.Field()
    phone = scrapy.Field()
    balance = scrapy.Field()
    balance_in = scrapy.Field()
    points = scrapy.Field()
    points_in = scrapy.Field()
    level = scrapy.Field()
    birthday = scrapy.Field()
    reg_time = scrapy.Field()
    reg_warehouse = scrapy.Field()
    coupon_num = scrapy.Field()     # 可用优惠券数量
    coupons = scrapy.Field()
    balance_bills = scrapy.Field()


class EdpCouponItem(scrapy.Item):
    card_no = scrapy.Field()
    name = scrapy.Field()
    quantity = scrapy.Field()
    sent_time = scrapy.Field()
    expire_at = scrapy.Field()


class EdpBalanceAuditItem(scrapy.Item):
    card_no = scrapy.Field()
    type = scrapy.Field()
    time = scrapy.Field()
    warehouse = scrapy.Field()
    amount = scrapy.Field()
    reward = scrapy.Field()
    operator = scrapy.Field()


class EdpPointsAuditItem(scrapy.Item):
    card_no = scrapy.Field()
    type = scrapy.Field()
    time = scrapy.Field()
    warehouse = scrapy.Field()
    amount = scrapy.Field()
    operator = scrapy.Field()


class EdpCouponAuditItem(scrapy.Item):
    card_no = scrapy.Field()
    type = scrapy.Field()
    time = scrapy.Field()
    warehouse = scrapy.Field()
    coupon_name = scrapy.Field()
    quantity = scrapy.Field()
    operator = scrapy.Field()
