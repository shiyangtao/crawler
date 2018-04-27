# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json
import codecs
from edp.items import EdpUserItem, EdpCouponItem, EdpBalanceAuditItem, EdpPointsAuditItem, EdpCouponAuditItem


class JsonWriterPipeline(object):

    files = {
        'EdpUserItem': None,
        'EdpCouponItem': None,
        'EdpBalanceAuditItem': None,
        'EdpPointsAuditItem': None,
        'EdpCouponAuditItem': None
    }

    def __init__(self, export_dir):
        self.export_dir = export_dir

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            export_dir=crawler.settings.get('JSON_EXPORT_DIR'),
        )

    def open_spider(self, spider):
        print 'open_spider:jsonwriter_pipeline'
        for k in self.files.keys():
            self.files[k] = codecs.open('%s/%s.json' % (self.export_dir, k), 'w', encoding='utf-8')

    def process_item(self, item, spider):

        if isinstance(item, EdpUserItem):
            if item['coupons'] is not None and len(item['coupons']) > 0:
                item['coupon_num'] = len(item['coupons'])
                for coupon in item['coupons']:
                    coupon_line = json.dumps(dict(coupon), ensure_ascii=False) + "\n"
                    self.files['EdpCouponItem'].write(coupon_line)

            if item['balance_bills'] is not None and len(item['balance_bills']) > 0:
                for bill in item['balance_bills']:
                    bill_line = json.dumps(dict(bill), ensure_ascii=False) + "\n"
                    self.files['EdpBalanceAuditItem'].write(bill_line)

            item['coupons'] = None
            item['balance_bills'] = None
            line = json.dumps(dict(item), ensure_ascii=False) + "\n"
            self.files['EdpUserItem'].write(line)

        elif isinstance(item, EdpCouponItem):
            line = json.dumps(dict(item), ensure_ascii=False) + "\n"
            self.files['EdpCouponItem'].write(line)

        elif isinstance(item, EdpBalanceAuditItem):
            line = json.dumps(dict(item), ensure_ascii=False) + "\n"
            self.files['EdpBalanceAuditItem'].write(line)

        elif isinstance(item, EdpPointsAuditItem):
            line = json.dumps(dict(item), ensure_ascii=False) + "\n"
            self.files['EdpPointsAuditItem'].write(line)

        elif isinstance(item, EdpCouponAuditItem):
            line = json.dumps(dict(item), ensure_ascii=False) + "\n"
            self.files['EdpCouponAuditItem'].write(line)

        return item

    def close_spider(self, spider):
        print 'close_spider:jsonwriter_pipeline'
        for k in self.files.keys():
            self.files[k].close()
