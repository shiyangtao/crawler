# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


import json
import codecs
from ym.items import YmItem


class JsonWriterPipeline(object):

    files = {
        'YmItem': None,
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
            self.files[k] = codecs.open('%s/%s.json' % (self.export_dir, k), 'a+', encoding='utf-8')

    def process_item(self, item, spider):
        if isinstance(item, YmItem):
            line = json.dumps(dict(item), ensure_ascii=False) + "\n"
            self.files['YmItem'].write(line)

        return item

    def close_spider(self, spider):
        print 'close_spider:jsonwriter_pipeline'
        for k in self.files.keys():
            self.files[k].close()
