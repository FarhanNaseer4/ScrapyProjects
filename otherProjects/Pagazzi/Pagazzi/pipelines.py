import os
import re

import scrapy
from scrapy.pipelines.images import ImagesPipeline


class CustomImagesPipeline(ImagesPipeline):

    def get_media_requests(self, item, info):
        for key, value in item.items():
            if 'Image' in key and value != '':
                yield scrapy.Request(url=value, meta={'index': ''.join(re.findall('\d+', key))})

    def file_path(self, request, response=None, info=None, *, item=None):
        return os.path.basename(f"{item.get('Name').replace(' ', '_')}_{request.meta.get('index')}.jpg")

    def item_completed(self, results, item, info):
        return item
