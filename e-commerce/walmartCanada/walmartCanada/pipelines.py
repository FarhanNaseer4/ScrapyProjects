# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import scrapy
from scrapy.exceptions import DropItem
from scrapy.pipelines.images import ImagesPipeline
from itemadapter import ItemAdapter


class WalmartcanadaPipeline(ImagesPipeline):
    # def process_item(self, item, spider):
    #     return item
    def get_media_requests(self, item, info):
        for key, value in item.get('Product_Name_url').items():
            yield scrapy.Request(url=value, meta={'id': key})
        item.pop('Product_Name_url', None)

    def file_path(self, request, response=None, info=None):
        image_guid = request.meta.get('id', '')
        return f'full/{image_guid}.jpg'

    def item_completed(self, results, item, info):
        image_paths = [x['path'] for ok, x in results if ok]
        if not image_paths:
            raise DropItem("Item contains no images")
        adapter = ItemAdapter(item)
        adapter['image_path'] = image_paths
        return item
