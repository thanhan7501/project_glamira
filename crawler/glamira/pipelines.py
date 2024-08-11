# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import json


class GlamiraPipeline:
    def process_item(self, item, spider):
        self.file.write(f'"{item["product_id"]}": "{item["product_name"]}"')
        if item["current_item"] != item["total_item"]:
            self.file.write(",")
        return item

    def open_spider(self, spider):
        file_name = spider.settings.get("COLLECTION_NAME")
        self.file = open(f"{file_name}.json", "w", encoding="utf-8")
        self.file.write("{")

    def close_spider(self, spider):
        self.file.write("}")
        self.file.close()
