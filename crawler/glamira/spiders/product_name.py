import scrapy
import pymongo


class ProductNameSpider(scrapy.Spider):
    name = "product-name"
    custom_settings = {
        "COLLECTION_NAME": "glamira",
        "FEEDS": {
            "gs://glamira_data/product_name/product_name.csv": {
                'format': 'csv',
                'fields': ['product_id', 'product_name'],
            }
        }
    }

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        spider = cls(settings)
        spider._set_crawler(crawler)
        return spider

    def __init__(self, settings):
        self.mongo_uri = settings.get("MONGODB_URI")
        self.mongo_db = settings.get("MONGODB_DB")
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        collection = settings.get("COLLECTION_NAME")
        self.collection = self.db[collection]
        self.products = []
        self.products_last_index = 0

    def start_requests(self):
        products = list(self.collection.aggregate([
            {
                "$match": {
                    "collection": {
                        "$in": ["view_product_detail", "select_product_option", "select_product_option_quality"]
                    }

                }
            },
            {
                "$group": {
                    "_id": "$product_id",
                    "doc": {"$first": "$$ROOT"}
                }
            },
            {
                "$replaceRoot": {"newRoot": "$doc"}
            }
        ]))
        self.products = products
        # products = self.collection.distinct("product_id")
        print(len(self.products))
        current_index = 0
        first_url = self.products[current_index]["current_url"]
        self.products_last_index = len(self.products) - 1
        yield scrapy.Request(
            url=first_url,
            callback=self.parse_next_item,
            dont_filter=True,
            meta={
                "product_id": self.products[current_index]["product_id"],
                "current_index": current_index,
                "last_index": self.products_last_index
            }
        )

    def parse_next_item(self, response):
        product_id = response.meta['product_id']
        current_index = response.meta['current_index']
        last_index = response.meta['last_index']
        if response.status not in (404, 500):
            product_name = response.xpath('//span[@class="base" and @data-ui-id="page-title-wrapper"]/text()').get()
            if product_name is not None:
                yield {
                    "product_id": product_id,
                    "product_name": product_name,
                    "current_item": current_index + 1,
                    "total_item": last_index + 1
                }

        if current_index < last_index:
            result = self.get_next_url(current_index)
            next_url = result["next_url"]
            current_index = result["current_index"]
            yield scrapy.Request(
                url=next_url,
                callback=self.parse_next_item,
                dont_filter=True,
                meta={
                    "product_id": self.products[current_index]["product_id"],
                    "current_index": current_index,
                    "last_index": self.products_last_index
                }
            )

    def get_next_url(self, current_index):
        current_index += 1
        next_url = self.products[current_index]["current_url"]
        if not next_url.startswith("http"):
            return self.get_next_url(current_index)

        return {
            "current_index": current_index,
            "next_url": next_url
        }