from datetime import datetime
import re
from urllib.parse import urljoin
from nameparser import HumanName
from pydispatch import dispatcher
from pymongo import MongoClient
from scrapy import signals
import scrapy


class DyerchamberSpider(scrapy.Spider):
    name = 'dyerchamber'
    start_urls = ['https://business.dyerchamber.com/list']

    ########################### MUST PATES THIS DATA INTO THE SCRAPER #############################
    scraped_data_database = 'Cron_Job'  # Replace the Database Name for Scraped Data
    custom_settings = {'ROBOTSTXT_OBEY': False,
                       'RETRY_TIMES': 5,
                       'HTTPERROR_ALLOW_ALL': True,
                       }
    cron_headers = ['_id', 'Business Name', 'Full Name', 'First Name', 'Last Name', 'Street Address',
                    'Valid To', 'State', 'Zip', 'Description', 'Phone Number', 'Phone Number 1', 'Email',
                    'Business_Site', 'Social_Media', 'Record_Type',
                    'Category', 'Rating', 'Reviews', 'Source_URL', 'Detail_Url', 'Services',
                    'Latitude', 'Longitude', 'Occupation',
                    'Business_Type', 'Lead_Source', 'State_Abrv', 'State_TZ', 'State_Type',
                    'SIC_Sectors', 'SIC_Categories', 'SIC_Industries', 'NAICS_Code', 'Quick_Occupation',
                    'Scraped_date', 'Meta_Description']
    ###############################################################################################

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
                  'application/signed-exchange;v=b3;q=0.9',
        'accept-language': 'en-US,en;q=0.9',
        'sec-ch-ua': '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/105.0.0.0 Safari/537.36 '
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        self.item_no = 0
        self.old_agents_set = set()
        self.new_agents_set = set()
        self.collection = self.establish_connection_and_return_collection(database_name=self.scraped_data_database,
                                                                          collection_name=f"{self.name}_cron")
        for index, record in enumerate(self.collection.find()):
            print('Reading Old Agent IDs from MongoDB')
            print(f"Record no {index} of Old {self.name}_cron is under process")
            self.old_agents_set.add(record.get('_id'))

    def parse(self, response):
        for data in response.css('div[id="mn-alphanumeric"] a'):
            url = data.css('::attr(href)').get()
            item = dict()
            for head in self.cron_headers:
                item[head] = ''
            item['Meta_Description'] = response.xpath('//meta[@name="description" or '
                                                      '@property="og:description"]/@content').get('').strip()
            if url:
                yield response.follow(url=url, callback=self.parse_listing, headers=self.headers, meta={'item': item})

    def parse_listing(self, response):
        item = response.meta['item']
        for data in response.css('div[itemprop="name"] a'):
            detail_url = data.css('::attr(href)').get()
            if detail_url:
                yield response.follow(url=detail_url, callback=self.detail_page, headers=self.headers,
                                      meta={'item': item})

    def detail_page(self, response):
        item = response.meta['item']
        item['Business Name'] = response.css('h1[itemprop="name"]::text').get('').strip()
        item['Street Address'] = response.css('div[itemprop="streetAddress"]::text').get('').strip()
        item['State'] = response.css('span[itemprop="addressRegion"]::text').get('').strip()
        item['Zip'] = response.css('span[itemprop="postalCode"]::text').get('').strip()
        item['Phone Number'] = response.css('div.mn-member-phone1::text').get('').strip()
        item['Phone Number 1'] = response.css('div.mn-member-phone2::text').get('').strip()
        item['Business_Site'] = response.css('a[itemprop="url"]::attr(href)').get('').strip()
        item['Social_Media'] = ', '.join(
            data.css('::attr(href)').get('') for data in response.css('li.gz-card-social a'))
        item['Source_URL'] = 'https://business.dyerchamber.com/list'
        item['Occupation'] = response.css('ul.mn-member-cats li::text').get('').strip()
        fullname = response.css('div.gz-member-repname::text').get('').strip()
        if fullname:
            item['Full Name'] = fullname
            item['First Name'] = fullname.split(' ')[0].strip()
            item['Last Name'] = fullname.split(' ')[-1].strip()
        item['Lead_Source'] = 'dyerchamber'
        item['Detail_Url'] = response.url
        item['Record_Type'] = 'Business'
        item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Inserting and Updating Data in MongoDB
        item['_id'] = item.get('Detail_Url')
        old_item = self.collection.find_one({'_id': item.get('_id')}) if self.collection.find_one({
            '_id': item.get('_id')}) else {}

        if old_item:
            item['FirstSessionID'] = f"{item.get('_id')}_" \
                                     f"{int(old_item.get('FirstSessionID', '').split('_')[-1]) + 1}"
            item['LastSessionID'] = old_item.get('FirstSessionID', '')
            item['CurrentStatus'] = 'Listed'
            item['Is Phone Number Change'] = 'False' if item.get('Phone Number') == old_item.get(
                'Phone Number') else 'True'
            item['TimesFound'] = old_item.get('TimesFound', '') + 1
        else:
            item['FirstSessionID'] = f"{item.get('_id')}_1"
            item['LastSessionID'] = 0
            item['CurrentStatus'] = 'New'
            item['Is Phone Number Change'] = 'First Run'
            item['TimesFound'] = 1

        self.collection.find_one_and_update(filter={"_id": item.get('_id')},
                                            update={"$set": item},
                                            upsert=True)
        print(f"Item No {self.item_no} Inserted and _ID is ---> ", item.get('_id'))
        self.new_agents_set.add(item.get('_id'))
        self.item_no += 1

    def spider_closed(self):
        delisted_agents = list(self.old_agents_set.difference(self.new_agents_set))
        if delisted_agents:
            for index, agent_id in enumerate(delisted_agents):
                self.collection.find_one_and_update(filter={"_id": agent_id},
                                                    update={"$set": {'CurrentStatus': 'Delistsed'}})
                print(f"Delisted Item No {index} Updated and _ID is ---> ", agent_id)


    @staticmethod
    def establish_connection_and_return_collection(database_name, collection_name):
        # client_string = 'mongodb+srv://developer:0AdhL9ZlntIUzeQk7@cluster2.zynf1.mongodb.net/devwork'
        connection_string = 'mongodb://localhost:27017'
        client = MongoClient(connection_string)
        db = client[database_name]  # Enter the Database Name
        collection = db[collection_name]  # Enter the Table Name
        return collection

