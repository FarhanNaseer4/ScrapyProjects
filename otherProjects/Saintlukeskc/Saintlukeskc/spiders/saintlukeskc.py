import json
from datetime import datetime

import scrapy


class SaintlukeskcSpider(scrapy.Spider):
    name = 'saintlukeskc'
    request_api = 'https://doctors.saintlukeskc.org/api/search?sort=relevance%2Cnetworks&page={}'
    zyte_key = '07a4b6f903574c1d8b088b55ff0265fc'
    base_url = 'https://doctors.saintlukeskc.org/'
    custom_settings = {
        'CRAWLERA_ENABLED': True,
        'CRAWLERA_APIKEY': zyte_key,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_crawlera.CrawleraMiddleware': 610
        },
        'FEED_URI': 'saintlukeskc.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
        'FEED_EXPORT_FIELDS': ['Business Name', 'Full Name', 'First Name', 'Last Name', 'Street Address',
                               'Valid To', 'State', 'Zip', 'Description', 'Phone Number', 'Phone Number 1', 'Email',
                               'Business_Site', 'Social_Media', 'Record_Type',
                               'Category', 'Rating', 'Reviews', 'Source_URL', 'Detail_Url', 'Services',
                               'Latitude', 'Longitude', 'Occupation',
                               'Business_Type', 'Lead_Source', 'State_Abrv', 'State_TZ', 'State_Type',
                               'SIC_Sectors', 'SIC_Categories',
                               'SIC_Industries', 'NAICS_Code', 'Quick_Occupation', 'Scraped_date', 'Meta_Description'],
        'HTTPERROR_ALLOW_ALL': True,
    }
    headers = {
        'authority': 'doctors.saintlukeskc.org',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cookie': 'visid_incap_2440428=oIZZ6ENhR5+F9j9WvEuYpzjxfWMAAAAAQUIPAAAAAAB5dEP6dzfDoM5CadkXecc2; _ga=GA1.2.478075120.1669198148; nmstat=a43cedd6-ff7b-0b83-d431-e1df022660e8; visid_incap_2834076=QhOQP1XESzunICv5rWS1wEbxfWMAAAAAQUIPAAAAAACb4TLcf+D8181orhP5M+px; consumer_user_token=b77f86fd-8cf9-46d3-ae89-a446c28c0f23; _gid=GA1.2.756854166.1669976311; consumer_tracking_token=52d51ba7-413c-498a-bf5d-a96fad69336b; search_shuffle_token=514fc84c-32ac-4857-8745-a9084ef2fb29; nlbi_2440428=WxytcPODjkGtsOf8opcjKAAAAAAR8iOagR9kb1cT4Gfkr6V/; incap_ses_219_2440428=rzDtKBiAWi/ROdvjsQsKA3/2imMAAAAAN/jM5FAgwK61arWzlO/Kww==; _gat=1; _dc_gtm_UA-157894082-3=1; incap_ses_1344_2834076=HMa7aS8ZZCYSrvondtmmEoT2imMAAAAAZReDdypFliiv8zHgXGqP3g==; reese84=3:PG5m9tASATnuSu5Zg6D0ag==:X2CVYajTRjU8ztJgQRhNTWiCAAYQIY2oI8MrcrQ0Cy8QMa3VtF8YGIaVNolJ2TfYbP0/fdAeVZi3JmP4snMlojS+3Dv1TeoZMAVBa4KPLDyiX2XJBFm/7Ys7HQwONjmycFCNuPcsBVq5/eYlIyIbERrSE8ZD/qc9dJObLEl4v+RGGI/mNefZ2VowBpV1K/A2il9MtH/nUmtSeSJnkT/KY86pezjdACITyxOfFA4SNsjIs2q1rtLuuDXi/IraF3c5Uze7f1aH/d1aDIasya40xaxhd/qpIWCJmMYF/dtPJd+9DcZe2HcCfsscjbF7na2lkbW94qA35wvTTIhw9/Gl7qdJCzsTAd0Z7oDUat5/1d41B8rcYWnQ1JEDtULUmXvZYGfzRDw7qRD2GQdM3s3kiRR+Z8Xe47qyyvZnbv5M96CbNheO7ddGeVJuMD3oF7pDqz/wCm5OpgU4dYBih/6U0vEc0GcxCTo6CStvOmYbzeA=:rSc7ZGPaF/5mcoD5ucwRMJvKWbZ/lwOReef9jy4/VA0=; nlbi_2834076=oSrle3ZSvmCJhnuW6VViOgAAAADk0srR8E4iDeH0yuoHqxqU; incap_ses_513_2834076=Uf2ifTeLQAk7+A7QmYseB5H2imMAAAAASBgVnDE+97OLYXL+TlDGFQ==; _gat_kyruusTracker=1; nlbi_2834076_2147483392=ru3CWbBaSC8k+GHQ6VViOgAAAABS/v2t5NOelFZBm0EMVwBd; consumer_tracking_token=52d51ba7-413c-498a-bf5d-a96fad69336b; consumer_user_token=b77f86fd-8cf9-46d3-ae89-a446c28c0f23; search_shuffle_token=514fc84c-32ac-4857-8745-a9084ef2fb29',
        'referer': 'https://doctors.saintlukeskc.org/search?sort=relevance%2Cnetworks&page=2',
        'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'x-csrf-header': 'saintlukes'
    }

    def start_requests(self):
        page = {'page_num': 1}
        yield scrapy.Request(url=self.request_api.format(1), callback=self.parse,
                             headers=self.headers, meta={'page_no': page})

    def parse(self, response):
        try:
            old_page = response.meta['page_no']
            json_data = json.loads(response.body)
            for data in json_data.get('data', {}).get('providers', []):
                item = dict()
                record_type = data.get('provider_type', '')
                id_name = data.get('name', {}).get('full_name', '')
                name = data.get('name', {})
                if "Group / Clinic" == record_type:
                    item['Business Name'] = name.get('full_name', '')
                else:
                    item['Full Name'] = name.get('full_name', '')
                    item['First Name'] = name.get('first_name', '')
                    item['Last Name'] = name.get('last_name', '')
                contact = data.get('contacts', [])
                if contact:
                    item['Phone Number'] = contact[0].get('value', '')
                location = data.get('locations', [])
                if location:
                    item['Street Address'] = location[0].get('street1', '')
                    item['State'] = location[0].get('state', '')
                    item['Zip'] = location[0].get('zip', '')
                p_id = data.get('id', '')
                if p_id:
                    item['Detail_Url'] = self.base_url + str(id_name.replace(' ', '+')) + '/' + str(p_id)
                item['Source_URL'] = 'https://doctors.saintlukeskc.org/'
                item['Lead_Source'] = 'saintlukeskc'
                item['Meta_Description'] = ""
                item['Occupation'] = record_type
                item['Record_Type'] = 'Business/Person'
                item['Scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                yield item
            total_page = json_data.get('data', {}).get('total_pages', '')
            old_no = old_page.get('page_num', '')
            new_page = old_no + 1
            if new_page <= total_page:
                old_page['page_num'] = new_page
                yield scrapy.Request(url=self.request_api.format(new_page), callback=self.parse,
                                     headers=self.headers, meta={'page_no': old_page})
        except Exception as ex:
            print('Error No Json | ' + str(ex))
