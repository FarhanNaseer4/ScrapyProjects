import csv
import json

import scrapy


class Scraper2Spider(scrapy.Spider):
    name = 'scraper_2'
    detail_base = 'https://www.indeed.com/viewjob?{}'
    custom_settings = {
        'FEED_URI': f'Scraper_2_Data.xlsx',
        'FEED_FORMAT': 'xlsx',
        'FEED_EXPORTERS': {'xlsx': 'scrapy_xlsx.XlsxItemExporter'},
        'FEED_EXPORT_ENCODING': 'utf-8',
        'FEED_EXPORT_FIELDS': ['Keyword', 'Search City', 'Job Title', 'Poster/company', 'Salary/wage rate', 'city',
                               'state', 'zipcode',
                               'RN license', 'Certifications', 'Skills', 'Specialities',
                               'Qualifications', 'Responsibilities', 'Job Detail Url'],
        'DOWNLOADER_MIDDLEWARES': {'scrapy_zyte_smartproxy.ZyteSmartProxyMiddleware': 610},
        'ZYTE_SMARTPROXY_ENABLED': True,
        'ZYTE_SMARTPROXY_APIKEY': 'bbe5eb726dcb4efba66324c3e4dce259',
        'X-Crawlera-Region': 'US',
    }

    # excel_file = 'Scraper_1_Output.xlsx'
    headers = {
        'authority': 'www.indeed.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cache-control': 'no-cache',
        'cookie': 'CTK=1h0i28tquholp800; PPID=""; __cf_bm=FYPHoEXPPQM8SNO2FqIrFUtB1Jp41AWjFzg4UdJCA5E-1688963664-0-AS4ZEv0JuUCjzkMHwje18vCEMCMDvMIsBqrDtj6dJanbsptciUEHDtpOk4wSkg7jMJ9j2QYf5UF0MzXOWHBEIKI=; _cfuvid=v3njtQOddonyfx7nkmSmmVVo_NFHrqyo5_T_N6ZA.XA-1688963664497-0-604800000; CSRF=4McAOrk3SI6uhCSR2gdjL6FlfgkhTzQ1; INDEED_CSRF_TOKEN=QPJocs4dbht54MU1qsogcjqI3te1kh4H; LV="LA=1688963665:CV=1688963665:TS=1688963665"; hpnode=1; _gid=GA1.2.226577698.1688963670; SURF=Nk7IE2hBkNVgIWdoZyj9hs0PArNX9xHN; CO=PK; RF="TFTzyBUJoNr6YttPP3kyivpZ6-9J49o-Uk3iY6QNQqKE2fh7FyVgtZT-hVucuZ8SOEqLW4lcgPA="; CTK=1h0i28tquholp800; _gcl_au=1.1.393825838.1688963694; _mkto_trk=id:699-SXJ-715&token:_mch-indeed.com-1688963695130-98485; NCR=1; _ga_5KTMMETCF4=GS1.1.1688963694.1.0.1688963699.0.0.0; indeed_rcc="PREF:LV:CTK:CO:UD"; _ga=GA1.2.25336558.1688963670; gonetap=3; g_state={"i_p":1688970905764,"i_l":1}; PREF="TM=1688963737977:LD=en:L=Wilmington%2C"; jaSerpCount=1; UD="LA=1688963737:CV=1688963737:TS=1688963737:SG=3d4b743d5bb805eed54d0a02b8824e44"; RQ="q=nu&l=Wilmington%2C+&ts=1688963738016"; JSESSIONID=4A4F7CFAD892D518A95FCFBA34FC4AAC; ac=PGgX8B7bEe6aZa11oSnPnQ#PG4LYB7bEe6aZa11oSnPnQ; _gat=1; _gali=jobsearch; PTK=tk=1h4v1aqbqlekg800&type=jobsearch&subtype=topsearch; __cf_bm=A_1FgycCjrAVJ5H6rPJt.ImMASk0K7eDYJKlOxdRmDI-1688964707-0-AabPzHlymNZ5K4HBfA7GjIWUgRELet3O4U3t9QMAsPJpGprXz1iX4jsM09WNf3afB7lcOzWpDGEzkxFeB97Zrys=',
        'pragma': 'no-cache',
        'referer': 'https://www.indeed.com/jobs?q=nu&l=Wilmington%2C%20&from=searchOnHP',
        'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
    }
    url = "https://www.indeed.com/jobs?q={}&l={}&vjk=51e4c487a3d8ca57"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.serial = self.get_serials()

    def get_serials(self):
        with open(f'input/scraper_1_input.csv', 'r') as csv_file:
            return list(csv.DictReader(csv_file))

    def start_requests(self):
        for terms in self.serial:
            keyword = terms.get('Keyword', '').strip().replace(' ', '+')
            city = terms.get('City', '').replace(',', '%2C').strip().replace(' ', '+')
            yield scrapy.Request(url=self.url.format(keyword, city), headers=self.headers, dont_filter=True,
                                 meta={'search': terms})

    def parse(self, response):
        terms = response.meta['search']
        script_data = response.css('script#mosaic-data::text').get('')
        if 'window.mosaic.providerData["mosaic-provider-jobcards"]=' in script_data:
            json_data = script_data.split('window.mosaic.providerData["mosaic-provider-jobcards"]=')[-1]
            main_json = \
                json_data.split('window.mosaic.providerData["mosaic-provider-passport-intercept"]=')[0].strip().rsplit(';',
                                                                                                                       1)[
                    0]
            loaded_json = json.loads(main_json)
            get_data = loaded_json.get('metaData', {}).get('mosaicProviderJobCardsModel', {}).get('results', [])
            for data in get_data:
                item = dict()
                item['Keyword'] = terms.get('Keyword', '')
                item['Search City'] = terms.get('City', '')
                item['Job Title'] = data.get('title', '')
                item['Poster/company'] = data.get('company', '')
                item['Salary/wage rate'] = data.get('estimatedSalary', {}).get('formattedRange', '')
                item['city'] = data.get('jobLocationCity', '')
                item['state'] = data.get('jobLocationState', '')
                item['zipcode'] = data.get('jobLocationPostal', '')
                item['Specialities'] = ', '.join(special for special in data.get('taxoAttributes', []))
                detail_url = data.get('viewJobLink', '')
                link_slug = detail_url.split('?')[-1]
                if link_slug:
                    yield scrapy.Request(url=self.detail_base.format(link_slug), callback=self.parse_details
                                         , headers=self.headers, meta={'item': item}, dont_filter=True)
            next_page = response.css('a[aria-label="Next Page"]::attr(href)').get()
            if next_page:
                yield response.follow(url=next_page, callback=self.parse, headers=self.headers, dont_filter=True,
                                      meta={'search': terms})

    def parse_details(self, response):
        item = response.meta['item']
        rate = item.get('Salary/wage rate', '')
        if not rate:
            item['Salary/wage rate'] = response.css('div#salaryInfoAndJobType span.eu4oa1w0::text').get('').strip()
        item['Skills'] = ''.join(
            response.xpath('//p[b[contains(text(),"Skills")]]/following-sibling::ul[1]/li/text() | '
                           '//*[contains(text(),"Requirements")]/following-sibling::p/text() | '
                           '//p[b[contains(text(),'
                           '"REQUIREMENTS")]]/following-sibling::ul[1]/li/text() | //*[contains('
                           'text(),"Skills")]/following-sibling::p/text()').getall())
        item['Qualifications'] = ''.join(response.xpath(
            '//*[*[contains(text(),"Education")]]/following-sibling::p/text() | //*[b[contains(text(),'
            '"EDUCATION")]]/following-sibling::ul[1]/li/text() | //*[*[contains(text(),'
            '"Qualifications")]]/following-sibling::p/text() | //*[b[contains(text(),'
            '"Qualifications")]]/following-sibling::ul[1]/li/text() | //p[contains(text(),"Qualification")]/text('
            ')').getall())
        item['Certifications'] = ','.join(
            response.xpath('//*[contains(text(),"Certification")]/following-sibling::ul[1]'
                           '/li/text()').getall())
        item['Responsibilities'] = ''.join(response.xpath(
            '//h2[*[contains(text(),"Duties")]]/following-sibling::p/text() | //*[b[contains(text(),'
            '"RESPONSIBILITIES")]]/following-sibling::ul[1]/li/text() | //*[b[contains(text(), '
            '"Responsibilities")]]/following-sibling::ul[1]/li/text() | //p[contains(text(), '
            '"Responsibilities")]/text()').getall())
        item['Job Detail Url'] = response.url
        yield item
