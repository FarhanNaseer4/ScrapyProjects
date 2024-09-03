import json
import scrapy
from scrapy import Selector
from selenium.common import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc
import time
from selenium.webdriver.support.wait import WebDriverWait


class DeloitteSpider(scrapy.Spider):
    name = 'deloitte'
    start_urls = ['https://dart.deloitte.com/USDART/home/publications/deloitte/accounting-spotlight']
    base_url = 'https://dart.deloitte.com{}'
    printer_view = 'https://dart.deloitte.com/wicket/resource/com.ovitas.deloitte.dartuicomponents.comp.PageHeadComp' \
                   '/resource/pdfjs-2.6.347/web/viewer-full-ver-1672959468000.html?file={}'
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
                  'application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/109.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
    }
    custom_settings = {
        'FEED_URI': f'output/deloitte.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
    }
    driver = None

    def start_requests(self):
        yield scrapy.Request(url=self.start_urls[0], callback=self.parse, headers=self.headers)

    def parse(self, response):
        for data in response.css('div.op-button-list a.op-button-list-item')[:3]:
            detail_url = data.css('::attr(href)').get()
            if detail_url:
                yield response.follow(url=detail_url, callback=self.parse_detail, headers=self.headers)

    def parse_detail(self, response):
        pdf_url = response.css('a.dart-xref-document::attr(href)').get('')
        if pdf_url:
            self.download_pdf(pdf_url)
        for details in response.css('section.dart-pgroup'):
            item = dict()
            minor_title = details.css('h3 span.dart-header-content-text::text').get('').strip()
            if 'Contacts' not in minor_title:
                item['Source'] = 'Deloitte'
                item['Major Title'] = response.css('div.dart-newsletter-heading-issue::text').get('').strip()
                item['Date'] = response.css('div.dart-newsletter-heading-date::text').get('').strip()
                item['Title'] = response.css('header.dart-dtl-topic-title span.dart-header-content-text::text').get(
                    '').strip()
                item['Minor Title'] = details.css('section.dart-pgroup h3 span.dart-header-content-text::text').get(
                    '').strip()
                text = ' '.join(para.css('::text').get('') for para in details.css('div.dart-paras div[name="p"]'))
                item['Text'] = text
                yield item

    def download_pdf(self, pdf_url):
        self.driver = uc.Chrome()
        self.driver.get(self.base_url.format(pdf_url))
        try:
            cookies = WebDriverWait(self.driver, 30).until(
                EC.visibility_of_element_located((By.ID, 'onetrust-accept-btn-handler')))
            cookies.click()
        except NoSuchElementException:
            pass
        html_page = self.driver.page_source
        selector_html = Selector(text=html_page)
        json_data = selector_html.xpath('//script[contains(text(), "pdfJsUrl")]/text()').get('')
        if json_data:
            loaded_json = json.loads(json_data)
            is_printable = loaded_json.get('isPrintable', '')
            if is_printable:
                try:
                    self.driver.get(self.printer_view.format(is_printable))
                    WebDriverWait(self.driver, 30).until(
                        EC.visibility_of_element_located((By.CSS_SELECTOR, 'div.textLayer')))
                    self.driver.find_element(By.ID, 'download').click()
                except NoSuchElementException:
                    pass
                time.sleep(5)
                self.driver.close()
