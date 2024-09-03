from scrapy import Spider, Request, Selector
from selenium.common import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc
import time
from selenium.webdriver.support.wait import WebDriverWait


class CopartSpider(Spider):
    name = 'copart'
    base_url = 'https://www.copart.com/{}'
    login_url = 'https://www.copart.com/login/'
    driver = None
    custom_settings = {
        'FEED_URI': f'output/vin_numbers.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
    }

    def get_credentials(self):
        with open(f'input/copart_credentials.txt', 'r') as credentials:
            lines = credentials.readlines()
            user = ''
            pass_w = ''
            for line in lines:
                if 'username' in line:
                    user = line.replace('username =', '')
                if 'password' in line:
                    pass_w = line.replace('password =', '')
            return user.replace(' ', ''), pass_w.replace(' ', '')

    def start_requests(self):
        yield Request(url='https://quotes.toscrape.com/', callback=self.parse)

    def parse(self, response):
        username, password = self.get_credentials()
        self.driver = uc.Chrome()
        self.driver.get(self.login_url)
        try:
            WebDriverWait(self.driver, 20).until(
                EC.visibility_of_element_located((By.ID, 'username')))
            self.driver.find_element(By.ID, "username").send_keys(username)
            time.sleep(2)
            self.driver.find_element(By.ID, 'password').send_keys(password)
            time.sleep(2)
            self.driver.find_element(By.CSS_SELECTOR, 'button.loginfloatright').click()
        except NoSuchElementException:
            pass
        try:
            WebDriverWait(self.driver, 20).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, 'ul.watchlist_data li.view_all a')))
            self.driver.find_element(By.CSS_SELECTOR, 'ul.watchlist_data li.view_all a').click()
        except NoSuchElementException:
            pass
        time.sleep(10)
        page_html = self.driver.page_source
        selector_html = Selector(text=page_html)
        lot_url = selector_html.css('a.search-results::attr(href)').getall()
        time.sleep(10)
        check = 0
        try:
            if self.driver.find_element(By.XPATH, '//li[@class="paginate_button next"][1]/a'):
                check = 1
            while check == 1:
                self.driver.find_element(By.XPATH, '//li[@class="paginate_button next"][1]/a').click()
                time.sleep(15)
                page_html = self.driver.page_source
                selector_html = Selector(text=page_html)
                for url in selector_html.css('a.search-results'):
                    lot_url.append(url.css('::attr(href)').get())
                if self.driver.find_element(By.XPATH, '//li[@class="paginate_button next disabled"][1]/a'):
                    check = 0
        except NoSuchElementException:
            pass
        time.sleep(5)
        for url in lot_url:
            item = dict()
            self.driver.get(self.base_url.format(url))
            data = WebDriverWait(self.driver, 20).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, 'span#vinDiv span')))
            vin = data.text
            item['vin'] = vin
            yield item
