import csv
from scrapy import Spider, Request, Selector
from selenium.common import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc
import time
from selenium.webdriver.support.wait import WebDriverWait


class CarfaxonlineSpider(Spider):
    name = 'carfaxonline'
    login_url = 'https://www.carfaxonline.com/login'
    driver = None
    custom_settings = {
        'FEED_URI': f'output/carfaxonline.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
        'FEED_EXPORT_FIELDS': ['Vin_Number', 'model_name', 'brand_title', 'service_records', 'Total Owners',
                               'Owner1 year purchased',
                               'Owner1 Type', 'Owner1 length of ownership', 'Owner1 Owned states/provinces',
                               'Owner1 miles driven per year', 'Owner1 Last odometer reading', 'Owner2 year purchased',
                               'Owner2 Type', 'Owner2 length of ownership', 'Owner2 Owned states/provinces',
                               'Owner2 miles driven per year', 'Owner2 Last odometer reading', 'Owner3 year purchased',
                               'Owner3 Type', 'Owner3 length of ownership', 'Owner3 Owned states/provinces',
                               'Owner3 miles driven per year', 'Owner3 Last odometer reading', 'Owner4 year purchased',
                               'Owner4 Type', 'Owner4 length of ownership', 'Owner4 Owned states/provinces',
                               'Owner4 miles driven per year', 'Owner4 Last odometer reading']
    }

    def get_credentials(self):
        with open(f'input/carfaxonline_credentials.txt', 'r') as credentials:
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
        original_window = self.driver.current_window_handle
        self.driver.get(self.login_url)
        WebDriverWait(self.driver, 20).until(
            EC.visibility_of_element_located((By.ID, 'username')))
        self.driver.find_element(By.ID, "username").send_keys(username)
        time.sleep(2)
        self.driver.find_element(By.ID, 'password-input').send_keys(password)
        time.sleep(2)
        self.driver.find_element(By.ID, 'login_button').click()
        vin_dict = self.get_vin_numbers()
        for vin in vin_dict:
            item = dict()
            vin_no = vin.get('vin', '')
            item['Vin_Number'] = vin_no
            try:
                input_vin = WebDriverWait(self.driver, 20).until(
                    EC.visibility_of_element_located((By.ID, 'vin')))
                input_vin.send_keys(vin_no)
            except NoSuchElementException:
                pass
            time.sleep(2)
            self.driver.find_element(By.ID, 'header_run_vhr_button').click()
            time.sleep(5)
            for window_handle in self.driver.window_handles:
                if window_handle != original_window:
                    self.driver.switch_to.window(window_handle)
                    break
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div#headerMakeModelYear')))
            html = self.driver.page_source
            self.driver.close()
            self.driver.switch_to.window(original_window)
            time.sleep(10)
            html_selector = Selector(text=html)
            item['model_name'] = html_selector.css('div#headerMakeModelYear::text').get('').strip()
            item['brand_title'] = html_selector.xpath('//span[contains(text(), "Branded Title")]/text()').get(
                '').replace(
                'Branded Title:', '').strip()
            item['service_records'] = html_selector.css('div#vhrHeaderRow1 span strong::text').get('').strip()
            owners_name = html_selector.xpath("//table[@id='summaryOwnershipHistoryTable']//tr[@class='secHdrRow']/th["
                                              "contains(@class,'ownerColumnTitle')]/img/following-sibling::text()").getall()
            purchased = []
            for purchase in html_selector.xpath('//table[@id="summaryOwnershipHistoryTable"]//tr[@class="summaryOdd" '
                                                'or @class="summaryEven"][1]/td[@class="statCol"]'):
                value = purchase.xpath('./div/text()').get()
                if value:
                    purchased.append(value)
                else:
                    purchased.append('---')
            type_owner = []
            for t_owner in html_selector.xpath('//table[@id="summaryOwnershipHistoryTable"]//tr[@class="summaryOdd" '
                                               'or @class="summaryEven"][2]/td[@class="statCol"]'):
                value = t_owner.xpath('./div/text()').get()
                if value:
                    type_owner.append(value)
                else:
                    type_owner.append('---')
            ownership_length = []
            for length in html_selector.xpath('//table[@id="summaryOwnershipHistoryTable"]//tr['
                                              '@class="summaryOdd" '
                                              'or @class="summaryEven"][3]/td[@class="statCol"]'):
                value = length.xpath('./div/text()').get()
                if value:
                    ownership_length.append(value)
                else:
                    ownership_length.append('---')
            state = []
            for owner_state in html_selector.xpath('//table[@id="summaryOwnershipHistoryTable"]//tr['
                                                   '@class="summaryOdd" or @class="summaryEven"][4]/td['
                                                   '@class="statCol"]'):
                value = owner_state.xpath('./div/text()').get()
                if value:
                    state.append(value)
                else:
                    state.append('---')
            miles = []
            for mile in html_selector.xpath('//table[@id="summaryOwnershipHistoryTable"]//tr[@class="summaryOdd" '
                                            'or @class="summaryEven"][5]/td[@class="statCol"]'):
                value = mile.xpath('./div/text()').get()
                if value:
                    miles.append(value)
                else:
                    miles.append('---')
            odometer_reading = html_selector.xpath('//table[@id="summaryOwnershipHistoryTable"]//tr['
                                                   '@class="summaryOdd" '
                                                   'or @class="summaryEven"][6]/td[@class="statCol"]/div/text()').getall()
            owner = 0
            i = 0
            for o_number in owners_name:
                if o_number:
                    owner_number = o_number.replace('\n', '').replace(' ', '')
                    if '1-2' in owner_number:
                        owner += 2
                        item['Owner1 year purchased'], item['Owner2 year purchased'] = purchased[i], purchased[i]
                        item['Owner1 Type'], item['Owner2 Type'] = type_owner[i], type_owner[i]
                        item['Owner1 length of ownership'], item['Owner2 length of ownership'] = ownership_length[i], \
                            ownership_length[i]
                        item['Owner1 Owned states/provinces'], item['Owner2 Owned states/provinces'] = state[i], state[
                            i]
                        item['Owner1 miles driven per year'], item['Owner2 miles driven per year'] = miles[i], miles[i]
                        item['Owner1 Last odometer reading'], item['Owner2 Last odometer reading'] = odometer_reading[
                            i], odometer_reading[i]
                    else:
                        item[f'{owner_number} year purchased'] = purchased[i]
                        item[f'{owner_number} Type'] = type_owner[i]
                        item[f'{owner_number} length of ownership'] = ownership_length[i]
                        item[f'{owner_number} Owned states/provinces'] = state[i]
                        item[f'{owner_number} miles driven per year'] = miles[i]
                        item[f'{owner_number} Last odometer reading'] = odometer_reading[i]
                        owner += 1
                    i += 1
            item['Total Owners'] = owner
            yield item

    def get_vin_numbers(self):
        with open(f'output/vin_numbers.csv', 'r', encoding='utf-8-sig') as file:
            return list(csv.DictReader(file))
