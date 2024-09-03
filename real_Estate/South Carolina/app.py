import requests, json
from bs4 import BeautifulSoup
from lxml import etree
from datetime import datetime
from proxy import start_session

session = start_session("https://scor.sled.sc.gov/")
current_date = datetime.now()
payload={}
OFFENDER_CARD = {}

headers = {
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Cookie': 'ASP.NET_SessionId=trivixssa0wd35mufnacpvs3',
    'Host': 'scor.sled.sc.gov',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-user': '?1',
    'Sec-Fetch-Site': 'none',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
}


with open('/Users/ab/Desktop/Python/spetan/profiles.json') as json_file:
    data = json.load(json_file)
    

#for obj in data:   
for i in range(20):
    # Main
    url =  data[i]['profile link']
    #url =  obj['profile link']
    response = None 
    response = session.get(url, headers=headers,verify=False)
    while response is None or response.status_code != 200:
        response = session.get(url, headers=headers,verify=False)
    soup = BeautifulSoup(response.content, 'html.parser')
    dom = etree.HTML(str(soup))

    name = dom.xpath('//*[@id="aspnetForm"]/div[4]/div[2]/div[2]/div[1]')[0].text.strip()
    add = dom.xpath('//*[@id="aspnetForm"]/div[4]/div[2]/div[2]/div[7]/div[2]/text()')
    ress = dom.xpath('//*[@id="aspnetForm"]/div[4]/div[2]/div[2]/div[7]/div[3]/text()')
    if add and ress:
        address = add[0].strip() + ress[0].strip()
    else:
        address = None
    div_aliases = dom.xpath('//*[@id="aspnetForm"]/div[4]/div[2]/div[2]/div[5]/div')
    if div_aliases[1] == 'NoneType':
        aliases = []
        for i in range(1,len(div_aliases)):
            aliases.append(dom.xpath('//*[@id="aspnetForm"]/div[4]/div[2]/div[2]/div[5]/div')[i].text.strip())
    else:
        aliases = None
    county = dom.xpath('//*[@id="aspnetForm"]/div[4]/div[2]/div[2]/div[7]/div[4]/text()')
    if county:
        county = county[1].strip()
    else:
        county = None
    sex = dom.xpath('/html/body/form/div[4]/div[2]/div[2]/div[6]/div[2]/text()')[1].strip()
    race = dom.xpath('//*[@id="aspnetForm"]/div[4]/div[2]/div[2]/div[6]/div[3]/text()')[1].strip()
    date_of_birth = dom.xpath('/html/body/form/div[4]/div[2]/div[2]/div[6]/div[5]/text()')[1].strip().replace('/','-')
    height = dom.xpath('//*[@id="aspnetForm"]/div[4]/div[2]/div[2]/div[8]/div[2]/text()')[1].strip()
    weight = dom.xpath('//*[@id="aspnetForm"]/div[4]/div[2]/div[2]/div[8]/div[3]/text()')[1].strip()
    hair = dom.xpath('//*[@id="aspnetForm"]/div[4]/div[2]/div[2]/div[8]/div[4]/text()')[1].strip()
    eye = dom.xpath('//*[@id="aspnetForm"]/div[4]/div[2]/div[2]/div[8]/div[5]/text()')[1].strip()
    age = int((current_date - datetime.strptime(f'{date_of_birth}', '%m-%d-%Y')).days / 365)
    img_0 = 'https://scor.sled.sc.gov/' + soup.find('img',id='ctl00_ContentPlaceHolder1_OffenderDetails1_listImages_ctrl0_ImageIdLit')['src']
    img_1 = 'https://scor.sled.sc.gov/' + soup.find('img',id='ctl00_ContentPlaceHolder1_OffenderDetails1_listImages_ctrl1_ImageIdLit')['src']
    img_2 = 'https://scor.sled.sc.gov/' + soup.find('img',id='ctl00_ContentPlaceHolder1_OffenderDetails1_listImages_ctrl2_ImageIdLit')['src']
    img_3 = 'https://scor.sled.sc.gov/' + soup.find('img',id='ctl00_ContentPlaceHolder1_OffenderDetails1_listImages_ctrl3_ImageIdLit')['src']
    img_4 = 'https://scor.sled.sc.gov/' + soup.find('img',id='ctl00_ContentPlaceHolder1_OffenderDetails1_listImages_ctrl4_ImageIdLit')['src']
    image_urls = [img_0,img_1,img_2,img_3,img_4]
    offense = soup.find('table',id='ctl00_ContentPlaceHolder1_OffenderDetails1_offenses')
    if offense:
        tr = offense.find_all('tr')
        for i in range(1,len(tr)):
            td = tr[i].find_all('td')
            conviction_date = td[0].text
            conviction_state = td[1].text
            statut = td[2].text
            description = td[3].text
            details_link = 'https://scor.sled.sc.gov/' + td[3].find('a',href=True)['href']
            response = None 
            response = session.get(details_link, headers=headers,data=payload)
            while response is None or response.status_code != 200:
                response = session.get(details_link, headers=headers,data=payload)
            soup_0 = BeautifulSoup(response.content, 'html.parser')
            details = soup_0.find('span',id='txtDesc').text
            offenses = {
                'Description': description,
                'Date Convicted': conviction_date,
                'Conviction State': conviction_state,
                'Release Date': None,
                'Details': details,
                'County of Conviction': county,
                'Case Number': None,
                'Sentence': None,
                'State': conviction_state,
                'Probation Conditions': None
                        }
          
            
    # Scars
    
    url = data[i]['profile link'].replace('Main','Scars')
    #url = obj['profile link'].replace('Main','Scars')
    response = None
    response = session.get(url, headers=headers,data=payload,verify=False)
    while response is None or response.status_code != 200:
        response = session.get(url, headers=headers,data=payload,verify=False)
    soup = BeautifulSoup(response.content, 'html.parser')
    dom = etree.HTML(str(soup))
    
    table = soup.find('table', width="98%")
    if table:
        scars = []
        tr = table.find_all('tr')
        for i in range(1,len(tr)):
            td = tr[i].find_all('td')
            scars.append(td[1].text.strip() + ' / ' + td[2].text.strip())
    else:
        scars = None


    # OtherAddresses
    url = data[i]['profile link'].replace('Main','OtherAddresses')
    #url = obj['profile link'].replace('Main','OtherAddresses')
    response = None
    response = session.get(url, headers=headers, data=payload,verify=False)
    while response is None or response.status_code != 200:
        response = session.get(url, headers=headers, data=payload,verify=False)
    soup = BeautifulSoup(response.content, 'html.parser')
    dom = etree.HTML(str(soup))
    add = dom.xpath('//*[@id="aspnetForm"]/div[4]/div[2]/div[2]/div[4]/div[3]')
    ress = dom.xpath('//*[@id="aspnetForm"]/div[4]/div[2]/div[2]/div[4]/div[4]/text()')
    if add and ress:
        work_addresses = add[0].text.strip() + ress[0].strip()
    else:
        work_addresses = None


    basic_details = {
            'Name': name,
            'Registration #': None,
            'Last Verification Date': None,
            'Aliases': aliases,
            'Level': None,
            'Status': None,
            'Registrant Type': None,
            'Registration Start Date': None,
            'Registration End Date': None,
            'Lifetime Registration': None
            }

    physical_description = {
            'Age': age,
            'Date of Birth': date_of_birth,
            'Height': height,
            'Sex': sex,
            'Weight': weight,
            'Race': race,
            'Eyes': eye,
            'Hair': hair,
            'Scars/Tattoos': scars,
                        }

    addresses = {
            'Address': address,
            'Work Addresses': work_addresses,
            'School Addresses': None,
            'Volunteer Addresses': None,
            'Other Residential Addresses': None,
            }

    OFFENDER_CARD['offenses'] = offenses
    OFFENDER_CARD['basic_details'] = basic_details
    OFFENDER_CARD['physical_description'] = physical_description
    OFFENDER_CARD['addresses'] = addresses
    OFFENDER_CARD['image_urls'] = image_urls
    OFFENDER_CARD['age'] = age
    OFFENDER_CARD['date_of_birth'] = date_of_birth
    OFFENDER_CARD['date_of_birth_raw'] = None
    OFFENDER_CARD['state'] = None
    OFFENDER_CARD['warrants'] = []
    OFFENDER_CARD['comments'] = []
    print(OFFENDER_CARD)

    with open('/Users/ab/Desktop/Python/spetan/data_profile_test.json', 'a') as f:
        f.write(json.dumps(OFFENDER_CARD))
        f.write('\n')
        f.close()
        print('SUCCEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEESS !!!')