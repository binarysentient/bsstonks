from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import csv
import time

SCREENSHOT_PATH = '/home/soyeb84/rbi_scraping_screenshot/'
DATA_WRITE_PATH = '/home/soyeb84/rbi_data/'



## Initiates the crawl based on following rule
# navigate to intended page
# .....
def initiate_rbi_crawl(driver):
    seed_url = 'https://www.rbi.org.in/Scripts/BS_NSDPDisplay.aspx?param=4'

    driver.get(seed_url)
    takeScreenshot(driver)
    #Assume the page is loaded now, click on 2021 with the assumption that it is unclicked
    #The steps below should be repeatable as we know the dates we want to extract
    col_headers = ['date',
         'week_1_end_date','week_1_yield',
         'week_2_end_date','week_2_yield',
         'week_3_end_date','week_3_yield',
         'week_4_end_date','week_4_yield',
         'week_5_end_date','week_5_yield',
         'week_6_end_date','week_6_yield']

    for year in range(2020, 2012, -1):
        result = crawl_for_year(year)
        with open(DATA_WRITE_PATH + '{}.csv'.format(year), 'w') as csvfile:  
            writer = csv.DictWriter(csvfile, fieldnames = col_headers)
            writer.writeheader()
            for data in result:
                writer.writerow(data)
        print('Finished crawling data for  the year {}'.format(year))
        print(result)


def crawl_for_year(year):
    
    print('Going to start crawling for year {}'.format(year))

    year_element = driver.find_element_by_css_selector('#btn{}'.format(year))
    year_element.click()
    time.sleep(3)    
    takeScreenshot(driver)
    # print(driver.page_source)
    all_months_selector = '//*[@id="{}0"]'.format(year)#'a#{}0'.format(year)
    results = []
    
    #Do we need this check, may be not at this stage?
    print(all_months_selector)
    all_months_element = driver.find_element_by_xpath(all_months_selector)
    all_months_element.click()
    takeScreenshot(driver)

    soup = BeautifulSoup(driver.page_source)
    all_months_table = soup.find('table', attrs={'class':'tablebg'})
    table_body = all_months_table.find('tbody')
    rows = table_body.find_all('tr')
    current_date = None
    
    links = []
    dates = []
    
    for row_id in range(0, len(rows), 2):
        date_string = rows[row_id].find_all('th')[0].text
        link = rows[row_id + 1].find_all('a')[0]['href']
        print('{} -> {}'.format(date_string,link))
        links.append(convert_to_ratio_and_rates_link(driver.current_url, link))
        dates.append(date_string)

    date_link_list = tuple(zip(dates, links))
    
    for index, tuples in enumerate(date_link_list):
        
        date = tuples[0]
        link = tuples[1]
        
        time.sleep(3)
        
        print('Navigating to {}'.format(date))
        driver.get(link)
        takeScreenshot(driver)

        print('Going to retrieve 91 day primary yield for {}'.format(date))
        result = get_91_day_primary_yield(driver.page_source)

        data_dict = {}
        data_dict['date'] = date
        
        for index, tuples in enumerate(result):
            data_dict['week_{}_end_date'.format(index + 1)] = tuples[0]
            data_dict['week_{}_yield'.format(index + 1)] = tuples[1]   
        
        # print(data_dict)
        results.append(data_dict)
    return results



# Returns a list of tuples in following format
# (('Jul. 3 2021', '3.14'),
#  ('Jun. 4 2020', '3.41'),
#  ('Jun. 11 2020', '3.40'),
#  ('Jun. 18 2020', '3.47'),
#  ('Jun. 25 2020', '3.47'),
#  ('Jul. 2 2020', '3.44'))
def get_91_day_primary_yield(html_doc):
    detail_soup = BeautifulSoup(html_doc)
    detail_table = detail_soup.find('table', attrs={'class':'tablebg'}).find('table')
    table_body = detail_table.find('tbody')
    rows = table_body.find_all('tr')
    current_year = ''
    previous_year = ''
    dates = []
    treasure_91_day_yield_value = []
    for row_id in range(1, len(rows)):
        #first row is always for the year, skip first column
        tds = rows[row_id].find_all('td')
        if row_id == 1:
            current_year = tds[1].text
            previous_year = tds[2].text
        elif row_id == 2:
            for col_id in range(0, (len(tds))):
                suffix = ''
                if col_id == 0:
                    suffix = previous_year
                else:
                    suffix = current_year
                dates.append(tds[col_id].text[:len(tds[col_id].text)] + ' ' +  suffix)
        elif len(tds) > 1: 
            if '91-Day Treasury Bill' in tds[0].text:
                for col_id in range(1, (len(tds))):
                    treasure_91_day_yield_value.append(tds[col_id].text)
                

    return tuple(zip(dates, treasure_91_day_yield_value))


def convert_to_ratio_and_rates_link(original_url, new_path):
    parts = new_path.split('&')
    new_url = original_url + '&' + parts[len(parts)-1]
    print(new_url)
    return new_url


def takeScreenshot(driver) :
    epoch_seconds = int(time.time())
    driver.save_screenshot(SCREENSHOT_PATH + '{}.png'.format(epoch_seconds))

chrome_options = Options()
chrome_options.add_argument("--window-size=1920,1080")

driver = webdriver.Remote("http://127.0.0.1:4444/wd/hub", chrome_options.to_capabilities())

# driver = webdriver.Remote(
#   command_executor='http://localhost:3000/webdriver', # uses headless browser by browserless.io - https://hub.docker.com/r/browserless/chrome
#   desired_capabilities=chrome_options.to_capabilities()
# )
initiate_rbi_crawl(driver)

driver.quit()
