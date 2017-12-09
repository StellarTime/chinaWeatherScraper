# -*- coding: utf-8 -*-
"""Get historic weather data from www.tianqihoubao.com given list of cities.
"""

from bs4 import BeautifulSoup
from datetime import datetime
from dateutil.relativedelta import relativedelta
# from multiprocessing import pool
import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import threading
import time
import urllib.request
from updateCityList import updateCitiesStatus




def getUrlList(url_home, cities, write_result, file_out):
    """Get url list from the home page.

    url_home: str.
    cities: a list of cities.
    write_log: logical for writing log.
    """
    response = urllib.request.urlopen(url_home)
    page = response.read().decode('gb18030')
    soup = BeautifulSoup(page, 'html.parser')
    a_list = []
    dd = soup.find_all('div', class_='citychk')[0].find_all('dd')
    for d in dd:
        a_list += d.find_all('a')

    a_dict = {}
    for a in a_list:
        tmp_city = a.string.strip(' ')
        tmp_url = url_home + a['href'].split('/')[-1]
        a_dict[tmp_city] = tmp_url

    url_cities = pd.DataFrame(columns=['url'], index=a_dict.keys())
    for city in cities:
        for a_city in a_dict.keys():
            if str(a_city) in str(city):
                url_cities.loc[a_city] = a_dict[a_city]

    if write_result:
        url_cities.to_csv(file_out, index_label=False, encoding='utf-8')

    url_cities = url_cities.dropna(axis=0, how='any')
    return url_cities



def getCities(thread_id, i_url_cities, dirOut, dirRefOut, startMonth, endMonth):
    """Fuction for each thread, get data of all cities in the alotted url list"""
    err_cities = []
    no_data_cities = []
    browser = webdriver.Chrome(executable_path='/usr/local/bin/chromedriver')
    counter = i_url_cities.shape[0]
    for city, url_city in i_url_cities.iterrows():
        print('Thread {}: {} started ...'.format(thread_id, city))
        error, res = getCity(browser, url_city['url'], startMonth, endMonth)
        if not error:
            res = res.sort_values('date')
            if res.shape[0] > 0:
                res.to_csv(dirOut + city + '_' + startMonth + '_' + endMonth + '.csv', encoding='utf-8', index=False)
                # print('\n{} done.\nFile at: {}\n\n'.format(city, dirOut))
                print('Thread {}: {} done. ({}*{}) ({} of {})'.format(thread_id, city, res.shape[0], res.shape[1], i_url_cities.shape[0] - counter, i_url_cities.shape[0]))
            else:
                print('Thread {}: {} no data! ({} of {})'.format(thread_id, city, i_url_cities.shape[0] - counter, i_url_cities.shape[0]))
                no_data_cities.append(city)

        else:
            print('Thread {}: {} error connecting to url. ({} of {})'.format(thread_id, city, i_url_cities.shape[0] - counter, i_url_cities.shape[0]))
            err_cities.append(city)

        counter -= 1


    browser.quit()
    err_cities = pd.DataFrame(err_cities)
    err_cities.to_csv(dirRefOut + 'err_cities.csv', index=False)
    no_data_cities = pd.DataFrame(no_data_cities)
    no_data_cities.to_csv(dirRefOut + 'no_data_cities.csv', index=False)



def getCity(browser, url_city, startMonth, endMonth):
    """Get weather data for each city.

    startMonth, endMonth: '%Y%m'
    """
    month_list = []
    curr_month = datetime.strptime(startMonth, '%Y%m')
    end_month = datetime.strptime(endMonth, '%Y%m')
    while curr_month <= end_month:
        month_list.append(curr_month.strftime('%Y%m'))
        curr_month += relativedelta(months=+1)

    error = False
    res = pd.DataFrame(columns = ['date', 'weather_1', 'weather_2', 'high', 'low', 'wind'])
    for month in month_list:
        url_city_month = url_city[:-5] + '/month/{}.html'.format(month)
        error, tmp_res = getCityMonth(browser, url_city_month)
        if not error:
            res = res.append(tmp_res, ignore_index=True)

    return error, res



def getCityMonth(browser, url_city_month):
    """Weather data given city and month"""
    error = False
    try:
        browser.get(url_city_month)
    except:
        time.sleep(10)
        try:
            browser.get(url_city_month)
        except:
            print('Could not connect to {}. Quitting.'.format(url_city_month))
            error = True

    try:
        WebDriverWait(browser, 5).until(EC.visibility_of_element_located((By.XPATH, '//div[@id="bdshare"]')))
    except TimeoutException:
        print('Timed out loading {}'.format(url_city_month))
        error = True

    res = pd.DataFrame(columns = ['date', 'weather_1', 'weather_2', 'high', 'low', 'wind'])
    if not error:
        tr_list = browser.find_elements_by_xpath('//div[@id="content"]/table[@class="b"]/tbody/tr')
        for i in range(1, len(tr_list)):
            tr = tr_list[i]
            td_list = tr.find_elements_by_tag_name('td')
            tmp_date = datetime.strptime(td_list[0].text, '%Y年%m月%d日')
            tmp_weather_1 = td_list[1].text.split('/')[0].strip(' ')
            tmp_weather_2 = td_list[1].text.split('/')[1].strip(' ')
            tmp_high = td_list[2].text.split('/')[0].strip(' ').strip(' ')[:-1]
            tmp_low = td_list[2].text.split('/')[1].strip(' ').strip(' ')[:-1]
            tmp_wind = td_list[3].text
            res.loc[i - 1] = [tmp_date, tmp_weather_1, tmp_weather_2, tmp_high, tmp_low, tmp_wind]
            # print(str(tmp_date) + ' done.')

    return error, res










if __name__ == '__main__':
    startMonth = '201602'
    endMonth = '201612'
    # startMonth = '201501'
    # endMonth = '201512'

    dirOut = '/home/allen/Documents/timeSeriesCollector/data/1105/'
    dirRefOut = '/home/allen/Documents/timeSeriesCollector/ref/1105/'
    if not os.path.exists(dirOut):
        os.makedirs(dirOut)

    if not os.path.exists(dirRefOut):
        os.makedirs(dirRefOut)

    # cities = pd.read_excel('/home/allen/Documents/timeSeriesCollector/快车标准城市名-id.xlsx', sheet_name='城市大区省份优步')
    # cities = list(cities.loc[cities['麦肯锡分级'].isin(['一线城市', '二线城市'])]['城市'])
    # cities = list(pd.read_csv(dirRefOut + 'cities_incomplete.csv')['0'])
    cities = ['西宁', '拉萨', '兰州', '嘉峪关', '石河子', '银川', '郑州', '开封', '呼伦贝尔']
    print(cities)
    print(len(cities))
    input('Press enter to continue ...')

    # updateCitiesStatus(330, False)

    url_home = 'http://www.tianqihoubao.com/lishi/'
    # url_cities = getUrlList(url_home, cities, True, dirRefOut + 'url_cities.csv')
    url_cities = getUrlList(url_home, cities, False, 0)

    i = 0
    threadSize = 3
    while True:
        tmpThread = threading.Thread(target=getCities, args=(i, url_cities.iloc[(threadSize * i):(threadSize * i + threadSize), :], dirOut, dirRefOut, startMonth, endMonth))
        tmpThread.start()
        if (threadSize * i + threadSize) > len(url_cities):
            break

        i += 1

    # updateCitiesStatus(330, False)
