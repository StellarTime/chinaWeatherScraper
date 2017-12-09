# -*- coding: utf-8 -*-
"""Get historic weather data from www.weather.com given list of cities.
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



def incompleteCities(cities, dir_data):
    cities_incomplete = cities
    for root, dirs, files in os.walk(dir_data):
        for file_name in files:
            if file_name == '.DS_Store':
                continue

            d = pd.read_csv(root + file_name)
            city = file_name.split('_')[0]
            print(city, d.shape)
            cities_incomplete.remove(city)

    return cities_incomplete



def getCities(thread_id, cities, dir_out, start_month, end_month):
    """Scrape data for all cities in a list."""
    browser = webdriver.Chrome(executable_path='/usr/local/bin/chromedriver')
    first_time = True
    for city in cities:
        if first_time:
            first_time = False
            time_sleep = 30
        else:
            time_sleep = 10

        error, res = getCity(browser, time_sleep, city, start_month, end_month)
        if error:
            print('\nThread {}, city {}, error occurred.\n'.format(thread_id, city))
        else:
            res.to_csv(dir_out + '{}_{}_{}_weather.csv'.format(city, start_month, end_month), index=False)
            print('Thread {}, city {}, done.'.format(thread_id, city))



def getCity(browser, time_sleep, city, start_month, end_month):
    """Get weather data for one city."""
    url_home = 'https://weather.com/zh-CN/weather/monthly/l/CHXX0008:1:CH'
    error = False
    res = pd.DataFrame(columns=['data', 'high', 'low'])

    try:
        browser.get(url_home)
    except:
        time.sleep(20)
        try:
            browser.get(url_home)
        except:
            print('Could not connect to {}. Quitting.'.format(url_home))
            error = True
            return error, res

    time.sleep(time_sleep)
    # try:
    #     WebDriverWait(browser, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="APP"]/div/div[6]/div[2]/div/div/div[3]/span')))
    # except:
    #     print('Timed out loading {}'.format(url_home))
    #     error = True
    #     return error, res

    try:
        search_box = browser.find_element_by_xpath('//*[@id="APP"]/div/div[6]/div[1]/div/div[1]/div/div[1]/div/input')
        search_box.click()
        time.sleep(1)
        search_box.send_keys(city)
        time.sleep(2)
        print('City name entered.')
    except:
        try:
            time.sleep(3)
            search_box = browser.find_element_by_xpath('//*[@id="APP"]/div/div[6]/div[1]/div/div[1]/div/div[1]/div/input')
            search_box.click()
            time.sleep(1)
            search_box.send_keys(city)
            time.sleep(2)
            print('City name entered.')
        except:
            print('Error sending input.')
            error = True
            return error, res

    try:
        first_result = browser.find_element_by_xpath('//*[@id="APP"]/div/div[6]/div[1]/div/div[1]/div/div[2]/div[2]/div/ul/li[1]')
        first_result.click()
        print('City selected and clicked.')
    except:
        time.sleep(5)
        try:
            first_result = browser.find_element_by_xpath('//*[@id="APP"]/div/div[6]/div[1]/div/div[1]/div/div[2]/div[2]/div/ul/li[1]')
            first_result.click()
            print('City selected and clicked.')
        except:
            print('Error sending query.')
            error = True
            return error, res

    time.sleep(5)

    curr_month = datetime.strptime(start_month, '%Y%m')
    end_month = datetime.strptime(end_month, '%Y%m')
    while curr_month <= end_month:
        error, tmp_res = getCityMonth(browser, curr_month)
        if not error:
            res = res.append(tmp_res, ignore_index=True)
        curr_month += relativedelta(months=+1)


    return error, res




def getCityMonth(browser, curr):
    """Given city and month.

    curr is a datetime object.
    """
    error = False
    res = pd.DataFrame(columns=['date', 'high', 'low'])

    select = browser.find_element_by_id('month-picker')
    options = select.find_elements_by_tag_name('option')
    curr_string = '{}月 {}'.format(curr.month, curr.year)
    for op in options:
        if op.text == curr_string:
            op.click()
            print('Month clicked.')
            break

    time.sleep(3)

    try:
        span = browser.find_element_by_xpath('//*[@id="APP"]/div/div[7]/div[2]/div[3]/main/div[1]/span')
        day_cell_list = span.find_elements_by_xpath('//div[@classname="dayCell opaque"]')
        print('Day cells ({}) found.'.format(len(day_cell_list)))
    except:
        time.sleep(3)
        try:
            span = browser.find_element_by_xpath('//*[@id="APP"]/div/div[7]/div[2]/div[3]/main/div[1]/span')
            day_cell_list = span.find_elements_by_xpath('//div[@classname="dayCell opaque"]')
            print('Day cells ({}) found.'.format(len(day_cell_list)))
        except:
            error = True
            print('Error finding day cells.')
            return error, res


    skip_this = True
    for i in range(len(day_cell_list)):
        day_cell = day_cell_list[i]
        tmp_content = day_cell.text.split('\n')
        tmp_date = tmp_content[0]
        # tmp_high = int(tmp_content[1][:-1])
        # tmp_low = int(tmp_content[2][:-1])
        tmp_high = tmp_content[1][:-1]
        tmp_low = tmp_content[2][:-1]
        if tmp_date == '1':
            skip_this = False

        if not skip_this:
            tmp_full_date = curr.strftime('%Y-%m-') + datetime.strptime(tmp_date, '%d').strftime('%d')
            print(tmp_full_date)
            res.loc[i] = [tmp_full_date, tmp_high, tmp_low]

    # if res.shape[0] < 28:
    #     print('\n\nLess than 28 rows in result ?!')
    #     import pdb; pdb.set_trace()

    return error, res



if __name__ == '__main__':
    start_month = '201708'
    end_month = '201710'

    dir_out = '/Users/allenwang/myDoc/DiDi/causalImpact/weather/data2/'
    dir_ref = '/Users/allenwang/myDoc/DiDi/causalImpact/weather/ref/'
    if not os.path.exists(dir_out):
        os.makedirs(dir_out)

    if not os.path.exists(dir_ref):
        os.makedirs(dir_ref)


    cities = list(pd.read_csv(dir_ref + 'cities_next.csv')['city_long'])
    cities = incompleteCities(cities, dir_out)
    print(cities)
    input('Press enter to continue ...')

    i = 0
    thread_size = 5
    # thread_size = 100
    while True:
        tmpThread = threading.Thread(target=getCities, args=(i, cities[thread_size * i : thread_size * i + thread_size], dir_out, start_month, end_month))
        tmpThread.start()
        if (thread_size * i + thread_size) > len(cities):
            break

        i += 1
