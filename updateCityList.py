# -*- coding: utf-8 -*-
"""Update city list before running web scraper.

Already run tianqihoubao.py once to get weather data from www.tianqihoubao.com
How ever, the site did not have all the info. needed and several time-out exceptions occurred as well.
Hence the data needs to be checked, and a list of cities whose weather data are incorrect or incomplete should be geenerated.
"""

import os
import pandas as pd
import sys



def updateCitiesStatus(nrows, print_result):
    cities = pd.read_excel('/home/allen/Documents/timeSeriesCollector/快车标准城市名-id.xlsx', sheet_name='城市大区省份优步')
    cities = list(cities.loc[cities['麦肯锡分级'].isin(['一线城市', '二线城市'])]['城市'])

    path_in = '/home/allen/Documents/timeSeriesCollector/data/1103/'
    path_out = '/home/allen/Documents/timeSeriesCollector/ref/1103/'
    cities_complete = []
    for x, y, z in os.walk(path_in):
        for file_name in z:
            if file_name == '.DS_Store':
                continue

            d = pd.read_csv(path_in + file_name, encoding='utf8')
            if d.shape[0] > nrows:
                city = file_name.split('_')[0]
                cities_complete.append(city)

    cities_complete = pd.DataFrame(cities_complete)
    cities_complete.to_csv(path_out + 'cities_complete.csv', index=False, encoding='utf8')

    cities_incomplete = []
    for c in cities:
        ind_incomplete = True
        for cc in [a for aa in cities_complete.values.tolist() for a in aa]:
            if str(cc) in str(c):
                ind_incomplete = False

        if ind_incomplete:
            cities_incomplete.append(c)

    cities_incomplete = pd.DataFrame(cities_incomplete)
    cities_incomplete.to_csv(path_out + '/cities_incomplete.csv', index=False, encoding='utf8')

    if print_result:
        print(cities_complete.shape[0], cities_incomplete.shape[0])

    return cities_complete.shape[0], cities_incomplete.shape[0]



if __name__ == '__main__':
    if len(sys.argv)  == 2:
        updateCitiesStatus(int(sys.argv[1]), True)
    else:
        print('Error: No input argument!')
