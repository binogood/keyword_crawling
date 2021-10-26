#!/usr/bin/env python
# coding: utf-8


import requests
import time
import numpy as np
import pandas as pd
import sqlalchemy as db
import pymysql

from datetime import date, datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
from urllib.parse import quote

from sqlalchemy import create_engine

from apscheduler.schedulers.blocking import BlockingScheduler
# from config import DATABASE


chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("disable-gpu")
chrome_options.add_argument('lang=ko_KR')
chrome_options.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36")
path = 'path지정'


DATABASE = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'test',
    'password': '********',
    'database': 'test',
    'charset': 'utf8mb4'
}


def crawling_data(keyword_list,today):
    driver = webdriver.Chrome(path + 'chromedriver',options=chrome_options)
    today = today.strftime('%Y%m%d')
    result = []
    for idx in range(len(keyword_list)):
        url = f'https://search.naver.com/search.naver?where=view&query=패션+{keyword_list[idx]}&sm=tab_opt&nso=so%3Ar%2Cp%3Afrom{today}to{today}%2Ca%3Aall&mode=normal&main_q=&st_coll=&topic_r_cat='

        driver.get(url)
        SCROLL_PAUSE_TIME = 2

        last_height = driver.execute_script("return document.body.scrollHeight")
        if len(driver.find_elements_by_class_name('bx')) == 0:
            result.append([today, keyword_list[idx], 0])
            continue

        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_PAUSE_TIME)
            new_height = driver.execute_script("return document.body.scrollHeight")

            if new_height == last_height:
                break

            last_height = new_height

        soup = BeautifulSoup(driver.page_source, 'lxml')
        food_list = soup.find_all("li", attrs={"class": "bx"})
        result.append([today, keyword_list[idx], len(food_list) - 4])

    columns = ['date', 'keyword', 'count']
    result_df = pd.DataFrame(result, columns=columns)
    result_s = result_df.sort_values(by=['count'], ascending=False).reset_index(drop=True)
    result_s['rank'] = [i for i in range(1, len(result_s) + 1)]
    
    return result_s


def input_data(day_df,keyword_df,engine):
    day_df['date'] = day_df['date'].apply(lambda x: pd.to_datetime(str(x), format='%Y-%m-%d'))
    day_df = day_df[['keyword','count','rank','date']]
    day_df.columns = ['keyword_id','count','rank','date']
    
    key_list = day_df['keyword_id'].unique()
    
    df_list = pd.DataFrame()
    for i in key_list:
        tmp_id = keyword_df[keyword_df['name'] == i].id
        tmp_np = tmp_id.to_numpy()
        tmp = day_df[day_df['keyword_id'] == i]
        tmp['keyword_id'] = tmp_np[0]
        df_list = pd.concat([df_list, tmp])

    df_list.to_sql('ranking', engine, if_exists='append', index=False)



def week_save_data(keyword_df,week_sql_df,engine):
    week_data = pd.DataFrame()

    for i in range(1,len(keyword_df) + 1):
        tmp = week_sql_df[week_sql_df['keyword_id'] == i]
        tmp['date'] = tmp['date'].apply(lambda x: pd.to_datetime(str(x), format='%Y-%m-%d'))
        tmp2 = tmp.drop(['rank','id','keyword_id'], axis=1)
        tmp2 = tmp2.set_index('date')
        tmp3 = tmp2.resample('W').sum()
        tmp3['keyword_id'] = i
        week_data = pd.concat([week_data, tmp3])
        
    week_data['date'] = week_data.index
    week_data = week_data.reset_index(drop=True)
    date_list = week_data['date'].unique()
    
    last_data = pd.DataFrame()

    for i in date_list:
        tmp = week_data[week_data['date'] == i]
        tmp_s = tmp.sort_values('count', ascending=False)
        tmp_s['rank'] = [i for i in range(1,len(tmp_s)+1)]

        last_data = pd.concat([last_data, tmp_s])
        
    last_data = last_data[['keyword_id','count','rank','date']]
    last_data = last_data.reset_index(drop=True)
    
    last_data.to_sql('week_ranking',engine,if_exists='append', index=False)


def save_data(DATABASE):
    today = datetime.today()
    DB_URL = f"mysql+pymysql://{DATABASE['user']}:{DATABASE['password']}@{DATABASE['host']}:{DATABASE['port']}/{DATABASE['database']}?charset=utf8"
    engine = create_engine(DB_URL, encoding = 'utf-8')
    
    sql = "select * from keywords"
    keyword_df = pd.read_sql_query(sql, engine)
    keyword_list = keyword_df['name'].values.tolist()
    
    day_df = crawling_data(keyword_list,today)
    input_data(day_df,keyword_df, engine)
    
    if today.isoweekday() == 7:
        date1 = today.strftime('%Y-%m-%d')
        date2 = (today - timedelta(days=6)).strftime('%Y-%m-%d')
        week_sql = f'select * from ranking where DATE(date) between "{date2}" and "{date1}";'
        week_sql_df = pd.read_sql_query(week_sql, engine)
        week_save_data(keyword_df, week_sql_df, engine)
        


sched = BlockingScheduler()
sched.add_job(save_data, 'cron', hour='23', minute='40', id="test_1", args=[DATABASE])
sched.start()


