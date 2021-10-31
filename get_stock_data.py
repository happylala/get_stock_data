
'''
抓取股票每日交易数据
'''


from typing import Tuple
from numpy import int64
import pandas as pd
import os
from pymysql.cursors import Cursor
import requests
import re
import json
import time
from datetime import datetime
import pymysql
from sqlalchemy import engine


def crawl_data(url, max_try = 15):
    status = False
    for i in range(max_try):
        try:
            content = requests.get(url, timeout = 10).text
            status = True
        except Exception as e:
            print('抓取出错，次数：', i + 1, '发生错误：', e)
            time.sleep(3)
    if status:
        return content
    else:
        raise ValueError('抓取数据失败！')


def get_trade_date_from_sina(stock_code = 'sh000001'):
    url = 'https://hq.sinajs.cn/rn=1634478575417&list=' + stock_code
    content = crawl_data(url)
    # content = content.encoding('gbk')
    trade_date = content.split(',')[-4]
    return trade_date


def get_stock_data_from_eastmoney(trade_date):
    raw_url = """
    http://66.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112406987729625554595_1634484106912
    &pn=%s&pz=20&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23&
    fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152
    """
    # 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php'\
    # '/Market_Center.getHQNodeData?page=%s&num=40&sort=changepercent&asc=0&node=hs_a&symbol=&_s_r_a=page'
    page = 1
    data = pd.DataFrame()
    while True:
        url = raw_url % page
        print('正在抓取第{}页数据'.format(page))
        content = crawl_data(url)
        content = re.findall('\((.*?)\)', content)
        # print(content[0].replace("\"data\":null", "\"data\":'null'"))
        content = eval(content[0].replace("\"data\":null", "\"data\":'null'"))['data']
        # print(str(content['diff']).replace('\'', '\"'))
        # break
        # print(content)
        if content == 'null' or content == '[]':
            print('数据已抓完！')
            break
        content = json.loads(str(content['diff']).replace('\'', '\"'))
        df = pd.DataFrame(content)
        # df['trade_date'] = pd.to_datetime(get_trade_date_from_sina() + ' ' + df['ticktime'])
        df['trade_date'] = pd.to_datetime(trade_date)
        data = data.append(df)
        page += 1
        # time.sleep(1)
    data['stock_code'] = data['f13'].map(lambda x : 'sz' if x == 0 else 'sh') + data['f12']
    col = ['trade_date', 'stock_code', 'f12','f14','f2', 'f15','f16','f17','f3','f4','f5','f6','f7','f8','f9','f18','f20','f21','f23','f26']
    rename_col = {
            'f2':'trade',
            'f3':'changepercent',
            'f4':'pricechange',
            'f5':'volume',
            'f6':'amount',
            'f7':'amplitude',
            'f8':'turnover',
            'f9':'dynamic_pe',
            'f12':'code',
            'f14':'name',
            'f15':'high',
            'f16':'low',
            'f17':'open',
            'f18':'presettlement',
            'f20':'aggregate_value',
            'f21':'market_value',
            'f23':'pb',
            'f26':'first_trade_date'
            }
    data = data[col].rename(columns = rename_col)
    return data


def put_data_into_hdf(file_name, data):
    symbol_lst = set(data['stock_code'].values)
    hdf = pd.HDFStore(file_name, mode = 'w')
    for symbol in symbol_lst:
        hdf.append(key = symbol, value = data[data['stock_code'] == symbol])
        # hdf[symbol] = data[data['symbol'] == symbol]
    hdf.close()


def write_data_into_mysql(data, table_name, host = 'localhost', user = 'root', password = 'root', port = 3306, db_instance = ''):
    db = pymysql.connect(host = host, user = user, password = password, port = port, db = db_instance)
    cursor = db.cursor()
    key = data.columns
    value = data.values.tolist()
    values = [tuple(d) for d in value]
    sql = 'replace into {table} values({flag})'.format(table = table_name, flag = ','.join(['%s'] * len(key)))
    cursor.executemany(sql, values)
    db.commit()
    cursor.close()


def main():
    trade_date = pd.to_datetime(get_trade_date_from_sina())
    if  trade_date == datetime.now().date():
        data = get_stock_data_from_eastmoney(trade_date)
        # 去掉当天停盘的股票。此外，df中有空值的话是写不入hdf的
        data['open'].fillna(0, inplace = True)
        data.replace(to_replace = '-', value = 0, inplace = True)
        data = data[data['open']  - 0 > 0.00001]
        data.to_csv('data/data_{}.csv'.format(trade_date.strftime('%Y%m%d')), index = False)
        write_data_into_mysql(data, 'stock_data', db_instance = 'stock_data')
        put_data_into_hdf('stock_data.h5', data)
        print('写入数据完毕！')
    else:
        print('今天不是交易日，不用抓取数据。')


if __name__ == '__main__':
    main()
