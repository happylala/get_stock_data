import pandas as pd
import requests
import json
import datetime
import random
import pymysql



def get_sh_stock_exchange_index(days = 1):
    raw_url = 'http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?_var=kline_%sqfq&param=%s,%s,,,%s,qfq&r=0.%s'
    stock_code = 'sh000001'
    ktype = 'day'
    days = days
    rand_num = random.randint(10**15, 10**16-1)
    url = raw_url % (ktype, stock_code, ktype, days, rand_num)
    content = requests.get(url).text
    content = content.split('=', maxsplit = 1)[-1]
    content = json.loads(content)
    df = pd.DataFrame(content['data']['sh000001']['day'], columns = ['trade_date', 'presettlement', 'trade', 'high', 'low', 'volume'])
    df.insert(1, 'stock_code', 'sh000001')
    return df


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
    # day_delta = (datetime.datetime.now().date() - datetime.date(1990, 1, 1)).days
    data = get_sh_stock_exchange_index()
    data.to_csv('sh000001.csv', index = False)
    write_data_into_mysql(data, 'shanghai_stock_exchange_index', db_instance = 'stock_data')
    print(data)


if __name__ == '__main__':
    main()
