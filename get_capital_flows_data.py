'''
抓取资金流向数据
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








# 沪深A股资金流向连接
url = """
http://push2.eastmoney.com/api/qt/clist/
get?cb=jQuery112303439602113208118_1635594857492&fid=f62&po=1&pz=50&pn=1&np=1
&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2&fields=f12%2Cf14%2Cf2%2Cf3%2Cf62%2Cf184%2Cf66%2Cf69%2Cf72%2Cf75%2Cf78%2Cf81%2Cf84%2Cf87%2Cf204%2Cf205%2Cf124%2Cf1%2Cf13
"""


# 板块资金流

url = '''
http://push2.eastmoney.com/api/qt/clist/
get?cb=jQuery112307440708579704689_1635594089939&fid=f62&po=1&pz=50&pn=2&np=1
&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A90+t%3A2&fields=f12%2Cf14%2Cf2%2Cf3%2Cf62%2Cf184%2Cf66%2Cf69%2Cf72%2Cf75%2Cf78%2Cf81%2Cf84%2Cf87%2Cf204%2Cf205%2Cf124%2Cf1%2Cf13
'''


# 沪深A股主力排名
url = '''
http://push2.eastmoney.com/api/qt/clist/
get?cb=jQuery112305697970962671988_1635594352393&fid=f184&po=1&pz=50&pn=2&np=1
&fltt=2&invt=2&fields=f2%2Cf3%2Cf12%2Cf13%2Cf14%2Cf62%2Cf184%2Cf225%2Cf165%2Cf263%2Cf109%2Cf175%2Cf264%2Cf160%2Cf100%2Cf124%2Cf265%2Cf1&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2
'''