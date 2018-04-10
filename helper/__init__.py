from .entity import EntityCollector
#from .mongo import MongoMan
from . import mongo

import configparser
import logging
import pymysql
import datetime
import pytz

def load_config(filename):
    cfg= configparser.ConfigParser()
    if not cfg.read(filename):
        raise SystemExit("Config file loading failed: " + filename)
    return cfg



def connect_mysql(cfg):
    mycnf = cfg['SOURCE_MYSQL']
    conn = pymysql.connect(host=mycnf['host'], port=int(mycnf['port']), user=mycnf['user'], password=mycnf['pass'],
                           db=mycnf['db'], charset='utf8', cursorclass=pymysql.cursors.SSDictCursor)
    return conn



KOREA_TZ = pytz.timezone("Asia/Seoul")

def native_to_utc(native_dt):
    if native_dt is None:
        return None
    local_dt = KOREA_TZ.localize(native_dt, is_dst=None)
    utc_dt = local_dt.astimezone(pytz.utc)
    return utc_dt
