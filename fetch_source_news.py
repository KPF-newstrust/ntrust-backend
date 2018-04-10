import sys
import logging
import pymysql
import datetime
import pymongo
import configparser

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s", datefmt='%Y-%m-%d %H:%M:%S')

configs = configparser.ConfigParser()
if not configs.read("ntrust.ini"):
    logging.error("Config file loading failed")
    sys.exit(1)

import site
site.addsitedir(configs['NTRUST_MODULE']['path'])
from ntrust.sanitizer.constants import PROVIDER_CODE

mgo_cli = None
mgo_db = None

def open_mongo():
    global mgo_cli, mgo_db
    mgcnf = configs['DEST_MONGODB']
    mgo_cli = pymongo.MongoClient(mgcnf['uri'])
    mgo_db = mgo_cli[mgcnf['db']]
    coll = mgo_db.cm2016
    coll.create_index([('news_id', pymongo.ASCENDING)], unique=True)
    coll2 = mgo_db.daystat
    coll2.create_index([('date', pymongo.ASCENDING)], unique=True)
    coll2.create_index([('day', pymongo.ASCENDING)], unique=True)
    logging.info("Successfully connected to MongoDB %s", mgcnf['uri'])


def lower_keys(x):
    if isinstance(x, list):
        return [lower_keys(v) for v in x]
    elif isinstance(x, dict):
        return dict((k.lower(), lower_keys(v)) for k, v in x.items())
    else:
        return x


# 소스 MySQL DB 에 접속해서 해당 날짜의 기사를 전부 읽어 MongoDB 에 저장한다.
def fetch_news_source(target_date):
    mycnf = configs['SOURCE_MYSQL']
    logging.info("Checking for source news for %s", target_date)
    conn = pymysql.connect(host=mycnf['host'], port=int(mycnf['port']),user=mycnf['user'], password=mycnf['pass'], db=mycnf['db'],
                           charset='utf8', cursorclass=pymysql.cursors.SSDictCursor, connect_timeout=99)

    coll = mgo_db.cm2016

    till_date = target_date + datetime.timedelta(days=1)
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM _2016_CMS_NEWS WHERE INSERT_DT >= %s AND INSERT_DT < %s", (target_date, till_date))
        rows = cursor.fetchall()
        logging.info("Source news for %s: %d fetched", target_date, len(rows))
        cnt_ins = 0
        cnt_dup = 0
        for row in rows:
            row = lower_keys(row)
            """
            ret = coll.replace_one({"news_id":row['newsitem_id']}, row, True)
            if ret.matched_count > 0:
                cnt_dup += 1
            else:
                cnt_ins += 1
            """    
            old = coll.find_one({"news_id":row['newsitem_id']})
            if old is None:
                ins_id = coll.insert_one(row)
                cnt_ins += 1
            else:
                cnt_dup += 1


        logging.info("=> %d inserted, %d duplicated", cnt_ins, cnt_dup)

    conn.close()



def calc_day_stats(target_date):
    coll_src = mgo_db.cm2016

    dt_from = datetime.datetime.combine(target_date, datetime.time.min)
    dt_to = dt_from + datetime.timedelta(days=1)

    # 해당 일자 전체 기사 수
    total_count = coll_src.count({ "insert_dt": { "$gte": dt_from, "$lt": dt_to } })

    # 언론사별 기사 수
    docs = coll_src.aggregate([
        { "$match": { "insert_dt": { "$gte": dt_from, "$lt": dt_to } } },
        { "$group": { "_id": "$cp_id", "count": { "$sum": 1 } } }
    ])
    cnt_by_cp = {}
    for doc in docs:
        cp_id = doc['_id'][2:]
        if cp_id not in PROVIDER_CODE:
            logging.error("Unknown CP id: %s", cp_id)
            cp_id = '?'+cp_id
        else:
            cp_id = PROVIDER_CODE[cp_id]
        count = doc['count']
        cnt_by_cp[cp_id] = count

    # TODO: 카테고리별 기사 수

    daycode = dt_from.strftime("%Y-%m-%d")
    stat = { "date": dt_from, "day":daycode, "total": total_count, "count_by_cp": cnt_by_cp }
    ret = mgo_db.daystat.replace_one({ "day": daycode }, stat, True)
    logging.info("Day %s stat: total=%d, numCP=%d", daycode, total_count, len(cnt_by_cp))


if __name__ == "__main__":
    open_mongo()
    for mday in range(1,30+1):
        target_day = datetime.date(2016,6,mday)
        fetch_news_source(target_day)
        calc_day_stats(target_day)
    mgo_cli.close()


