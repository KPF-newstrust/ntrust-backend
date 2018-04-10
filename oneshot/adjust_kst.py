#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import oneshot_common
import helper

import datetime
import logging

# 파이썬 시간을 MongoDB 에 그냥 넣을때 KST(GMT+9)가 반영되지 않는다.
# 그래서 이전에 MySQL에서 데이터 이전할때 날짜가 전부 +9시가 되었는데,
# 날짜 필드만 KST timezone 적용하여 MongoDB 쪽 업데이트하는 스크립트

configs = helper.load_config("../ntrust.ini")

helper.mongo.connect(configs["DEST_MONGODB"])

coll_cm2016 = helper.mongo.get_collection("cm2016")

# MySQL 원본 DB 에서 cm2016 콜렉션으로 날짜만 다시 복사 (KST 조정)
def convert_cm2016():
    from_date = datetime.date(2016,6,1)
    till_date = datetime.date(2016,7,1)

    conn_mysql = helper.connect_mysql(configs)
    with conn_mysql.cursor() as cursor:
        cursor.execute("SELECT NEWSITEM_ID,INSERT_DT,EMBARGO_DT,FIRST_APP_DT FROM _2016_CMS_NEWS WHERE INSERT_DT >= %s AND INSERT_DT < %s LIMIT 1", (from_date, till_date))
        rows = cursor.fetchall()
        for row in rows:
            newsId = row["NEWSITEM_ID"]
            insertDt = helper.native_to_utc(row["INSERT_DT"])
            embargoDt = helper.native_to_utc(row["EMBARGO_DT"])
            firstAppDt = helper.native_to_utc(row["FIRST_APP_DT"])
            coll_cm2016.update_one({"newsitem_id": newsId}, {"$set": {
                "insert_dt": insertDt,
                "embargo_dt": embargoDt,
                "first_app_dt": firstAppDt }}, upsert=False)
            print("Date converted:", newsId)
    conn_mysql.close()

# cm2016 콜렉션에서 news 콜렉션으로 날짜 복사
def convert_news():
    coll_news = helper.mongo.get_collection("news")
    dateDic = dict()
    docs = coll_news.find(None, {"newsId":1}).limit(1)
    for doc in docs:
        src = coll_cm2016.find_one({"newsitem_id":doc["newsId"]}, {"insert_dt":1})
        if not src["insert_dt"]:
            raise RuntimeError("insert_dt is None")
        dateDic[doc["_id"]] = src["insert_dt"]

    print("Got %d dates, len(docs)=%d" % (len(dateDic), len(docs)))
    for k,v in dateDic:
        coll_news.update_one({"_id":k}, {"$set": {"insertDt": v}}, upsert=False)
        print("Updated %s => %s" % (k, v))

    print("Done2")



convert_news()

helper.mongo.close()
print("Done")