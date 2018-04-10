#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import oneshot_common
import helper
import xlrd
import re

import site
site.addsitedir("../../ntrust-worker")
from ntrust import byline

import datetime
import logging

# 기사 평가하기 데이터의 바이라인을 수동으로 설정한다.

configs = helper.load_config("../ntrust.ini")

RE_GET_NEWSID = re.compile(r"^http://ntadm\.solif\.kr/admin/assess/view\?nws=(.+?)\.(.+?)$")

def get_byline_score(_sheet,_row,idx):
    _3 = _sheet.cell(_row, idx+0).value.strip()
    _2 = _sheet.cell(_row, idx+1).value.strip()
    _1 = _sheet.cell(_row, idx+2).value.strip()
    _0 = _sheet.cell(_row, idx+3).value.strip()

    if len(_3) > 0:
        return 1
    if len(_2) > 0:
        return 0.8
    if len(_1) > 0:
        return 0
    if len(_0) > 0:
        return -1

    raise RuntimeError("Invalid byline score")

XLS_ROWS = list()

wb = xlrd.open_workbook("../data/byline_20171106.xlsx")
for sheet in wb.sheets():
    for row in range(2, sheet.nrows):
        solif_url = sheet.cell(row, 0).value.strip()
        m = RE_GET_NEWSID.match(solif_url)
        if not m:
            raise RuntimeError("Invalid solif url: " + solif_url)

        media_id = m.group(1)
        news_id = m.group(1) + "." + m.group(2)
        #print("%s => %s*%s" % (solif_url, news_id, media_id))

        db_val = get_byline_score(sheet, row, 1)
        real_val = get_byline_score(sheet, row, 5)

        raw_byline = sheet.cell(row, 9).value.strip()
        #print("db=%d, real=%d, %s" % (db_val, real_val, raw_byline))

        XLS_ROWS.append({"url":solif_url, "media_id":media_id, "news_id":news_id, "score_byline_real":real_val, "raw_byline":raw_byline})


helper.mongo.connect(configs["DEST_MONGODB"])

coll_news = helper.mongo.get_collection("news")
coll_assess = helper.mongo.get_collection("asStats")

for row in XLS_ROWS:
    doc = coll_news.find_one({"newsId":row["news_id"]}, {"score.byline":1, "bylines":1})

    target_score_byline = row["score_byline_real"]
    if target_score_byline == -1:
        #print("DELETE byline: %s (%f vs %f : %s)" % (row["url"], db_score_byline, target_score_byline, row["raw_byline"]))
        #coll_news.update_one({"newsId":row["news_id"]}, {"$unset":{"bylines":1}}, upsert=False)
        continue

    db_score_byline = doc["score"]["byline"]
    if db_score_byline == target_score_byline:
        continue

    bylines = byline.BylineExtractor(row["raw_byline"]).get_result()
    recalc_score = -1
    has_name = "name" in bylines
    has_email = "email" in bylines
    if has_name and has_email:
        recalc_score = 1
    elif has_name:
        recalc_score = 0.8
    elif has_email:
        recalc_score = 0

    if recalc_score != target_score_byline:
        print("Differ: %s (db=%f, real=%f) %s" % (row["url"], db_score_byline, target_score_byline, row["raw_byline"]))
    else:
        print("Updated as %s" % (bylines))
        coll_news.update_one({"newsId": row["news_id"]}, {"$set": {"bylines": [bylines]}}, upsert=False)

helper.mongo.close()

print("Done")
