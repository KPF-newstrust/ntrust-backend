#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import oneshot_common
import helper

import re
import datetime
import logging
import xlsxwriter

# MongoDB ntrust2 db 의 news 콜렉션에서 bylines 의 name 필드를 조사하여
# "~ 기자" 형식이 아닌 바이라인을 취합한다.

configs = helper.load_config("../ntrust.ini")

helper.mongo.connect(configs["DEST_MONGODB"])

coll_news = helper.mongo.get_collection("news")

reGIJA = re.compile(r"(기자|논설위원|논설실장)$")

def get_nongija_bylines():
    unique_names = dict()
    docs = coll_news.find({"bylines":{"$exists":True}},{"bylines":1, "_id":0})#.limit(100000)
    for doc in docs:
        for byline in doc["bylines"]:
            if "name" not in byline:
                continue
            name = byline["name"]
            if len(name) <= 3:
                continue
            if not reGIJA.search(name):
                if name in unique_names:
                    unique_names[name] += 1
                else:
                    unique_names[name] = 1

    return unique_names



def make_xlsx(bylines):
    workbook = xlsxwriter.Workbook('nongija.xlsx')
    sheet = workbook.add_worksheet("기자가 아닌 실명")
    sheet.write(0,0, 'non-gija')
    sheet.write(0,1, 'count')

    row = 1
    for k,v in bylines.items():
        sheet.write(row, 0, k)
        sheet.write(row, 1, v)
        row += 1

    workbook.close()
    print("XLSX created: %d rows" % (row))


if __name__ == "__main__":
    bylines = get_nongija_bylines()
    make_xlsx(bylines)
    #for k in sorted(bylines, key=lambda k: len(k), reverse=True):
    #    print(k, bylines[k])
    #print("Done")


helper.mongo.close()
