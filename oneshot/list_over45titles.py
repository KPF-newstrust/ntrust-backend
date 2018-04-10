#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import oneshot_common
import helper

import re
import datetime
import logging
import xlsxwriter

# 45자가 넘는 제목을 뽑아본다.

configs = helper.load_config("../ntrust.ini")

helper.mongo.connect(configs["DEST_MONGODB"])

coll_news = helper.mongo.get_collection("news")

def get_over45_titles():
    over45s = list()
    docs = coll_news.find({"$where":"this.title.length > 45"}, {"title":1, "newsId":1})#.limit(100)
    for doc in docs:
        over45s.append(doc)

    return over45s



def make_xlsx(overs):
    workbook = xlsxwriter.Workbook('over45titles.xlsx')
    sheet = workbook.add_worksheet("제목길이 45초과")
    sheet.write(0,0, '제목')
    sheet.write(0,1, '길이')
    sheet.write(0,2, 'news id')

    row = 1
    for doc in overs:
        sheet.write(row, 0, doc["title"])
        sheet.write(row, 1, len(doc["title"]))
        sheet.write(row, 2, doc["newsId"])
        row += 1

    workbook.close()
    print("XLSX created: %d rows" % (row))


if __name__ == "__main__":
    overs = get_over45_titles()
    make_xlsx(overs)
    #for title in overs:
    #    print(title)
    #print("Done")


helper.mongo.close()
