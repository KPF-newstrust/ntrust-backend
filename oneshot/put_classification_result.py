#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import oneshot_common
import helper

import datetime
import logging
import os
import re

# 기사 카테고리 예측 머신러닝 결과를 news 콜렉션에 넣는다.

RE_PROBABILITY = re.compile(r"\[\{'(.+?)':: (.+?)\}, \{'(.+?)':: (.+?)\}\]")

NewsCates = dict()

def parse_csv(pathname):
    inf = open(pathname)
    for line in inf.readlines():
        toks = line.split("|")
        if toks[0] in NewsCates:
            raise RuntimeError("duplicated news id: " + toks[0])
        m = RE_PROBABILITY.match(toks[2])
        if not m:
            raise RuntimeError("invalid probability spec: " + toks[2])

        NewsCates[toks[0]] = [ toks[1], [m.group(1), float(m.group(2))], [m.group(3), float(m.group(4))] ]

for (dirpath, dirnames, filenames) in os.walk("../data/etc_cates_with_edu"):
    for filename in filenames:
        if not filename.endswith(".csv"):
            continue
        pathname = os.path.join(dirpath, filename)
        parse_csv(pathname)


print("Total news: %d" % len(NewsCates))

configs = helper.load_config("../dev.ini")

helper.mongo.connect(configs["DEST_MONGODB"])

coll_news = helper.mongo.get_collection("news")

for k,v in NewsCates.items():
    cateCalc = v.pop(0)
    coll_news.update_one({"newsId":k}, {"$set":{"categoryCalc":cateCalc, "categoryCalc_p":v}}, upsert=False)

helper.mongo.close()

print("Done")
