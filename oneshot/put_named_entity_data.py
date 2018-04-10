#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import oneshot_common
import helper

import datetime
import logging
import os
import re

# 기사 Named Entity Recognition 머신러닝 결과를 news 콜렉션에 넣는다.

RE_PROBABILITY = re.compile(r"\[\{'(.+?)':: (.+?)\}, \{'(.+?)':: (.+?)\}\]")

NewsDic = dict()

def parse_csv(pathname):
    inf = open(pathname)
    for line in inf.readlines():
        toks = line.rstrip().split("|")
        if toks[0] in NewsDic:
            raise RuntimeError("duplicated news id: " + toks[0])

        ne_person = toks[1].split(",")
        ne_organ = toks[2].split(",")
        ne_location = toks[3].split(",")
        NewsDic[toks[0]] = [ ne_person, ne_organ, ne_location ]

for (dirpath, dirnames, filenames) in os.walk("../data/ner"):
    for filename in filenames:
        if not filename.endswith(".csv"):
            continue
        pathname = os.path.join(dirpath, filename)
        parse_csv(pathname)


print("Total news: %d" % len(NewsDic))

configs = helper.load_config("../dev.ini")

helper.mongo.connect(configs["DEST_MONGODB"])

coll_news = helper.mongo.get_collection("news")

for k,v in NewsDic.items():
    coll_news.update_one({"newsId":k}, {"$set":{ "ner_PS":v[0], "ner_OG":v[1], "ner_LC":v[2] }}, upsert=False)

helper.mongo.close()

print("Done")
