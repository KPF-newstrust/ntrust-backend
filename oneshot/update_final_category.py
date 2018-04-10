#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import oneshot_common
import helper

import datetime
import logging

# 최종 카테고리를 설정한다. 우선순위: 1.Man, 2.Calc, 3.Xls -> categoryFinal

configs = helper.load_config("../ntrust.ini")

helper.mongo.connect(configs["DEST_MONGODB"])

coll_news = helper.mongo.get_collection("news")

docs = coll_news.find({}, {"categoryXls":1, "categoryMan":1, "categoryCalc":1})#.limit(1000)
for doc in docs:
    final = doc["categoryXls"]
    calc = None
    if "categoryCalc" in doc:
        calc = doc["categoryCalc"]
        final = calc
    man = None
    if "categoryMan" in doc:
        man = doc["categoryMan"]
        final = man

    if len(final) <= 1:
        raise RuntimeError("Invalid final category:"+final)

    #print("Final category: (Xls %s, Calc %s, Man %s) => %s" % (doc["categoryXls"],calc,man,final))
    coll_news.update_one({"_id": doc["_id"]}, {"$set": {"categoryFinal": final}}, upsert=False)

helper.mongo.close()
print("Done")
