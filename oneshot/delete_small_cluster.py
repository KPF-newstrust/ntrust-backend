#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import oneshot_common
import helper

import datetime
import logging
import os
import re

configs = helper.load_config("../dev.ini")

helper.mongo.connect(configs["DEST_MONGODB"])

coll_news = helper.mongo.get_collection("news")


docs = coll_news.find({"clusterDelegate":True}, {"newsId":1})#.limit(10)
for doc in docs:
    #print(doc)    #print("Final category: (Xls %s, Calc %s, Man %s) => %s" % (doc["categoryXls"],calc,man,final))

    newsId = doc['newsId']
    count = coll_news.count({"clusterNewsId":newsId}, {})
    if count < 3:
        print(newsId, ':', count)
        #unset clusterDelegate
        coll_news.update_one(
            {"newsId":newsId},
            {"$unset":{"clusterDelegate":""}}
        )

        #unset clusterId, clusterNewsId, clusterSimilarity
        coll_news.update(
            {"clusterNewsId":newsId},
            {"$unset":{"clusterId":"", "clusterNewsId":"", "clusterSimilarity":""}}
        )
    #coll_news.update_one({"_id": doc["_id"]}, {"$set": {"categoryFinal": final}}, upsert=False)

helper.mongo.close()

print("Done")
