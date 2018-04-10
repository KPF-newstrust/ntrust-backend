#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import helper

configs = helper.load_config("../dev.ini")
helper.mongo.connect(configs["DEST_MONGODB"])
coll_news = helper.mongo.get_collection("news")
coll_anno = helper.mongo.get_collection("annotate")


docs = coll_anno.find({}, {"newsId":'true', 'category':'true'})#.limit(3)
for doc in docs:
    newsId = doc['newsId']

    news = coll_news.find_one({'newsId':newsId})
    print(doc['newsId'], doc['category'], '->', news['categoryFinal'], news['mediaId'], news['mediaName'])
    coll_anno.update(
        {'newsId':newsId},
        {'$set':
             {'category':news['categoryFinal'], 'mediaId':news['mediaId'], 'mediaName':news['mediaName']}
         }
    )

helper.mongo.close()
print("Done")

