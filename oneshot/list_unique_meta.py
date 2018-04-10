#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import oneshot_common
import helper

import datetime
import logging

# MongoDB ntrust2 db 의 posdic 콜렉션에서 사용된 meta 의 목록을 뽑는다.

configs = helper.load_config("../ntrust.ini")

helper.mongo.connect(configs["DEST_MONGODB"])

coll_posdic = helper.mongo.get_collection("posdic")

def get_unique_meta_list():
    unique_metas = dict()
    docs = coll_posdic.find({"meta":{"$exists":True}},{"meta":1, "_id":0})
    for doc in docs:
        for meta in doc["meta"]:
            if meta in unique_metas:
                unique_metas[meta] += 1
            else:
                unique_metas[meta] = 1

    return unique_metas


print(get_unique_meta_list())


helper.mongo.close()
print("Done")
