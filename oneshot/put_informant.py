#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import oneshot_common
import helper
import os

# 기사 실명정보원/익명정보원 머신러닝 결과를 news, asStat 콜렉션에 넣는다. NT2017-53

Dict = dict()

def parse_csv(pathname):
    inf = open(pathname)
    for line in inf.readlines():
        toks = line.rstrip().split("|")
        if toks[0] in Dict:
            raise RuntimeError("duplicated news id: " + toks[0])

        # 쉼표로 구분 & 없으면 빈 배열로
        informant_real = toks[1].split(",") if len(toks[1]) > 0 else []
        informant_anno = toks[2].split(",") if len(toks[2]) > 0 else []
        Dict[toks[0]] = [ informant_real , informant_anno ]

for (dirpath, dirnames, filenames) in os.walk("../data/informant"):
    for filename in filenames:
        if not filename.endswith(".csv"):
            continue
        pathname = os.path.join(dirpath, filename)
        parse_csv(pathname)


print("Total news: %d" % len(Dict))

configs = helper.load_config("../dev.ini")
helper.mongo.connect(configs["DEST_MONGODB"])
coll_news = helper.mongo.get_collection("news")

for k,v in Dict.items():
    # if k == '01100101.20160630225846623' : #(하나만 테스트)
    print(k, v)
    coll_news.update_one({"newsId":k}, {"$set":{ "informant_real":v[0], "informant_anno":v[1] }}, upsert=True)

helper.mongo.close()

print("Done")
