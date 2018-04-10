#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import oneshot_common
import helper
import re

# 기존 18만개 가량의 entity_dic 으로부터 posdic 에 넣기 위해 데이터를 추출하고 정제하여 입력한다.
# 예: 성씨 load 후 사람이름 entity 중에 성씨로 시작하는 (한국 이름)을 찾아 성을 떼고 이름을 posdic 에 이름 메타로 입력

configs = helper.load_config("../ntrust.ini")

helper.mongo.connect(configs["DEST_MONGODB"])

coll_posdic = helper.mongo.get_collection("posdic")
coll_entity = helper.mongo.get_collection("entity_dic")

korean_family_names = set()
korean_given_names = set()

def extract_korean_family_names():
    global korean_family_names
    docs = coll_posdic.find({"pos":"명사","meta":{"$in":["성씨"]}}, {"word":1, "_id":0})
    for doc in docs:
        korean_family_names.add(doc["word"])
    return korean_family_names

def process_entity_persons():
    global korean_given_names
    docs = coll_entity.find({"types":{"$in":["PS"]}, "word":{"$not":re.compile(' ')}, "$where":"(this.word.length > 2)"}, {"word":1, "_id":0})
    for doc in docs:
        name = doc["word"]
        for surname in korean_family_names:
            if name.startswith(surname):
                givenname = name[len(surname):]
                if len(givenname) == 2:
                    korean_given_names.add(givenname)
                break


def extract_location_names():
    docs = coll_entity.find({"types":{"$in":["LC"]}, "word":{"$not":re.compile(' ')}}, {"word":1, "_id":0}).limit(100)
    for doc in docs:
        print(doc)

#extract_korean_family_names()
#process_entity_persons()
#print(korean_given_names)

extract_location_names()


print("cnt=", len(korean_given_names))

helper.mongo.close()
print("Done")