# 사용할 수 없는 언론사 기사를 처리 DB 에서 삭제한다.
# 이 스크립트는 한번만 실행하면 되며, 다시 쓸일은 아마 없을 것이다.
import logging
import pymongo
from bson.objectid import ObjectId
import configparser
import sys

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s", datefmt='%Y-%m-%d %H:%M:%S')

configs = configparser.ConfigParser()
if not configs.read("ntrust.ini"):
    logging.error("Config file loading failed")
    sys.exit(1)

import site
site.addsitedir(configs['NTRUST_MODULE']['path'])
from ntrust.sanitizer.constants import PROVIDER_CODE

mgo_cli = None
mgo_db = None

def open_mongo():
    global mgo_cli, mgo_db
    mgcnf = configs['DEST_MONGODB']
    mgo_cli = pymongo.MongoClient(mgcnf['uri'])
    mgo_db = mgo_cli[mgcnf['db']]
    logging.info("Successfully connected to MongoDB %s", mgcnf['uri'])


RAW_AGREE_MEDIA_NAMES = """
경향신문
국민일보
내일신문

문화일보
서울신문
세계일보
한겨레
한국일보
경기일보
경인일보
강원도민일보
대전일보
중도일보
중부매일
충북일보
충청일보
충청투데이
경남신문
경남도민일보
경상일보

국제신문
대구일보
매일신문

부산일보
영남일보
울산매일
광주일보
무등일보
전남일보
전북도민일보

전북일보
제민일보

한라일보
디지털타임스
매일경제
서울경제
전자신문
파이낸셜뉴스
한국경제
헤럴드경제

MBC
SBS

YTN
OBS
"""

AGREE_MEDIA_NAMES = []
for line in RAW_AGREE_MEDIA_NAMES.split("\n"):
    line = line.strip()
    if line != "":
        AGREE_MEDIA_NAMES.append(line)

# 확인된 언론사 코드,이름 목록을 전부 출력하고 빅카인즈 사용가능 여부도 표시한다. (엑셀에 복사 목적)
def print_media_codelist():
    for k,v in sorted(PROVIDER_CODE.items()):
        is_agree = "YES" if v in AGREE_MEDIA_NAMES else ""
        print("'%s\t%s\t%s" % (k,v,is_agree))


AGREE_MEDIA_IDS = []

def prepare_agree_media_ids():
    global AGREE_MEDIA_IDS
    for line in AGREE_MEDIA_NAMES:
        found = False
        for k,v in PROVIDER_CODE.items():
            if v == line:
                AGREE_MEDIA_IDS.append(k)
                found = True
        if not found:
            print("NOT FOUND:", line)


def remove_nonagree_media_docs():
    coll = mgo_db.news
    docs = coll.find(None, {"mediaId":1, "mediaName":1})#.skip(1100).limit(200)
    is_new = []
    for doc in docs:
        if doc["mediaId"] not in AGREE_MEDIA_IDS:
            if doc["mediaId"] not in is_new:
                is_new.append(doc["mediaId"])
                print("REMOVE %s(%s)" % (doc["mediaName"], doc["mediaId"]))
            coll.delete_one({'_id': ObjectId(doc["_id"])})


if __name__ == "__main__":
    prepare_agree_media_ids()
    open_mongo()
    remove_nonagree_media_docs()
    print("Done")