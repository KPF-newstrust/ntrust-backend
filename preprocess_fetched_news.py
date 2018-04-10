import sys
import logging
import datetime
import pymongo
import configparser
import pprint

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt='%Y-%m-%d %H:%M:%S')

configs = configparser.ConfigParser()
if not configs.read("ntrust.ini"):
    logging.error("Config file loading failed")
    sys.exit(1)

import site
site.addsitedir(configs['NTRUST_MODULE']['path'])
from ntrust.sanitizer import Processor
from ntrust.sanitizer.constants import PROVIDER_CODE
import ntrust

mgo_cli = None
mgo_db = None

def open_mongo():
    global mgo_cli, mgo_db
    mgcnf = configs['DEST_MONGODB']
    mgo_cli = pymongo.MongoClient(mgcnf['uri'])
    mgo_db = mgo_cli[mgcnf['db']]
    coll_dst = mgo_db.news
    coll_dst.create_index([('newsId', pymongo.ASCENDING)], unique=True)
    coll_dst.create_index([('insertDt', pymongo.ASCENDING)], unique=False)
    logging.info("Successfully connected to MongoDB %s", mgcnf['uri'])

"""
def sanitize_and_split(media_id, content):
    sancon = Processor(media_id).sanitize(content)
    print("%s,%s\n=>%s" % (media_id, content, sancon))
    con = split_sentence(sancon['content'])
    for line in con:
        print(line)
    return sancon
"""

def convert_doc(doc):
    mediaId = doc['media_id']
    mediaName = PROVIDER_CODE[mediaId] if mediaId in PROVIDER_CODE else None

    sancon = Processor(mediaId, default_provider_code=mediaId).sanitize(doc['news_content'])

    if sancon['byline'] is None:
        sancon['byline'] = doc['writer_byline']

    sancon["newsId"] = doc["newsitem_id"]
    sancon["mediaId"] = mediaId
    sancon["mediaName"] = mediaName
    sancon["title"] = ntrust.sanitizer.base.sanitize_title(doc["title"])
    sancon["content"] = "\n".join(ntrust.sentences.split_sentence(sancon['content']))
    sancon["insertDt"] = doc["insert_dt"]
    sancon["pubDate"] = doc["pub_date"]
    sancon["categoryOrig"] = doc["subjectinfo"]
    sancon["url"] = doc["news_url"]

    return sancon
    #pprint("SanCon:%s, ByLine=%s" % (sancon['content'],sancon['byline']))


def process_day(target_day):
    dt_from = datetime.datetime.combine(target_day, datetime.time.min)
    dt_to = dt_from + datetime.timedelta(days=1)

    coll_src = mgo_db.cm2016
    coll_dst = mgo_db.news

    docs = coll_src.find({"insert_dt": {"$gte": dt_from, "$lt": dt_to}})
    for doc in docs:
        old = coll_dst.find_one({"newsId": doc['newsitem_id']})
        if old is None:
            sancon = convert_doc(doc)
            ins_id = coll_dst.insert_one(sancon)
            logging.info("Converted %s: %s", sancon["newsId"], sancon["title"])


if __name__ == "__main__":
    open_mongo()
    for mday in range(1,30+1):
        target_day = datetime.date(2016,6,mday)
        process_day(target_day)
    mgo_cli.close()

