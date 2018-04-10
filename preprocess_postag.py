# 처리DB에서 본문을 읽고 MeCab 형태소 분석을 해서 다른 필드에 저장한다.
import logging
import pymongo
import configparser
import sys
import timeit
from bson.objectid import ObjectId

from konlpy.tag import Hannanum
from konlpy.tag import Kkma
from konlpy.tag import Komoran
from konlpy.tag import Twitter

import helper

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s", datefmt='%Y-%m-%d %H:%M:%S')

configs = configparser.ConfigParser()
if not configs.read("ntrust.ini"):
    logging.error("Config file loading failed")
    sys.exit(1)

import site
site.addsitedir(configs['NTRUST_MODULE']['path'])
from ntrust import tagger

mongo = helper.MongoMan()

hannanum = Hannanum()
kkma = Kkma()
komoran = Komoran()
twitter = Twitter()


def postag_news(doc):
    print("date=%s, title=%s" % (doc["insertDt"], doc['title']))

    update_set = dict()
    lines = doc['content'].split("\n")

    # Mecab
    mecab_tags = []
    mecab_start_time = timeit.default_timer()
    for line in lines:
        tagResults = tagger.merge(line, deep=False)
        for tres in tagResults:
            #print(tres)
            if tres[0] == "":
                continue

            if '-' not in tres[2]:
                pos = ','.join(tres[1])
                mecab_tags.append({"word": tres[0], "pos": pos})

    mecab_end_time = timeit.default_timer()
    update_set["mecab_time"] = mecab_end_time - mecab_start_time
    update_set["mecab_postag"] = mecab_tags
    #print("Mecab: %f seconds: %s" % (update_set["mecab_time"], update_set["mecab_postag"]))

    # Hannanum
    hannn_tags = []
    hannn_start_time = timeit.default_timer()
    for line in lines:
        hpos = hannanum.pos(line)
        for pos in hpos:
            hannn_tags.append({"word": pos[0], "pos": pos[1]})

    hannn_end_time = timeit.default_timer()
    update_set["hannanum_time"] = hannn_end_time - hannn_start_time
    update_set["hannanum_postag"] = hannn_tags
    #print("Hannanum: %f seconds: %s" % (update_set["hannanum_time"], update_set["hannanum_postag"]))

    # Kkma
    kkma_tags = []
    kkma_start_time = timeit.default_timer()
    for line in lines:
        kpos = kkma.pos(line)
        for pos in kpos:
            kkma_tags.append({"word": pos[0], "pos": pos[1]})

    kkma_end_time = timeit.default_timer()
    update_set["kkma_time"] = kkma_end_time - kkma_start_time
    update_set["kkma_postag"] = kkma_tags
    #print("Kkma: %f seconds: %s" % (update_set["kkma_time"], update_set["kkma_postag"]))

    """# Komoran
    komor_tags = []
    komor_start_time = timeit.default_timer()
    for line in lines:
        mpos = komoran.pos(line)

    komor_end_time = timeit.default_timer()
    update_set["komoran_time"] = komor_end_time - komor_start_time
    update_set["komoran_postag"] = komor_tags
    print("Komoran: %f seconds: %s" % (update_set["komoran_time"], update_set["komoran_postag"]))
    """

    # Twitter
    twit_tags = []
    twit_start_time = timeit.default_timer()
    for line in lines:
        tpos = twitter.pos(line)
        for pos in tpos:
            twit_tags.append({"word": pos[0], "pos": pos[1]})

    twit_end_time = timeit.default_timer()
    update_set["twitter_time"] = twit_end_time - twit_start_time
    update_set["twitter_postag"] = twit_tags
    #print("Twitter: %f seconds: %s" % (update_set["twitter_time"], update_set["twitter_postag"]))

    return update_set


def postag_dayall(mday):
    news_entity = mongo.get_collection("news_entity")
    news_entity.create_index([('newsId', pymongo.ASCENDING)], unique=True)
    for doc in mongo.all_news_for_ymd(2016, 6, mday):
        try:
            update_set = postag_news(doc)
            news_entity.update_one({"newsId": doc["newsId"]}, {"$set": update_set}, upsert=True)
        except:
            logging.error("Exception occured: %s", sys.exc_info()[0])


def collect_entity():
    counter = 0
    news_entity = mongo.get_collection("news_entity")
    docs = news_entity.find({"status":{"$exists":False}})
    for doc in docs:
        ec_mecab = helper.EntityCollector()
        for item in doc["mecab_postag"]:
            for pos in item["pos"].split(","):
                if pos.startswith('N'):
                    ec_mecab.feed(item["word"])
        results = ec_mecab.get_result("mecab_")

        ec_hannanum = helper.EntityCollector()
        for item in doc["hannanum_postag"]:
            if item["pos"].startswith('N'):
                ec_hannanum.feed(item["word"])
        results = ec_hannanum.get_result("hannanum_", results)

        ec_kkma = helper.EntityCollector()
        for item in doc["kkma_postag"]:
            if item["pos"].startswith('N'):
                ec_kkma.feed(item["word"])
        results = ec_kkma.get_result("kkma_", results)

        ec_twit = helper.EntityCollector()
        for item in doc["twitter_postag"]:
            if item["pos"].startswith('N'):
                ec_twit.feed(item["word"])
        results = ec_twit.get_result("twitter_", results)

        results["status"] = 1
        news_entity.update_one({"newsId":doc["newsId"]}, {"$set":results})
        counter += 1
        print("Updated %d: %s" % (counter, doc["newsId"]))


mday_start = 0
if len(sys.argv) > 1 and sys.argv[1].isdigit():
    mday_start = int(sys.argv[1])

mday_end = mday_start + 1
if len(sys.argv) > 2 and sys.argv[2].isdigit():
    mday_end = int(sys.argv[2])

if mday_start > 0:
    for mday in range(mday_start, mday_end):
        postag_dayall(mday)

#doc = mongo.get_collection("news").find_one({"_id": ObjectId("5980eeaac7afb50f758c4bb2")})
#print(postag_news(doc))

#helper.EntityCollector.load_data(mongo.get_db())
#collect_entity()


#c = '950만 달러(미국돈 표시)짜리 럭셔리 맨션을 사들였다.'
#results = tagger.merge(c, deep=True)
#for res in results:
#    print(res)

#['표시', ['NNG'], ['-'], 'b64aca8c80a511e798eeb8e8563cd83c']
#['미국돈 표시', ['NNP', 'NNG'], ['M'], 'b64aca8c80a511e798eeb8e8563cd83c']

