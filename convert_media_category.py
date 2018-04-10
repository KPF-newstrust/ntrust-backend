import xlrd
import re
import logging
import pymongo
import configparser

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s", datefmt='%Y-%m-%d %H:%M:%S')

configs = configparser.ConfigParser()
if not configs.read("ntrust.ini"):
    raise SystemExit("Config file loading failed")



mgo_cli = None
mgo_db = None

def open_mongo():
    global mgo_cli, mgo_db
    mgcnf = configs['DEST_MONGODB']
    mgo_cli = pymongo.MongoClient(mgcnf['uri'])
    mgo_db = mgo_cli[mgcnf['db']]
    logging.info("Successfully connected to MongoDB %s", mgcnf['uri'])

MediaCates = dict()

VALID_NTCATES = ["문화 예술", "경제", "교육", "연예", "국제", "IT 과학", "라이프스타일", "정치", "사회", "스포츠", "사설·칼럼", "기타"]

def open_convert_table():
    wb = xlrd.open_workbook("data/media_category_match.xlsx")

    reSheetName = re.compile('(\d+) \((.+)\)')

    for s in wb.sheets():
        m = reSheetName.match(s.name)
        if m is None:
            raise Exception("Unknown sheet name format")
        media_id = m.group(1)
        print('Sheet:', s.name, "rows:", s.nrows, "MediaId:", media_id)
        cateMap = dict()
        for row in range(1,s.nrows):
            orig_cate_name = s.cell(row, 0).value
            if not isinstance(orig_cate_name, str):
                logging.warning("Skip non-string name: %s", orig_cate_name)
                continue
            orig_cate_name = orig_cate_name.strip()
            if orig_cate_name == "":
                continue

            nt_cate_name = s.cell(row, 2).value.strip()
            if nt_cate_name == "":
                continue
            if nt_cate_name not in VALID_NTCATES:
                raise Exception("Unknown NT category: " + nt_cate_name)
            cateMap[orig_cate_name] = nt_cate_name
            #print("%s => %s" % (orig_cate_name, nt_cate_name))

        #print("%s => %d categories" % (media_id, len(cateMap)))
        if len(cateMap) > 0:
            MediaCates[media_id] = cateMap


def convert_media_category_to_ntcategory():
    coll = mgo_db.news
    docs = coll.find()
    num_total = 0
    num_update = 0
    mute_cnt = 0
    unknown_media = set()
    logging.debug("Total %d docs", docs.count())
    for doc in docs:
        num_total += 1
        if doc["mediaId"] not in MediaCates.keys():
            if not doc["mediaId"] in unknown_media:
                unknown_media.add(doc["mediaId"])
                print("No media:", doc["mediaId"])
            continue
        oldCate = doc["categoryOrig"]
        if oldCate == "":
            ntCate = "기타"
        elif oldCate not in MediaCates[doc["mediaId"]]:
            logging.warning("Invalid categoryOrig %s in media %s", oldCate, doc["mediaId"])
            continue
        else:
            ntCate = MediaCates[doc["mediaId"]][oldCate]
        #print("%s: %s => %s" % (doc["_id"], oldCate, ntCate))
        coll.update({"_id": doc["_id"]}, {"$set": {"categoryXls": ntCate}})
        num_update += 1
        mute_cnt += 1
        if mute_cnt > 1000:
            mute_cnt = 0
            logging.debug("%d of %d updated", num_update, num_total)

if __name__ == "__main__":
    open_convert_table()
    open_mongo()
    convert_media_category_to_ntcategory()
    print("Done")