# 개체명 엑셀파일을 읽어 몽고DB에 저장한다.
import sys
import logging
import configparser
import time
import pymongo
import xlrd

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s", datefmt='%Y-%m-%d %H:%M:%S')

configs = configparser.ConfigParser()
if not configs.read("ntrust.ini"):
    logging.error("Config file loading failed")
    sys.exit(1)


mgo_cli = None
mgo_db = None

def open_mongo():
    global mgo_cli, mgo_db
    mgcnf = configs['DEST_MONGODB']
    mgo_cli = pymongo.MongoClient(mgcnf['uri'])
    mgo_db = mgo_cli[mgcnf['db']]
    logging.info("Successfully connected to MongoDB %s", mgcnf['uri'])

EntityDic = dict()

SUPPORTED_TYPES = [ "PS", "OG", "LC", "PL", "PR", "EV" ]

def read_entity_excel():
    wb = xlrd.open_workbook("data/entity_20160830.xlsx")
    for sheet in wb.sheets():
        for row in range(1, sheet.nrows):
            word = sheet.cell(row, 0).value.strip()
            type = sheet.cell(row, 1).value.strip()

            if type not in SUPPORTED_TYPES:
                print("Unsupported type:", type)
                return False

            if word in EntityDic:
                arr = EntityDic[word]
                if type in arr:
                    continue

                #print("Entity word %s(%s) duplicated (prev type: %s)" % (word, type, EntityDic[word]))
                arr.append(type)
                continue

            EntityDic[word] = [type]

    return True

def read_location_excel():
    wb = xlrd.open_workbook("data/entity_location.xlsx")
    for sheet in wb.sheets():
        for row in range(0, sheet.nrows):
            word = sheet.cell(row, 0).value.strip()
            type = "LC"

            if word in EntityDic:
                print("Location word %s duplicated" % (word))
                arr = EntityDic[word]
                if type in arr:
                    continue

                arr.append(type)
                continue

            EntityDic[word] = [type]
    return True

def save_entity_db():
    coll = mgo_db.entity_dic
    coll.create_index([('word', pymongo.ASCENDING)], unique=True)
    for k, arr in EntityDic.items():
        doc = {"word":k, "types":arr}
        coll.insert_one(doc)

    print("entity_dic saved")

if not read_entity_excel():
    raise SystemExit

if not read_location_excel():
    raise SystemExit

open_mongo()
save_entity_db()

if mgo_cli is not None:
    mgo_cli.close()

print("Done %s" % (len(EntityDic)))