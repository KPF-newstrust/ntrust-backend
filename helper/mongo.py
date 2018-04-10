import logging
import configparser
import datetime
import sys
import pymongo

class MongoMan():
    def __init__(self):
        self.mgo_cli = None
        configs = configparser.ConfigParser()
        if not configs.read("ntrust.ini"):
            logging.error("NTRUST config file reading failed.")
            sys.exit(1)

        mgcnf = configs['DEST_MONGODB']
        self.mgo_cli = pymongo.MongoClient(mgcnf['uri'])
        self.mgo_db = self.mgo_cli[mgcnf['db']]
        logging.info("Successfully connected to MongoDB %s", mgcnf['uri'])

    def __del__(self):
        print("MongoConn implicitly closed")
        self.close()

    def close(self):
        if self.mgo_cli is not None:
            self.mgo_cli.close()
            self.mgo_cli = None

    def get_db(self):
        return self.mgo_db

    def get_collection(self, collname):
        return self.mgo_db[collname]

    def all_news_for_ymd(self, year, month, day, lim=None):
        ymd = datetime.date(year, month, day)
        dt_from = datetime.datetime.combine(ymd, datetime.time.min)
        dt_to = dt_from + datetime.timedelta(days=1)
        docs = self.mgo_db.news.find({"insertDt": {"$gte": dt_from, "$lt": dt_to}}, no_cursor_timeout=True)
        cnt = 0
        for doc in docs:
            yield doc
            cnt += 1
            if lim is not None and cnt >= lim:
                return




mgo_cli = None
mgo_db = None

def connect(config):
    global mgo_cli, mgo_db
    if "uri" not in config:
        raise SystemExit("Invalid mongo config section")
    mgo_cli = pymongo.MongoClient(config['uri'])
    mgo_db = mgo_cli[config['db']]
    mgo_cli.server_info() # will raise exception if failed
    logging.info("Successfully connected to MongoDB %s", config['uri'])

def close():
    global mgo_cli, mgo_db
    if mgo_cli is not None:
       mgo_cli.close()
       mgo_cli = None
       mgo_db = None

def get_db():
    if mgo_db is None:
        raise RuntimeError("MongoDB not connected")
    return mgo_db

def get_collection(collname):
    if mgo_db is None:
        raise RuntimeError("MongoDB not connected")
    return mgo_db[collname]
