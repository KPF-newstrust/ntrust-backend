import sys
import logging
import pymysql
import configparser
import time

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s", datefmt='%Y-%m-%d %H:%M:%S')

configs = configparser.ConfigParser()
if not configs.read("ntrust.ini"):
    logging.error("Config file loading failed")
    sys.exit(1)

import site
site.addsitedir(configs['NTRUST_MODULE']['path'])
from ntrust.sanitizer.constants import PROVIDER_CODE

mycnf = configs['SOURCE_MYSQL']
conn = pymysql.connect(host=mycnf['host'], port=int(mycnf['port']), user=mycnf['user'], password=mycnf['pass'],
                       db=mycnf['db'], charset='utf8', cursorclass=pymysql.cursors.SSDictCursor, connect_timeout=99)

def get_categories(media_id, name):
    print("-------------------- BEGIN: %s (%s) --------------------" % (media_id, name))
    start_time = time.time()
    with conn.cursor() as cursor:
        cursor.execute("SELECT SUBJECTINFO as cate ,count(SUBJECTINFO) as cnt FROM _2016_CMS_NEWS WHERE media_id=%s GROUP BY SUBJECTINFO", media_id)
        rows = cursor.fetchall()
        for row in rows:
            print("\"%s\"\t%s" % (row['cate'], row['cnt']))
    elapsed = time.time() - start_time
    print("--------------- END: %s (%s) %d categories ---------- (%s sec)\n" % (media_id, name, len(rows), elapsed))

for cpKey,cpName in PROVIDER_CODE.items():
    #print("%s: %s" % (cpKey, cpName))
    get_categories(cpKey, cpName)


conn.close()
print("Done")