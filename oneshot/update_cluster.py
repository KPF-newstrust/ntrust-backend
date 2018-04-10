#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import helper


def parse_csv(pathname):
    inf = open(pathname)
    for line in inf.readlines():
        toks = line.rstrip().split("|")
        news_id = toks[0]
        delegate_news_id = toks[1]
        cluster_id = toks[2]
        similarity = toks[3]

        # ClusterDic
        #     - cluster_id
        #         [0] delegate_news_id
        #         [1] list()
        #             - news_id
        #             - similarity
        if cluster_id != "":
            if cluster_id not in ClusterDic:
                ClusterDic[cluster_id] = (delegate_news_id, list())
            elif ClusterDic[cluster_id][0] != delegate_news_id:
                raise RuntimeError("Cluster[%s] news_id diff: %s vs %s", cluster_id, ClusterDic[cluster_id][0],
                                   delegate_news_id)
            ClusterDic[cluster_id][1].append((news_id, similarity))


if __name__ == '__main__':
    configs = helper.load_config("../dev.ini")
    helper.mongo.connect(configs["DEST_MONGODB"])
    coll_news = helper.mongo.get_collection("news")

    NewsDic = dict()
    ClusterDic = dict()
    parse_csv("../data/cluster.csv")
    print("Total news: %d, total clusters: %d" % (len(NewsDic), len(ClusterDic)))

    # 기존 클러스터 정보 제거 (update)
    coll_news.update(
        {}, {"$unset": {"clusterDelegate": ""}}, multi=True
    )
    # unset clusterId, clusterNewsId, clusterSimilarity
    coll_news.update(
        {}, {"$unset": {"clusterId": "", "clusterNewsId": "", "clusterSimilarity": ""}}, multi=True
    )

    # 클러스터 정보 입력 (update)
    i = 0
    for (cluster_id, (delegate_news_id, similar_items)) in ClusterDic.items():
        if len(similar_items) > 2:
            i += 1
            similar_items.sort(key=lambda x: float(x[1]), reverse=True)
            print(i, "|", delegate_news_id)

            Dedup = dict()
            for (news_id, similarity) in similar_items:
                if similarity not in Dedup:
                    Dedup[similarity] = news_id
                    print("\t%s: %s" % (news_id, similarity))
                    coll_news.update_one({"newsId": news_id},
                                         {"$set": {"clusterNewsId": delegate_news_id, "clusterSimilarity": float(similarity)}},
                                         upsert=False)

            coll_news.update_one({"newsId": delegate_news_id},
                                 {"$set": {"clusterDelegate": True, "clusterNewsCount": len(Dedup)}},
                                 upsert=False)

    helper.mongo.close()
    print("Done")
