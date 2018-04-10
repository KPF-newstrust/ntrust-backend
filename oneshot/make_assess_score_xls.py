#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import oneshot_common
import helper
import re
import xlsxwriter

# 언론재단 요청 사항. 기사평가하기 158개의 기사의 요인별 상관관계를 계산하기 위해서
# 계량평가요인, 각종 스코어, 저널리즘가치점수 등을 취합해 xls 로 생성한다.

configs = helper.load_config("../ntrust.ini")

helper.mongo.connect(configs["DEST_MONGODB"])

coll_assess = helper.mongo.get_collection("asStats")
coll_news = helper.mongo.get_collection("news")

workbook = xlsxwriter.Workbook('assess_journal_scores.xlsx')
sheet = workbook.add_worksheet("기사평가")

sheet.set_column('A:A', 40)
sheet.set_column('B:B', 30)

merge_format = workbook.add_format({
    'border': 1,
    'align': 'center',
    'valign': 'vcenter',
    'fg_color': 'yellow'})

sheet.merge_range("C1:X1", "계량요인 및 점수", merge_format)
sheet.merge_range("Y1:AJ1", "위원회 점수", merge_format)
sheet.merge_range("AK1:AU1", "가중치 없는 저널리즘 가치 점수", merge_format)
sheet.merge_range("AV1:BF1", "가중치 적용된 저널리즘 가치 점수", merge_format)

rownum = 1
sheet.write(rownum, 0, '기사평가 페이지 URL')
sheet.write(rownum, 1, '제목')

sheet.write(rownum, 2, '바이라인')
sheet.write(rownum, 3, '바이라인 점수')

sheet.write(rownum, 4, '이미지 개수')
sheet.write(rownum, 5, '이미지수 점수')

sheet.write(rownum, 6, '기사 길이')
sheet.write(rownum, 7, '기사길이 점수')

sheet.write(rownum, 8, '평균문장 길이')
sheet.write(rownum, 9, '평균문장길이 점수')

sheet.write(rownum, 10, '문장당 평균 부사수')
sheet.write(rownum, 11, '문장당 평균 부사수 점수')

sheet.write(rownum, 12, '제목 길이')
sheet.write(rownum, 13, '제목길이 점수')

sheet.write(rownum, 14, '제목에 물음표/느낌표 수')
sheet.write(rownum, 15, '제목에 물음표/느낌표 점수')

sheet.write(rownum, 16, '제목의 부사수')
sheet.write(rownum, 17, '제목의 부사수 점수')

sheet.write(rownum, 18, '수치 인용수')
sheet.write(rownum, 19, '수치 인용수 점수')

sheet.write(rownum, 20, '인용문 수')
sheet.write(rownum, 21, '인용문수 점수')

sheet.write(rownum, 22, '인용문 길이 비율')
sheet.write(rownum, 23, '인용문 길이 비율 점수')

sheet.write(rownum, 24, '독이성')
sheet.write(rownum, 25, '투명성')
sheet.write(rownum, 26, '사실성')
sheet.write(rownum, 27, '유용성')
sheet.write(rownum, 28, '균형성')
sheet.write(rownum, 29, '다양성')
sheet.write(rownum, 30, '독창성')
sheet.write(rownum, 31, '중요성')
sheet.write(rownum, 32, '심층성')
sheet.write(rownum, 33, '선정성')
sheet.write(rownum, 34, '평균')
sheet.write(rownum, 35, '합계')

sheet.write(rownum, 36, '독이성')
sheet.write(rownum, 37, '투명성')
sheet.write(rownum, 38, '사실성')
sheet.write(rownum, 39, '유용성')
sheet.write(rownum, 40, '균형성')
sheet.write(rownum, 41, '다양성')
sheet.write(rownum, 42, '독창성')
sheet.write(rownum, 43, '중요성')
sheet.write(rownum, 44, '심층성')
sheet.write(rownum, 45, '선정성')
sheet.write(rownum, 46, '합계')

sheet.write(rownum, 47, '독이성')
sheet.write(rownum, 48, '투명성')
sheet.write(rownum, 49, '사실성')
sheet.write(rownum, 50, '유용성')
sheet.write(rownum, 51, '균형성')
sheet.write(rownum, 52, '다양성')
sheet.write(rownum, 53, '독창성')
sheet.write(rownum, 54, '중요성')
sheet.write(rownum, 55, '심층성')
sheet.write(rownum, 56, '선정성')
sheet.write(rownum, 57, '합계')


docs = coll_assess.find({})#.limit(5)
for doc in docs:
    news = coll_news.find_one({"newsId":doc["news_id"]})
    if not news:
        raise RuntimeError("News not found id=" + doc["news_id"])

    rownum += 1
    sheet.write(rownum, 0, "http://newstrust.kr/admin/assess/view?nws=" + doc["news_id"])
    sheet.write(rownum, 1, doc["title"])

    byline = ""
    if "bylines" in news:
        for bl in news["bylines"]:
            byline = ",".join(bl.values())
            break

    score = news["score"]
    sheet.write(rownum, 2, byline)
    sheet.write(rownum, 3, score["byline"])

    sheet.write(rownum, 4, news["image_count"])
    sheet.write(rownum, 5, score["imageCount"])

    sheet.write(rownum, 6, news["content_length"])
    sheet.write(rownum, 7, score["contentLength"])

    sheet.write(rownum, 8, news["content_avgSentenceLength"])
    sheet.write(rownum, 9, score["avgSentenceLength"])

    sheet.write(rownum, 10, news["content_avgAdverbsPerSentence"])
    sheet.write(rownum, 11, score["avgAdverbCountPerSentence"])

    sheet.write(rownum, 12, news["title_length"])
    sheet.write(rownum, 13, score["titleLength"])

    sheet.write(rownum, 14, news["title_numQuestion"] + news["title_numExclamation"])
    sheet.write(rownum, 15, score["titlePuncCount"])

    sheet.write(rownum, 16, len(news["title_adverbs"]))
    sheet.write(rownum, 17, score["titleAdverbCount"])

    sheet.write(rownum, 18, news["content_numNumber"])
    sheet.write(rownum, 19, score["numberCount"])

    sheet.write(rownum, 20, len(news["quotes"]))
    sheet.write(rownum, 21, score["quoteCount"])

    sheet.write(rownum, 22, news["content_quotePercent"] * 100)
    sheet.write(rownum, 23, score["quotePercent"])

    sheet.write(rownum, 24, doc["readability"])
    sheet.write(rownum, 25, doc["clariry"])
    sheet.write(rownum, 26, doc["reality"])
    sheet.write(rownum, 27, doc["usefulness"])
    sheet.write(rownum, 28, doc["balance"])
    sheet.write(rownum, 29, doc["variety"])
    sheet.write(rownum, 30, doc["uniqueness"])
    sheet.write(rownum, 31, doc["importance"])
    sheet.write(rownum, 32, doc["deep"])
    sheet.write(rownum, 33, doc["inflammation"])
    sheet.write(rownum, 34, doc["average"])
    sheet.write(rownum, 35, doc["sum"])

    vanilla = doc["vanilla"]
    sheet.write(rownum, 36, vanilla["readability"])
    sheet.write(rownum, 37, vanilla["transparency"])
    sheet.write(rownum, 38, vanilla["factuality"])
    sheet.write(rownum, 39, vanilla["utility"])
    sheet.write(rownum, 40, vanilla["fairness"])
    sheet.write(rownum, 41, vanilla["diversity"])
    sheet.write(rownum, 42, vanilla["originality"])
    sheet.write(rownum, 43, vanilla["importance"])
    sheet.write(rownum, 44, vanilla["depth"])
    sheet.write(rownum, 45, vanilla["sensationalism"])
    sheet.write(rownum, 46, doc["vanilla_totalSum"])

    journal = doc["journal"]
    sheet.write(rownum, 47, journal["readability"])
    sheet.write(rownum, 48, journal["transparency"])
    sheet.write(rownum, 49, journal["factuality"])
    sheet.write(rownum, 50, journal["utility"])
    sheet.write(rownum, 51, journal["fairness"])
    sheet.write(rownum, 52, journal["diversity"])
    sheet.write(rownum, 53, journal["originality"])
    sheet.write(rownum, 54, journal["importance"])
    sheet.write(rownum, 55, journal["depth"])
    sheet.write(rownum, 56, journal["sensationalism"])
    sheet.write(rownum, 57, doc["journal_totalSum"])

    print(doc["news_id"])

workbook.close()
helper.mongo.close()
print("Done")