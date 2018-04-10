import xlsxwriter
import re


def make_xlsx(all_data):
    workbook = xlsxwriter.Workbook('media_categories.xlsx')

    def add_sheet(media_id, media_name, categories):
        sheet = workbook.add_worksheet("%s (%s)" % (media_id, media_name))

        sheet.write(0,0, '카테고리')
        sheet.write(0,1, '기사 갯수')

        row = 1
        for cate in categories:
            sheet.write(row, 0, cate[0])
            sheet.write(row, 1, int(cate[1]))
            row += 1

    for k in sorted(all_data.keys()):
        data = all_data[k]
        add_sheet(data[0], data[1], data[2])

    workbook.close()
    print("XLSX created")


RE_BEGIN = re.compile('^-+ BEGIN: (\d+) \((.+)\) -+$')
RE_END = re.compile('^-+ END: (\d+) \((.+)\) (\d+) categories -+')
RE_CATE = re.compile('^"(.*)"\t(\d+)$')
DataAll = {}

with open("media_cates.txt") as f:
    state = 0
    err = ""
    cur_media_id = None
    cur_media_name = None
    cur_media_cates = []
    for line in f.readlines():
        line = line.strip()
        if line == "":
            continue

        if state == 0:
            m = RE_BEGIN.match(line)
            if m is None:
                err = line
                break
            cur_media_id = m.group(1)
            cur_media_name = m.group(2)
            cur_media_cates = []
            state = 1

        elif state == 1:
            if line[0] == '-':
                m = RE_END.match(line)
                if m is None:
                    err = line
                    break
                media_id = m.group(1)
                media_name = m.group(2)
                cate_cnt = int(m.group(3))
                if media_id != cur_media_id or media_name != cur_media_name or cate_cnt != len(cur_media_cates):
                    print("ERROR: data inconsistency")
                    err = line
                    break
                #print("END: %s, %s, %d,%d" % (media_id, media_name, cate_cnt, len(cur_media_cates)))
                DataAll[media_id] = (media_id, media_name, cur_media_cates)
                state = 0

            else:
                m = RE_CATE.match(line)
                if m is None:
                    err = line
                    break

                cate = m.group(1)
                cnt = m.group(2)
                #print("[%s]: %s" % (cate,cnt))
                cur_media_cates.append((cate,cnt))


    if err != "":
        print("ERROR in line:", err)
    else:
        make_xlsx(DataAll)



print("Done")
