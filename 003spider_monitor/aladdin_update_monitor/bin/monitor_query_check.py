#! /bin/env python
#encoding=gb18030
# -*- encoding: gb18030 -*-

import sys
import getopt
import time

import json;
import logging;
import string;
from urllib import quote

def set_log_config():
    logging.basicConfig(\
            filename = "demo.log",\
            format = "[%(asctime)s][%(levelname)s][%(funcName)s][line:%(lineno)d]:%(message)s",\
            level = logging.DEBUG,\
            filemode = "w" \
            )
def main():
    query_file = "../data/monitor_query.dict"
    query_orign_file = query_file + "_temp"
    query_inter_file = "../conf/query_trans.conf"

    query_inter_dict = {}
    #加载配置
    with open(query_inter_file) as file_in:
        for line in file_in:
            line = line.decode("gb18030").strip("\n")
            part = line.split("\t")
            fq = part[0]
            if len(part) > 1:
                tq = part[1]
            else:
                tq = None
            query_inter_dict[fq] = tq
            out_line = "%s\t%s"%(fq, tq)
            logging.debug(out_line.encode("gb18030"))

    file_in = file(query_orign_file)
    file_out = file(query_file, "w")
    for line in file_in:
        line_temp = line.decode("gb18030").strip("\n")
        part = line_temp.split("\t")
        query = part[0]
        #不换
        if query not in query_inter_dict:
            file_out.write(line)
            continue
        tq = query_inter_dict[query]
        #屏蔽
        if tq == None or tq == "":
            continue
        out_line = "%s\t%s\n"%(tq, "\t".join(part[1:]))
        file_out.write(out_line.encode("gb18030"))
    file_in.close()
    file_out.close()

if __name__ == '__main__':
    set_log_config()
    opts, args = getopt.getopt(sys.argv[1:], "hi:o:", ["version", "file="])
    for op, value in opts:
        sys.stderr.write("op[%s]\tvalue[%s]\n"%(op, value))
    beg = time.time()
    main()
    tm = time.time()
    sys.stderr.write("%.4f\n" % (tm - beg) )
