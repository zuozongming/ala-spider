#! /bin/env python
#encoding=gb18030
# -*- encoding: gb18030 -*-

import sys
import getopt
import time
import datetime

import json;
import logging;
import string;
from urllib import quote

def set_log_config():
    logging.basicConfig(\
#            filename = "demo.log",\
            format = "[%(asctime)s][%(levelname)s][%(funcName)s][line:%(lineno)d]:%(message)s",\
            level = logging.DEBUG,\
            filemode = "w" \
            )

def trans_string_to_date_time(time_str, schema="%Y-%m-%d %H:%M:%S"):
    the_time = time.strptime(time_str, schema)
    ret = int(time.mktime(the_time))
    return ret

class GroupRet(object):
    def __init__(self, query):
        self.query = query
        self.update_time = None
        self.url = None
        self.srcid = None
        self.content_key = None
        self.tplt = None
        self.type = None

        #下面是算出来的
        #更新的时间点
        self.time_tag = None
        #表示当前更新的时间
        self.update_key = None
        self.uri = None
        self.status = 0
        self.detail_list = {}

    def load_json(self, group_id, j_data):
        err_ret = -1
        if group_id not in j_data:
            return err_ret
        self.detail_list = j_data[group_id]
        if len(j_data[group_id]) == 0:
            return err_ret
        latest_version = j_data[group_id][-1]
        if 'data' not in latest_version:
            return err_ret
        data_node = latest_version['data']
        if "type" not in data_node:
            return err_ret
        self.type = data_node["type"]
        self.url = data_node["url"]
        self.srcid = str(data_node["srcid"])
        self.tplt = data_node["tplt"]
        self.update_time = data_node["update_time"]

        self.time_tag = trans_string_to_date_time(self.update_time)
        self.content_key = data_node["content_key"]
        if self.type == "zongyi":
            temp = self.content_key.rstrip(u"期").replace("-", "")
            if len(temp) != 8:
                out_line = "zongyi content_key error. query[%s], group_id[%s], content[%s]"%(self.query, group_id, self.content_key)
                logging.error(out_line.encode("gb18030"))
                return err_ret
            self.update_key = int(temp)
            if self.update_key > 20160000 or self.update_key < 19710000:
                out_line = "zongyi content_key error. query[%s], group_id[%s], content[%s]"%(self.query, group_id, self.content_key)
                logging.error(out_line.encode("gb18030"))
                return err_ret
        elif self.type == "comic" or self.type == "tv":
            temp = self.content_key.split("/")[0]
            try:
                self.update_key = int(temp)
            except:
                out_line = "temp not int %s\t%s"%(self.query, temp)
                logging.error(out_line.encode("gb18030"))
                self.update_key = 0
        elif self.type == "movie":
            pass
        else:
            out_line = "type error. query[%s], group_id[%s]"%(self.query, group_id)
            logging.debug(out_line.encode("gb18030"))
        self.status = 1
        return 0
        out_line = "%s\t%s"%(self.type, self.content_key)
        logging.debug(out_line.encode("gb18030"))

    def get_update_key_begin_time_tag(self):
        ret = None
        for signle_ret in self.detail_list:
            if 'data' not in signle_ret:
                continue
            data_node = signle_ret['data']
            if "content_key" not in data_node:
                continue
            if data_node["content_key"] == self.content_key:
                ret = trans_string_to_date_time(data_node["update_time"])
                return ret
        return ret

class QueryDetail(object):
    def __init__(self, query):
        self.query = query
        self.crawler_dict = {}

        #下面两个属性可能空缺, 从query文件中来
        self.domain = None
        self.index = None

    def out_info(self):
        out_line = "%s\t%s\t%s"%(self.query, self.index, self.crawler_dict.keys())
        logging.debug(out_line.encode("gb18030"))
        pass


    def compare_check_out(self, group_id_list, file_out, msg1, msg2=""):
        out_line = "%s\t%s\t%s\t%s\t\n"%(self.query, group_id_list, msg1, msg2)
        file_out.write(out_line.encode("gb18030"))
#        logging.debug(out_line.encode("gb18030"))

    def compare_check(self, group_id_list, file_out):
        if len(group_id_list) != 2:
            out_line = "compare_check group_id_list error. query[%s], group_id_list[%s]"%(self.query, group_id_list)
            logging.error(out_line.encode("gb18030"))
        monitor_id = group_id_list[0]
        matched_id = group_id_list[1]
        monitor_gr = GroupRet(self.query)
        matched_gr = GroupRet(self.query)

        matched_gr.load_json(matched_id, self.crawler_dict)
        if matched_gr.status == 0:
            out_line = "matched_gr.status is 0, ignore. query[%s], group_id_list[%s]"%(self.query, group_id_list)
            logging.error(out_line.encode("gb18030"))
            return 0
        monitor_gr.load_json(monitor_id, self.crawler_dict)
        if monitor_gr.status == 0:
            self.compare_check_out(group_id_list, file_out, "not callback")
            return 0
        if matched_gr.type != monitor_gr.type:
            out_line = "monitor_gr and matched_gr different type. query[%s], group_id_list[%s], type[%s:%s]"%(self.query, group_id_list,  monitor_gr.type, matched_gr.type)
#            logging.debug(out_line.encode("gb18030"))
            return 0
        if monitor_gr.update_key < matched_gr.update_key:
            start_time = matched_gr.get_update_key_begin_time_tag()
            time_gap = int(time.time()) - start_time
            if time_gap > 300:
                self.compare_check_out(group_id_list, file_out, "update_slow", "%s < %s\tdebug:now_time[%s], time_tag[%s] - begin_time[%s] = %s s, %s hour"%(monitor_gr.update_key, matched_gr.update_key, time.ctime(), time.time(), start_time, time_gap, time_gap/3600.0))
#            out_line = "update slow. query[%s], group_id_list[%s], update_key[%s:%s]"%(self.query, group_id_list,  monitor_gr.update_key, matched_gr.update_key)
#            logging.debug(out_line.encode("gb18030"))


'''
        try:
            matched_gr.load_json(matched_id, self.crawler_dict)
        except:
            out_line = "matched_gr error query[%s]"%self.query
            logging.debug(out_line.encode("gb18030"))

        try:
            monitor_gr.load_json(monitor_id, self.crawler_dict)
        except:
            out_line = "monitor_gr error query[%s]"%self.query
            logging.debug(out_line.encode("gb18030"))
'''

class TroubleSeeker(object):
    def __init__(self, flow_in):
        #输入文件
        self.query_info_file = "../data/monitor_query.dict"

        #输入流
        self.data_in = flow_in
        #query词典
        self.query_detail_dict = {}

        #对比的组
        self.compare_group_list = [ \
                ["sample", "online"], \
                ["pad_sample", "online"], \
                ]
        self.pc_out = '../data/pc_monitor_out'
        self.pad_out = '../data/pad_monitor_out'

    def run(self):
        self.load_all_info()
        self.seek_trouble()

    def load_all_info(self):
        #读入query的类别和顺序
        with open(self.query_info_file) as file_in:
            idx = 0
            for line in file_in:
                idx += 1
                line = line.decode("gb18030").strip("\n")
                part = line.split("\t")
                query = part[0]
                domain = part[2]
                qd = QueryDetail(query)
                qd.index = idx
                qd.domain = domain
                self.query_detail_dict[query] = qd

        for line in self.data_in:
            line = line.decode("utf8").strip("\n")
            part = line.split("\t")
            query = part[0]
            group_id = part[1]
            j_str = part[2]
            j_data = json.loads(j_str)
            if query not in self.query_detail_dict:
                qd = QueryDetail(query)
                self.query_detail_dict[query] = qd
            qd = self.query_detail_dict[query]
            qd.crawler_dict[group_id] = j_data

    def seek_trouble(self):
        f_o_pc = file(self.pc_out, "w")
        f_o_pad = file(self.pad_out, "w")
        query_list = sorted(self.query_detail_dict.keys(), key=lambda k:self.query_detail_dict[k].index)
#        for query in self.query_detail_dict:
        for query in query_list:
            qd = self.query_detail_dict[query]
#            qd.out_info()
            if qd.index == None:
                continue
            qd.compare_check(self.compare_group_list[0], f_o_pc)
            qd.compare_check(self.compare_group_list[1], f_o_pad)
        f_o_pc.close()
        f_o_pad.close()

def main():
    idx = 0
    ts = TroubleSeeker(sys.stdin)
    ts.run()
    return 0

def test():
    ret = trans_string_to_date_time("2015-02-15 19:38:34")


if __name__ == '__main__':
    opts, args = getopt.getopt(sys.argv[1:], "hi:o:", ["version", "file="])
    set_log_config()
    for op, value in opts:
        sys.stderr.write("op[%s]\tvalue[%s]\n"%(op, value))
    beg = time.time()
    main()
#    test()
    tm = time.time()
    sys.stderr.write("%.4f\n" % (tm - beg) )
