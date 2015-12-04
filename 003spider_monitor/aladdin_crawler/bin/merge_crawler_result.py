#!/usr/bin/env python
# -*- coding: utf-8 -*-
########################################################################
# 
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
# 
########################################################################
 
"""
File: merge_crawler_result.py
Author: yudonglee(yudonglee@baidu.com)
Date: 2014/11/11 13:12:50
"""
import re
import time
import logging
import argparse
import ConfigParser
import urlparse
import sys
import os
import json

if __name__ == "__main__":
    #parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-q", dest = "query_file_path", required = True, help = "query文件路径")
    parser.add_argument("-r", dest = "result_file_path", required = True, help = "result文件路径")
    parser.add_argument("-o", dest = "output_path", required = True, help = "输出文件路径")
    args = parser.parse_args()

    #logging
    log_name = "merge_crawler_result"
    logger = logging.getLogger(log_name)
    #logger.setLevel(logging.DEBUG) 
    logger.setLevel(logging.INFO) 

    formatter = logging.Formatter('%(levelname)s: %(asctime)s: %(name)s %(process)s ' + 
        '[%(filename)s:%(lineno)s] %(message)s')  

    fh = logging.FileHandler(log_name + ".log")  
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter) 
    logger.addHandler(fh)


    line_counter = 0
    query_list = []
    query_file = open(args.query_file_path, "r")
        
    for line in query_file:
        line_counter += 1
        line = line.rstrip("\r\n ")
        cols = line.split("\t")
        query = cols[0]
        query_list.append(query)

    line_counter = 0
    result_dict = {}
    result_file = open(args.result_file_path, "r")
    for line in result_file:
        line_counter += 1
        line = line.rstrip("\r\n ")
        cols = line.split("\t")

        query = cols[0]
        if len(query) == 0:
            continue

        key = cols[1]
        json_str = cols[2]
        result_item = json.loads(json_str)

        if query not in result_dict:
            result_dict[query] = {}
            result_dict[query][key] = result_item
        else:
            #key在词典中不存在或数据不为空时，重置dict内容
            if (key not in result_dict[query] or 
                    (result_item['data'] is not None and len(result_item['data']) > 0)):
                result_dict[query][key] = result_item
    try:
        output_file = open(args.output_path, 'w')
    except IOError as e:
        logger.warning("fail to open file[%s]" % args.output_path) 

    for query in query_list:
        content_str = ""
        if query in result_dict:
            content_str = json.dumps(result_dict[query])
            output_file.write("%s\t%s\t%d\n"%(query, content_str, len(result_dict[query])))
        else:
            output_file.write("%s\t%s\t%d\n"%(query, content_str, 0))
