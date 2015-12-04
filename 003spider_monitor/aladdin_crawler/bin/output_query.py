#!/usr/bin/env python
# -*- coding: gb18030 -*-
########################################################################
# 
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
# 
########################################################################
 
'''
File: output_query.py
Author: liyudong(liyudong@baidu.com)
Date: 2014/06/18 15:14:40
'''
import re
import time
import logging
import argparse
import ConfigParser
import sys
import os

if __name__ == "__main__":
    #parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-q", dest = "query_file_path", required = True, help = "query文件路径")
    parser.add_argument("-d", dest = "data_dict_path", required = True, help = "query的数据词典路径")
    args = parser.parse_args()

    #logging
    log_name = "output_query"
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.INFO) 

    formatter = logging.Formatter('%(levelname)s: %(asctime)s: %(name)s %(process)s ' + 
        '[%(filename)s:%(lineno)s] %(message)s')  

    fh = logging.FileHandler(log_name + ".log")  
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter) 
    logger.addHandler(fh)

    data_dict = {}
    data_file = open(args.data_dict_path, "r")
    line_no = 1
    for line in data_file:
        line = line.strip("\r\n").strip()
        cols = line.split("\t")
        query = cols[2]
        type = cols[1]
        diff = cols[3]
        if query in data_dict:
            pass
        else:
            item = {}
            item['type'] = type
            item['diff'] = diff
            data_dict[query] = item
        line_no += 1
        
    query_file = open(args.query_file_path, "r")
    for line in query_file:
        line = line.strip("\r\n").strip()
        cols = line.split("\t")
        query = cols[0]
        if query in data_dict:
            print "%s\t%s\t%s"%(query, data_dict[query]['type'], data_dict[query]['diff'])
        else:
            print "%s\tNULL"%(query)

