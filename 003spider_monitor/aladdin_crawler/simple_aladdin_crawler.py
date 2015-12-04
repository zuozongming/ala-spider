#!/usr/bin/env python
# -*- coding: gb18030 -*-
########################################################################
# 
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
# 
########################################################################
 
'''
File: simple_aladdin_crawler.py
Author: liyudong(liyudong@baidu.com)
Date: 2014/06/18 15:14:40
'''
import argparse
import logging
import urllib2
import urllib
import socket
import time
import sys
import os
import re

def parse_html_srcid(html):
    result = []
    if( len(html) == 0 ) :
        return result
    items = html.split("\n")
    for item in items :
        m = re.findall(r'srcid="(\d+)"', item)
        if( len(m) > 0 ) :
            for srcid in m :
                srcid = int(srcid)
                result.append(srcid)
    return result

def main(args, logger):
    site_url = args.target_url
    params = { 'ie' : 'utf-8' }
    try:
        fsock = open(args.input_path, "rb", 0)
        foutput = open(args.output_path, 'w+')
        line_no = 0
        logging.debug(213)
        for line in fsock:
            line_no += 1
            #time.sleep(5)
            cols = line.strip("\n").strip("\r").split("\t")
            query = cols[0]
            query = query.strip()
            if len(query) == 0:
                logger.warning("query is empty at line[%d]"%(line_no))
                continue
            uquery = query.decode("GB18030")
            new_query = uquery.encode("UTF-8")
            params['wd'] = new_query
            url = site_url
            url += "&"
            url += urllib.urlencode(params)
            #logging.debug(url)
            socket.setdefaulttimeout(100)
            req = urllib2.Request(url)
            response = urllib2.urlopen(req)
            result = response.read()
            src_id_list = parse_html_srcid(result)
            if len(src_id_list) > 0:
                foutput.write("%s\t%s\n"%(query, ",".join(src_id_list)))
    except IOError:
        logger.critical("IO error: input_path[%s] output_path[%s]" % 
                        (args.input_path, args.output_path))
    pass
if __name__ == "__main__":
    #parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", dest = "input_path", required = True, help = "query文件路径")
    parser.add_argument("--output", dest = "output_path", required = True, help = "结果的输出路径")
    parser.add_argument("--url", dest = "target_url", required = True, help = "要抓取的目标url")
    args = parser.parse_args()

    #logging
    log_name = "simple_aladdin_crawler"
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.DEBUG) 

    formatter = logging.Formatter('%(levelname)s: %(asctime)s: %(name)s %(process)s ' + 
        '[%(filename)s:%(lineno)s] %(message)s')  

    fh = logging.FileHandler(log_name + ".log")  
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter) 
    logger.addHandler(fh)

    main(args, logger)

#/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
