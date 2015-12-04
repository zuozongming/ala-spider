#!/usr/bin/env python
# -*- coding: gb18030 -*-
########################################################################
# 
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
# 
########################################################################
 
'''
File: base_content_parser.py
Author: liyudong(liyudong@baidu.com)
Date: 2014/06/18 12:00:58
'''

import json

class BaseContentParser:
    '''
    base content parser
    '''
    def __init__(self):
        '''
        class constructor
      
        Args:
            self :
        Return:   
        Raise: 
        '''
        pass

    def parse(self, str_content):
        '''
        parse html link
      
        Args:
            self :
        Return:   
        Raise: 
        '''
        error_msg = None
        result_list = []
        try:
            json_data = json.loads(str_content)
        except ValueError as e:
            error_msg = "fail to load json string"
            return error_msg, result_list
        for result_idx, result in enumerate(json_data['results']):

            result_item = {}
            result_item['url'] = result['url']
            result_item['title'] = result['title']
            result_item['position'] = result_idx
            result_item['srcid'] = 0
            result_item['tplt'] = ""
            result_item['stdstg'] = ""
            result_item['stdstl'] = ""

            item_data = result['data']
            #过滤非特型结果
            if ('SrcId' not in item_data or 
                    'resultData' not in item_data or 
                    'tplData' not in item_data['resultData']):
                result_list.append(result_item)
                continue

            result_item['srcid'] = int(item_data['SrcId'])

            if 'StdStg' in item_data:
                result_item['stdstg'] = item_data['StdStg']

            if 'StdStl' in item_data:
                result_item['stdstl'] = item_data['StdStl']

            ext_data = item_data['resultData']['extData']
            tpl_data = item_data['resultData']['tplData']
            if 'tplt' in ext_data:
                result_item['tplt'] = ext_data['tplt']

            if (len(result_item['title']) == 0 and 
                    'title' in tpl_data and len(tpl_data['title']) > 0):
                result_item['title'] = tpl_data['title']

            result_list.append(result_item)
        return error_msg, result_list

if __name__ == "__main__":
    import time
    import url_downloader
    downloader = url_downloader.UrlDownloader()

    url = "http://desk.zol.com.cn/"
    error_msg, html, redirected_url = downloader.download(url)
    print error_msg, url, redirected_url, len(html)
    time.sleep(2)

    page = HtmlParser([], ['.*\.(gif|png|jpg|bmp)$'])
    links = page.filter_links(url, html)
    print links
