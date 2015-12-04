#!/usr/bin/env python
# -*- coding: gb18030 -*-
########################################################################
# 
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
# 
########################################################################
 
'''
File: video_content_parser.py
Author: liyudong(liyudong@baidu.com)
Date: 2014/06/18 12:00:58
'''

import json

class VideoContentParser:
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
        parse json string
      
        Args:
            self :
        Return:   
        Raise: 
        '''
        error_msg = None
        result_data = {}
        try:
            json_data = json.loads(str_content)
        except ValueError as e:
            error_msg = "fail to load json string"
            return error_msg, result_data

        vdo_srcid_blacklist = {
                21018 : 1
                }
        vdo_srcid_dict = {
                6869 : 1
                }
        vdo_tplt_blacklist = {
                'naturalresult' : 1
                }
        vdo_tplt_dict = {
                "zx_tv_video" : 1,
                "movievideo" : 1,
                "vd_tamasha" : 1,
                "vd_comic" : 1,
                "vd_sitcom" : 1,
                "seriesmovie" : 1,
                "tv_video" : 1,
                "zx_new_tvideo" : 1,
                "zx_new_mvideo" : 1,
                #"hotmovie" : 1,
                "video_alone" : 1,
                "zx_tamasha" : 1
                }
        for result_idx, result in enumerate(json_data['results']):
            item_data = result['data']
            #过滤非特型结果
            if ('SrcId' not in item_data or 
                    'resultData' not in item_data or 
                    'tplData' not in item_data['resultData']):
                continue
            srcid = int(item_data['SrcId'])
            if srcid in vdo_srcid_blacklist:
                continue
            if 'StdStg' not in item_data:
                stdstg = ""
            else:
                stdstg = item_data['StdStg']

            if 'StdStl' not in item_data:
                stdstl = ""
            else:
                stdstl = item_data['StdStl']
            position = result_idx
            url = result['url']
            title = result['title']
            if 'resultData' not in item_data or 'tplData' not in item_data['resultData']:
                continue
            ext_data = item_data['resultData']['extData']
            tpl_data = item_data['resultData']['tplData']
            if 'tplt' not in ext_data:
                tplt = ""
            else:
                tplt = ext_data['tplt']
            if len(title) == 0 and 'title' in tpl_data and len(tpl_data['title']) > 0:
                title = tpl_data['title']
                    
            video_type = 0

            if tplt in vdo_tplt_blacklist:
                video_type = 0
            elif tplt in vdo_tplt_dict:
                video_type = 1
            elif srcid in vdo_srcid_dict:
                video_type = 1
            elif 'big_poster' in tpl_data:
                video_type = 1
            elif ('title' in tpl_data and isinstance(tpl_data['title'], basestring) and
                    tpl_data['title'].find(u"高清视频在线观看") > 0):
                video_type = 1

            if video_type == 0:
                continue

            result_data['srcid'] = srcid
            result_data['stdstg'] = stdstg
            result_data['stdstl'] = stdstl
            result_data['tplt'] = tplt
            result_data['position'] = position
            result_data['url'] = url
            result_data['title'] = title
            result_data['tplData'] = tpl_data

            result_data['tplData'].pop("brief", None)
            result_data['tplData'].pop("brief_short", None)
            result_data['tplData'].pop("summary", None)
            result_data['tplData'].pop("type_new", None)
            result_data['tplData'].pop("al_title", None)
            result_data['tplData'].pop("al_title_new", None)
            result_data['tplData'].pop("al_slogan", None)

            key_filter_map = {
                    'loc' : 1,
                    'se_class' : 1,
                    'se_uri' : 1,
                    'se_sitekey' : 1,
                    'se_lang' : 1,
                    'se_area' : 1,
                    'se_year' : 1,
                    'se_year' : 1,
                    'show_name' : 1,
                    'se_director' : 1,
                    'url' : 1,
                    'title' : 1,
                    'latestEp' : 1,
                    'totalEp' : 1,
                    'newest_episode' : 1,
                    'videoPlay' : 1,
                    'vlink' : 1,
                    'station' : 1,
                    'se_sitename' : 1,
                    'se_maxepisode' : 1,
                    'director' : 1,
                    'actor' : 1,
                    'episode' : 1
                    }
            if 'result' in result_data['tplData']:
                for res_idx, res in enumerate(result_data['tplData']['result']):
                    to_remove_key_list = []
                    for res_k, res_v in res.iteritems():
                        if res_k not in key_filter_map:
                            to_remove_key_list.append(res_k)
                    for remove_key in to_remove_key_list:
                        result_data['tplData']['result'][res_idx].pop(remove_key, None)
            return error_msg, result_data
        return error_msg, result_data

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
