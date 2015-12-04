#!/usr/bin/env python
# -*- coding: gb18030 -*-
########################################################################
# 
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
# 
########################################################################
 
'''
File: url_saver.py
Author: liyudong(liyudong@baidu.com)
Date: 2014/06/18 14:52:42
'''

import json

class UrlSaver:
    '''
    save url content
    '''

    def __init__(self, save_path):
        '''
        class constructor
      
        Args:
            self :
            save_path :
        Return:   
        Raise: 
        '''

        self._data_path = save_path
        self.open()

    def save(self, url, content, data_key):
        '''
        save url content
      
        Args:
            self :
            url :
            content :
        Return: if error return error msg otherwise return None
        Raise: 
        '''

        if content is None:
            return None
        query = content['query']
        content.pop("query", None)
        content_str = json.dumps(content)
        try:
            if self.file_handle is None:
                return "fail to open file: %s"%(self._data_path)
            self.file_handle.write("%s\t%s\t%s\n"%(query, data_key, content_str))
        except IOError as e:
            return "fail to open file[%s]: %s"%(self._data_path, e)
        return None

    def open(self):
        try:
            self.file_handle = open(self._data_path, 'w')
        except IOError as e:
            self.file_handle = None

    def close(self):
        f.close()


if __name__ == "__main__":
    import url_downloader
    url = "爱情公寓"
    downloader = url_downloader.UrlDownloader()
    error_msg, html, redirected_url = downloader.download(url)
    saver = UrlSaver("../data/result.dict")
    error_msg = saver.save(url, html)
    print error_msg
