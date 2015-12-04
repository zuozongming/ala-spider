#!/usr/bin/env python
# -*- coding: gb18030 -*-
########################################################################
# 
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
# 
########################################################################
 
'''
File: url_downloader_test.py
Author: liyudong(liyudong@baidu.com)
Date: 2014/06/19 02:23:01
'''

import unittest

import logging
import os
import sys
sys.path.append('../src/')

import url_downloader

class UrlDownloaderTestCase(unittest.TestCase):
    '''
    url download test case
    '''

    def setUp(self):
        '''
        test case set up
      
        Args:
            self :
        Return:   
        Raise: 
        '''

        self.downloader = url_downloader.UrlDownloader()

    def tearDown(self):
        '''
        test case tear down
      
        Args:
            self :
        Return:   
        Raise: 
        '''

        pass

    def test_download(self):
        '''
        save html test case
      
        Args:
            self :
        Return:   
        Raise: 
        '''

        url = "http://www.baidu.com/"
        error_msg, html, redirect_url =  self.downloader.download(url)
        self.assertEqual(error_msg, None)


if __name__ == "__main__":
    unittest.main()

