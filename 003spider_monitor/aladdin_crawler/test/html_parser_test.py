#!/usr/bin/env python
# -*- coding: gb18030 -*-
########################################################################
# 
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
# 
########################################################################
 
'''
File: html_parser_test.py
Author: liyudong(liyudong@baidu.com)
Date: 2014/06/19 02:22:12
'''

import unittest

import logging
import sys
sys.path.append('../src/')

import html_parser

class HtmlParserTestCase(unittest.TestCase):
    '''
    html parser test case
    '''

    def setUp(self):
        '''
        test case set up
      
        Args:
            self :
        Return:   
        Raise: 
        '''

        self.parser = html_parser.HtmlParser()

    def tearDown(self):
        '''
        test case tear down
      
        Args:
            self :
        Return:   
        Raise: 
        '''

        pass

    def test_filter_links(self):
        '''
        test filter links
      
        Args:
            self :
        Return:   
        Raise: 
        '''

        url = "http://www.baidu.com"
        html = "<html><a href=\"s?wd=test\"/></html>"
        links = self.parser.filter_links(url, html)
        self.assertEqual(len(links), 1)
        self.assertEqual("http://www.baidu.com/s?wd=test", links[0])


if __name__ == "__main__":
    unittest.main()

