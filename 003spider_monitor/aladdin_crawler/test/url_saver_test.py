#!/usr/bin/env python
# -*- coding: gb18030 -*-
########################################################################
# 
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
# 
########################################################################
 
'''
File: url_saver_test.py
Author: liyudong(liyudong@baidu.com)
Date: 2014/06/19 02:23:11
'''

import unittest

import logging
import os
import sys
sys.path.append('../src/')

import url_saver

class UrlSaverTestCase(unittest.TestCase):
    '''
    url save test case
    '''

    def setUp(self):
        '''
        test case set up
      
        Args:
            self :
        Return:   
        Raise: 
        '''

        self.saver = url_saver.UrlSaver("./")

    def tearDown(self):
        '''
        test case tear down
      
        Args:
            self :
        Return:   
        Raise: 
        '''

        os.remove("./www.baidu.com")

    def test_save(self):
        '''
        save file test case
      
        Args:
            self :
        Return:   
        Raise: 
        '''

        url = "http://www.baidu.com/"
        html = "<html>test</html>"
        error_msg = self.saver.save(url, html)
        self.assertEqual(error_msg, None)


if __name__ == "__main__":
    unittest.main()
