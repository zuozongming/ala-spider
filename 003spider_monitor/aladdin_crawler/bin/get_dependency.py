#!/usr/bin/env python
# -*- coding: gb18030 -*-
########################################################################
# 
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
# 
########################################################################
 
"""
��ù�����ĳ��.py�ļ����������ļ�����·��
File: get_depend_file.py
Author: liupu(liupu@baidu.com)
Date: 2014/09/09 17:22:25
"""

import sys
import os
import modulefinder

# ��ӹ�����python�ļ���·��
ROOT_PATH = os.path.split(os.path.dirname(os.path.realpath(__file__)))[0]
BIN_PATH = os.path.join(ROOT_PATH, "bin")
INCLUDES_PATH = os.path.join(ROOT_PATH, "includes")
PYLIB_PATH = os.path.join(ROOT_PATH, "pylib")
UTILS_PATH = os.path.join(ROOT_PATH, "utils")
if not BIN_PATH in sys.path:
    sys.path.append(BIN_PATH)
if not INCLUDES_PATH in sys.path:
    sys.path.append(INCLUDES_PATH)
if not PYLIB_PATH in sys.path:
    sys.path.append(PYLIB_PATH)
if not UTILS_PATH in sys.path:
    sys.path.append(UTILS_PATH)

jobs = ["build_feature", "compute_req", "delivery_result", "generate_result", "pre_process"]
JOB_ROOT = os.path.join(ROOT_PATH, "jobs")
for job in jobs:
    job_path = os.path.join(JOB_ROOT, job)
    job_bin = os.path.join(job_path, "bin")
    job_includes = os.path.join(job_path, "includes")
    job_so = os.path.join(job_path, "so")
    # ��so�µĸ�����Ŀ¼Ҳ����
    filenames = []
    if os.path.exists(job_so):
        filenames = os.listdir(job_so)
    for fn in filenames:
        # ���������ļ�
        if fn.startswith("."):
            continue
        fn = os.path.join(job_so, fn)
        if os.path.isdir(fn) and (not fn in sys.path):
            sys.path.append(fn)
    if not job_bin in sys.path:
        sys.path.append(job_bin)
    if not job_includes in sys.path:
        sys.path.append(job_includes)
    if not job_so in sys.path:
        sys.path.append(job_so)

def get_file_name(file_name):
    if file_name.rfind(".pyc") == len(file_name) - 4:
        return file_name[0:-1]
    else:
        return file_name

def get_depend_file(file_name):
    """
    @brief
        ��õ���pyhon�ļ�������������python�ļ���
    @param
        file_name   str     �ļ���
    @retval
        ���������ļ��б�
    """
    finder = modulefinder.ModuleFinder()
    finder.run_script(get_file_name(file_name))
    depend_files = set()
    for name, mod in finder.modules.items():
        if mod.__file__ is None:
            continue
        file_path = "%s" % (mod.__file__)
        if file_path.find("/python") == -1:
            depend_files.add(file_path)
    return depend_files


def get_depend_path(file_name):
    """
    @brief
        ��õ���python��������·��
    @param
        file_name   str     �ļ���
    @retval
        ��������·���б�
    """
    finder = modulefinder.ModuleFinder()
    finder.run_script(get_file_name(file_name))

    need_depend_path = set()
    for name, mod in finder.modules.items():
        if mod.__file__ is None:
            continue
        file_path = "%s" % (mod.__file__)
        if file_path.find("/python") == -1:
            need_depend_path.add(os.path.split(file_path)[0])
    return need_depend_path

# һ������
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: python %s <file_path>" % (sys.argv[0])
        sys.exit(-1)
    file_name = sys.argv[1]
    print "=" * 42 + "��������·���б�" + "=" * 42
    for path in get_depend_path(file_name):
        print path
    
    print "=" * 42 + "���������ļ��б�" + "=" * 42
    for dpf in get_depend_file(file_name):
        print dpf

#/* vim: set ts=4 sw=4 sts=4 tw=100 */
