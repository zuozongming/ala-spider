#!/bin/bash
#�ܿؽű�
#������������ֵ
#0 ����
#1 �ɼ���
#-1 �˳�

#����·��
base_script_dir=$(cd "$(dirname "$0")"; cd ../; pwd)
PYTHON_BIN="/home/tools/tools/python/2.7.2/64/bin/python2.7"
#����·��
crawler_query_path="${base_script_dir}/data/crawler_query.dict"
crawler_result_path="${base_script_dir}/data/crawler_result.dict"
monitor_query_path="${base_script_dir}/data/monitor_query.dict"

MAIL_LIST="duanjiawang@baidu.com"
