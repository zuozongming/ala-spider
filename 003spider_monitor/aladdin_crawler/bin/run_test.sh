#!/bin/bash
usage() { 
    echo "Usage: $0 [-c <crawler_conf_path>] [-q <query_file_path>] [-o <result_file_path>]" 1>&2
    exit 1
}
TEMP=`getopt -o hc:q:o: --long test:,help -- $@`
if [[ $? != 0 ]]; then
    usage
fi
eval set -- $TEMP

HELP=""
crawler_conf_path=""
query_file_path=""
result_file_path=""

while true;do
    case "$1" in
        '-c') crawler_conf_path=$2
              shift 2;; 
        '-q') query_file_path=$2
              shift 2;; 
        '-o') result_file_path=$2
              shift 2;; 
        '-h') HELP=1
              shift;;
        '--help') HELP=1
              shift;;
        '--') shift; break;;
    esac
done

if [[ $HELP == 1 || $crawler_conf_path == "" || $query_file_path == "" || $result_file_path == "" ]];then
    usage
fi
echo "crawler_conf_path=${crawler_conf_path=}"
echo "query_file_path=${query_file_path}"
echo "result_file_path=${result_file_path}"
process_num=200
digit_num=3
PYTHON_BIN="/home/tools/tools/python/2.7.2/64/bin/python"

rm crawl.log*
#crawler_conf_path="../conf/crawler.conf"
#query_file_path="../data/query.dict"
#result_file_path="../data/result.dict"
dict_suffix=".sub_"
total=`wc -l < ${query_file_path}`
workload=$((total / process_num))

result_file_num=${process_num}
if [ $((workload * result_file_num)) -ne $total ]; then
    if [ $workload == 0 ]; then
        workload=1
        result_file_num=$total
    else
        result_file_num=$((total / workload))
        if [ $((workload * result_file_num)) -ne $total ]; then
            result_file_num=$((result_file_num + 1))
        fi
    fi
fi
#rm ${query_file_path}${dict_suffix}*
#rm ${result_file_path}${dict_suffix}*
crawler_result_file_path=${result_file_path}".crawler"
split -l ${workload} -d ${query_file_path} -a ${digit_num} ${query_file_path}${dict_suffix}
for i in `seq 0 $((result_file_num - 1))`
do
    sub_file_suffix=`printf "%0${digit_num}d" ${i}`
    sub_query_file_path=${query_file_path}${dict_suffix}${sub_file_suffix}
    sub_result_file_path=${crawler_result_file_path}${dict_suffix}${sub_file_suffix}
    $PYTHON_BIN aladdin_crawler.py -c ${crawler_conf_path} -q ${sub_query_file_path} -o ${sub_result_file_path} &
done
wait
for i in `seq 0 $((result_file_num - 1))`
do
    sub_file_suffix=`printf "%0${digit_num}d" ${i}`
    sub_query_file_path=${query_file_path}${dict_suffix}${sub_file_suffix}
    sub_result_file_path=${crawler_result_file_path}${dict_suffix}${sub_file_suffix}
    if [ $i == 0 ]; then
        cat ${sub_result_file_path} > ${crawler_result_file_path}
    else
        cat ${sub_result_file_path} >> ${crawler_result_file_path}
    fi
    rm ${sub_query_file_path}
    rm ${sub_result_file_path} 
done
