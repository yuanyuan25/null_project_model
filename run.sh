#!/bin/bash
set -o errexit
work_dir=`pwd`
cd $work_dir
source /d1/home/yuanyuan/.bashrc

spark-submit $work_dir/main.py >>1.log

# python $work_dir/main.py
