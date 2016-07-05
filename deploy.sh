# !/bin/bash
LOCAL_HOME=`pwd`
DATA_DIR=/d3/${USER}/null_project
HDFS_HOME=/personal/${USER}/null_project

# mkdir
mkdir -p $DATA_DIR/data/bin_data
mkdir -p $DATA_DIR/data/raw_data
mkdir -p $DATA_DIR/data/export
mkdir -p $DATA_DIR/log
# mkdir static

#  ln -s 
ln -s $DATA_DIR/data
ln -s $DATA_DIR/log

# hdfs mkdir
hadoop fs -mkdir -p $HDFS_HOME

# sed
# sed -i "s#work_dir.*=.*#work_dir=$LOCAL_HOME#g" run.sh
# sed -i "s#hdfs_dir.*=.*#hdfs_dir=$HDFS_HOME#g" conf/main.cfg
