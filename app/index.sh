#!/bin/bash

echo "This script runs mapreduce jobs via Hadoop streaming to index documents."

INPUT_PATH=${1:-/index/data}
OUTPUT_PATH1=/tmp/index/output_1
OUTPUT_PATH2=/tmp/index/output_2

# Важно! Выключить YARN
export HADOOP_CONF_DIR=""
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_YARN_HOME=$HADOOP_HOME

# 1. Первый пайплайн: inverted index + doc_length
echo "Running first MapReduce (Inverted Index)..."
hdfs dfs -rm -r -f $OUTPUT_PATH1

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -D mapreduce.framework.name=local \
  -input "$INPUT_PATH" \
  -output "$OUTPUT_PATH1" \
  -file mapreduce/mapper1.py \
  -file mapreduce/reducer1.py \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py"

# 2. Второй пайплайн: term statistics
echo "Running second MapReduce (Statistics)..."
hdfs dfs -rm -r -f $OUTPUT_PATH2

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -D mapreduce.framework.name=local \
  -input "$INPUT_PATH" \
  -output "$OUTPUT_PATH2" \
  -file mapreduce/mapper2.py \
  -file mapreduce/reducer2.py \
  -mapper "python3 mapper2.py" \
  -reducer "python3 reducer2.py"
