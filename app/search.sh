#!/bin/bash
# This script will run the query.py as a Spark job on YARN.

if [ -z "$1" ]; then
  echo "Usage: bash search.sh \"some query terms\""
  exit 1
fi

echo "This script will include commands to search for documents given the query using Spark RDD"

# Активируем твоё venv
source .venv/bin/activate

# Указываем Python
export PYSPARK_DRIVER_PYTHON=$(which python)
export PYSPARK_PYTHON=./.venv/bin/python

spark-submit \
  --master yarn \
  --deploy-mode client \
  --archives /app/.venv.tar.gz#.venv \
  query.py "$@"
