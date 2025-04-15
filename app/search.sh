#!/bin/bash
# app/search.sh
echo "Running the query application on YARN..."
source .venv/bin/activate

# Set PYSPARK DRIVER and EXECUTOR Python
export PYSPARK_DRIVER_PYTHON=$(which python)
export PYSPARK_PYTHON=./.venv/bin/python

# Pass the query as argument to query.py
spark-submit --master yarn --archives /app/.venv.tar.gz#.venv query.py "$1"
