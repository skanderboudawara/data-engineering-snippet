#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <filename>"
    exit 1
fi

docker-compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --conf "spark.log.level=ERROR" \
    /transform/"$1"
