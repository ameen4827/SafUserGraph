#!/bin/bash
now=$(date +"%Y-%m-%d-%S")
cd /home

echo "Triggering Spark Submit"

echo " Executing command - "
echo "spark-submit --master yarn --deploy-mode cluster  --conf spark.app.name=usernetwork --num-executors  2 --driver-memory 4g --executor-memory 6g --executor-cores 4  --class org.smaf.Parsers.TweetParser SocialUserNetwork-csv.jar"
spark-submit --master yarn --deploy-mode cluster  --conf spark.app.name=usernetwork --num-executors  2 --driver-memory 4g --executor-memory 6g --executor-cores 4  --class org.smaf.Parsers.TweetParser /home/SocialUserNetwork-csv.jar