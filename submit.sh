function logger { # function to print logs
  min_lth=40
  src_lth=${#1}
  actual_lth=${min_lth}
  blank_lth=1
  sub_lth=$(expr ${src_lth} - ${min_lth})
  TOP=""
  content="${1}"
  
  if [ ${sub_lth} -ge $(expr 2 + 2 \* ${blank_lth}) ]; \
  then \
    actual_lth=$(expr ${src_lth} + 2 + 2 \* ${blank_lth}); \
  else \
    blank_lth=$(expr ${min_lth} - ${src_lth} - 2); \
    blank_lth=$(expr ${blank_lth} / 2); \
  fi; \
  
  # splice bound line
  for((i=1;i<=${actual_lth};i++)); \
  do \
    TOP="${TOP}*"; \
  done; \
  
  # splice content
  for((i=1;i<=${blank_lth};i++)); \
  do \
    content=" ${content} "; \
  done; \
  content="*${content}*"
  
  echo "${TOP}"
  echo "${content}"
  echo "${TOP}"
}



set -e
clear
logger "START"

spark-submit \
--master yarn \
--deploy-mode client \
--class org.apache.spark.examples.SparkPi \
--name sparker_test \
main.py \
2

logger "SUCCEED"
