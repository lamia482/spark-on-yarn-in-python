'''
 > File: main.py
 > Date: 2018/06/11
 > Rule: get started with spark in python
'''
import os
import sys
import time
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from common.logger import logger

spark = SparkSession.builder\
        .enableHiveSupport()\
        .getOrCreate()

__LOCALPATH__ = '/home/e0024/workspace/python/spark'


def local_path(path):
  '''
  '''
  p = 'file:' + os.path.join(__LOCALPATH__, path)
  logger.debug('transform file: {} to local path: {}'.format(path, p))
  return p
  

def hdfs_path(path):
  '''
  '''
  p = os.path.join(
        'hdfs://master0:9000/user/e0024/', 
        path)
  logger.debug('transform file: {} to hdfs path: {}'.format(path, p))
  return p


def init_spark():
  '''
  '''
  try:
    spark
  except NameError as ex:
    spark = SparkSession.builder\
            .enableHiveSupport()\
            .getOrCreate()
  logger.info('Initialize spark')
  return spark
  

def close_spark():
  '''
  '''
  spark.stop()
  logger.info('Close spark')
  return
  

def keep():
  '''
  '''
  while True:
    logger.info('detect alive...')
    time.sleep(1)
  return


def go_0():
  '''
  '''
  sc = spark.sparkContext
  sc.setLogLevel('WARN')
  textFile = sc.textFile(hdfs_path('file.txt'))
  logger.info('rows: {}'.format(textFile.count())) # to get the number of rows in this DataFrame
  logger.info('first: {}'.format(textFile.first()))
  linesWithCtx = textFile.filter(lambda line: 'Hello' in line)
  logger.info('ctx:: {}'.format(linesWithCtx.count()))
  

if __name__ == '__main__':
  '''main entry
  '''
  spark = init_spark()
  go_0()
  # keep()
  close_spark()
