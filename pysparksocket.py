# -*- coding:utf-8 -*-
import os
import sys
from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext

#配置环境变量并导入pyspark
os.environ['SPARK_HOME'] = r'/opt/modules/spark-2.2.1-bin-hadoop2.7'
sys.path.append("/opt/modules/spark-2.2.1-bin-hadoop2.7/python/lib")
sys.path.append("/opt/modules/spark-2.2.1-bin-hadoop2.7/python/lib/py4j-0.10.4-src.zip")

appName ="test" #应用程序名称
master= "spark://10.12.64.229:7070" # 主节点
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc,1)

lines = ssc.socketTextStream("10.12.64.229",9999)
words = lines.flatMap(lambda line:line.split(' '))
pairs = words.map(lambda word:(word,1))
wordCounts = pairs.reduceByKey(lambda x,y: x + y)
wordCounts.pprint()
outputFile = "/streaming/ss"
wordCounts.saveAsTextFiles(outputFile)

ssc.start()
ssc.awaitTermination()