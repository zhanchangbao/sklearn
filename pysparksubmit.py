# -*- coding:utf-8 -*-
import os
import sys
from pyspark import SparkContext, SparkConf

#配置环境变量并导入pyspark
# 集群运行环境
# os.environ['SPARK_HOME'] = r'/opt/modules/spark-2.2.1-bin-hadoop2.7'
# sys.path.append("/opt/modules/spark-2.2.1-bin-hadoop2.7/python/lib")
# sys.path.append("/opt/modules/spark-2.2.1-bin-hadoop2.7/python/lib/py4j-0.10.4-src.zip")
# 本地运行环境
os.environ['SPARK_HOME'] = 'D:\spark-2.2.1-bin-hadoop2.7'
sys.path.append('D:\spark-2.2.1-bin-hadoop2.7\python')

appName ="test" #应用程序名称
master= "spark://10.12.64.229:7070" # 主节点hostname
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)
res = distData.reduce(lambda a, b: a + b)
print("===========================================")
print (res)
print("===========================================")