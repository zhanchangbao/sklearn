from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
import os
import sys

# 集群运行环境
# os.environ['SPARK_HOME'] = r'/opt/modules/spark-2.2.1-bin-hadoop2.7'
# sys.path.append("/opt/modules/spark-2.2.1-bin-hadoop2.7/python/lib")
# sys.path.append("/opt/modules/spark-2.2.1-bin-hadoop2.7/python/lib/py4j-0.10.4-src.zip")

# 本地运行环境
os.environ['SPARK_HOME'] = 'D:\spark-2.2.1-bin-hadoop2.7'
sys.path.append('D:\spark-2.2.1-bin-hadoop2.7\python')

def start():
    sc = SparkContext("local[2]","NetWordCount")
    ssc = StreamingContext(sc,5)

    rdd0 = ssc.socketTextStream("10.12.64.229",9999)
    # rdd1 = rdd0.flatMap(lambda x:flatmapFunc(x))
    pairrdd = rdd0.map(lambda x:(x.get("shopid"),1))
    # rdd2 = pairrdd.reduceByKey(lambda x,y:test())
    # rdd2.foreachRDD(lambda x:forex)
    pairrdd.pprint()
    ssc.start()
    ssc.awaitTermination()

def mapFunc(x):
    return (json.loads(x).get("shopid"),1)

def flatmapFunc(x):
    x = json.loads(x)
    return (x.get("shopid"),x.get("shopname")),(x.get("shopid"),x.get("lat")),(x.get("shopid"),x.get("lng"))

if __name__ == '__main__':
    start()



