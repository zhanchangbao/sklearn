from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split,from_json
from pyspark.sql.types import StructType,StringType,IntegerType,DoubleType
import os
import sys

# 集群运行环境
# os.environ['SPARK_HOME'] = r'/opt/modules/spark-2.2.1-bin-hadoop2.7'
# sys.path.append("/opt/modules/spark-2.2.1-bin-hadoop2.7/python/lib")
# sys.path.append("/opt/modules/spark-2.2.1-bin-hadoop2.7/python/lib/py4j-0.10.4-src.zip")

# 本地运行环境
os.environ['SPARK_HOME'] = 'D:\spark-2.2.1-bin-hadoop2.7'
sys.path.append('D:\spark-2.2.1-bin-hadoop2.7\python')

# test1
spark = SparkSession.builder.appName("test").getOrCreate()

lines = spark.readStream.format("socket").option("host","10.12.64.229").option("port",9999).load()
#
words = lines.select(explode(split(lines.value," ")).alias("word"))
#
# wordCounts = words.groupBy("word").count()
#
#
# query = wordCounts.writeStream \
#     .outputMode("Update") \
#     .format("console") \
#     .start()
#
# query.awaitTermination()

words = df.selectExpr("CAST(value AS STRING)")

schema = StructType().add("name",StringType()).add("age",StringType()).add("sex",StringType())

res = words.select(from_json("value",schema).alias("data")).select("data.*")
Counts = res.groupBy("name").count()
query = Counts.writeStream.format("console").outputMode("complete").start()
query1 = res.writeStream.format("console").outputMode("complete").start()

query.awaitTermination()