# coding=utf-8
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F
import os
import sys

# 集群运行环境
# os.environ['SPARK_HOME'] = r'/opt/modules/spark-2.2.1-bin-hadoop2.7'
# sys.path.append("/opt/modules/spark-2.2.1-bin-hadoop2.7/python/lib")
# sys.path.append("/opt/modules/spark-2.2.1-bin-hadoop2.7/python/lib/py4j-0.10.4-src.zip")

# 本地运行环境
os.environ['SPARK_HOME'] = 'D:\spark-2.2.1-bin-hadoop2.7'
sys.path.append('D:\spark-2.2.1-bin-hadoop2.7\python')

# 程序
spark = SparkSession.builder\
    .master("local[*]")\
    .appName("test.dataframe")\
    .getOrCreate()

# df = spark \
#   .readStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "10.12.64.205:9092") \
#   .option("subscribe", "greetings") \
#   .load()
# df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# 第1步，加载数据，默认为字符串类型的单列，列名为value
data = {"a":1,"b":2}
df = spark.createDataFrame(data, T.StringType())
df.printSchema()
df.show()

schema = T.JsonType(T.StructType([
    T.StructField("a", T.IntegerType()),
    T.StructField("b", T.IntegerType())
]))

# 第2步，将列转为数组类型
df = df.select(F.from_json(df["value"], schema).alias("json"))
df.printSchema()
df.show()

# 第3步，将列转为Struct类型
df = df.select(F.explode(df["json"]).alias("col"));
df.printSchema()
df.show()

# 第4步，对Struct进行拆分
col = df["col"]
df = df.withColumn("a", col["a"]) \
    .withColumn("b", col["b"]) \
    .drop("col")
df.printSchema()
df.show()
#
# print("success")