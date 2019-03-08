from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
import pymysql
import json

def main():
    connection = pymysql.connect(
        host='10.12.64.229',
        user='root',
        password='123456',
        db='test',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    cursor = connection.cursor()
    start()

def start():
    sconf = SparkConf()
    sconf.set('spark.cores.max', 3)
    sc = SparkContext(appName='pysparkdfka', conf=sconf)
    ssc = StreamingContext(sc, 5)
    brokers = "10.12.64.205:9092"
    topic = 'test'
    stream = KafkaUtils.createDirectStream(ssc, [topic], kafkaParams={"metadata.broker.list": brokers})
    json = stream.map(lambda v : v[1])
    json.foreachRDD(lambda rdd : rdd.foreach(echo))

    json.pprint()
    ssc.start()
    ssc.awaitTermination()

def echo(rdd):
    rdd = json.loads(rdd)
    shopid = rdd.get('id', '')
    shopname = rdd.get('shopname', '')
    shoptype = rdd.get('shoptype', '')
    lat = rdd.get('lat', '')
    lng = rdd.get('lng', '')
    obtained = rdd.get('obtained', '')
    recommend = rdd.get('recommend', '')
    sql = "insert into test(shopname,shoptype,shoplat,shoplng,obtained,recommend) \
    values ('{}','{}','{}','{}','{}','{}')" .format(shopname,shoptype,lat,lng,obtained,recommend)
    cursor.execute(sql)
    connection.commit()

if __name__ == '__main__':
    main()
