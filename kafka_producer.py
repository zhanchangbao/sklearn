# -* coding:utf8 *-
import time
import json
import uuid
import random
import threading
from pykafka import KafkaClient

# 创建kafka实例
hosts = '10.12.64.205:9092'
client = KafkaClient(hosts=hosts)

# 打印一下有哪些topic
print(client.topics)

# 创建kafka producer句柄
topic = client.topics['test2']
producer = topic.get_producer()

# work
def work():
    while 1:
        msg = json.dumps({
            "shopid": str(uuid.uuid4()).replace('-', ''),
            "shopname":"jiyongtest",
            "shoptype": random.randint(1, 2),
            "shoplat":random.randint(12, 100),
            "shoplng": random.randint(12, 100),
            "obtained": random.randint(12, 100),
            "recommend": random.randint(12, 100)})
        producer.produce(bytes(msg, encoding='utf-8'))

# 多线程执行
thread_list = [threading.Thread(target=work) for i in range(10)]
for thread in thread_list:
    thread.setDaemon(True)
    thread.start()

time.sleep(60)

# 关闭句柄, 退出
producer.stop()