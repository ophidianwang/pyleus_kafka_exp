# -*- coding: utf-8 -*-

from threading import Thread
import time
from pykafka import KafkaClient
import logging

# create logger
logger = logging.getLogger('pykafka.cluster')
logger.setLevel(logging.DEBUG)

"""
class EmitThread(Thread):
    def __init__(self, threadName):
        super(EmitThread,  self ).__init__(name = threadName)
        self.messages = []

    def push(self, message):
        self.messages.append(message)

    def run(self):
        while True:
            if len(self.messages)!=0:
                print(self.messages.pop(0))
                time.sleep(1)

emit_threads = []
for i in range(0,4):
    new_thread = EmitThread("thread_{0}".format(i))
    new_thread.start()
    emit_threads.append(new_thread)

print("There are {0} emit_threads".format(len(emit_threads)))
"""

client = KafkaClient(hosts='172.17.24.217:9092,172.17.24.218:9092,172.17.24.219:9092')  # 建立kafka連線client
print("%s" % client.topics)  # 列出有哪些topic
topic = client.topics['cep_storm']  # 取得指定的kafka_topic物件

consumer = topic.get_balanced_consumer(consumer_group="pyleus-kafka_spout_example",
                                       zookeeper_connect='172.17.24.217:2181,172.17.24.218:2181,172.17.24.219:2181',
                                       consumer_timeout_ms=500,
                                       auto_commit_enable=False)  # 建立consumer

# consumer = topic.get_simple_consumer(consumer_group="exp1",consumer_timeout_ms=500)
cursor = 0
start_timestamp = time.time()
# 開始消耗kafka_message
for message in consumer:
    # message 為物件, 有兩個member: offset和value
    if message is not None:
        cursor += 1
        # print message.offset, message.value
        # emit_threads[cursor%4].push("#{0} {1}".format(message.offset, message.value))

    else:
        print "no more message"
        # break
    if cursor%10000 == 0:
        print("consume #{0} msg.".format(cursor))
    
    # if cursor >= 100000:
        # break
else:
    print("over")
end_timestamp = time.time()
print("spend {0} seconds consuming {1} messages".format(end_timestamp - start_timestamp, cursor))
# consumer.commit_offsets()  # 主動commit offset, auto_commit_enable=True的話就不用
