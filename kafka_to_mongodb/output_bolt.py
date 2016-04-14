# -*- coding: utf-8 -*-
from time import time
import logging
from pymongo import MongoClient
from pyleus.storm import SimpleBolt


log = logging.getLogger("kafka_to_mongodb.output_bolt")


class OutputBolt(SimpleBolt):

    OUTPUT_FIELDS = ['dummy']
    mongo_host = "172.17.24.211"
    mongo_port = 27017
    mongo_db = "cep_storm"
    mongo_collection = "pyleus_result"

    def initialize(self):
        """
        Instead of using normal python constructor, set instance member in this function
        """
        log.debug("initialize @ {0}".format(time()))
        self.client = MongoClient(OutputBolt.mongo_host, OutputBolt.mongo_port)
        self.db = self.client[OutputBolt.mongo_db]
        self.collection = self.db[OutputBolt.mongo_collection]
        self.counter = 0

    def process_tick(self):
        log.warning("tick @ {0}".format(time()))

    def process_tuple(self, tup):
        doc = {"date": tup.values[0],
               "hour": tup.values[1],
               "msisdn": tup.values[2],
               "total_uplink": tup.values[3],
               "total_downlink": tup.values[4],
               "records": tup.values[5].split(",")  # split str to list
               }
        object_id = self.collection.insert_one(doc).inserted_id
        self.counter += 1


if __name__ == '__main__':
    OutputBolt().run()