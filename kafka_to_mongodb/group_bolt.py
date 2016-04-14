# -*- coding: utf-8 -*-
from time import time
import logging
from collections import defaultdict
from pyleus.storm import SimpleBolt


log = logging.getLogger("kafka_to_mongodb.group_bolt")


class GroupBolt(SimpleBolt):

    OUTPUT_FIELDS = ['date', 'hour', 'msisdn', 'total_uplink', 'total_downlink', "records"]

    def initialize(self):
        """
        Instead of using normal python constructor, set instance member in this function
        """
        log.debug("initialize @ {0}".format(time()))
        self.total_uplink = {}
        self.total_downlink = {}
        self.total_records = {}
        self.counter = 0

    def process_tick(self):
        log.warning("tick @ {0}".format(time()))
        self.toNextBolt()

    def process_tuple(self, tup):
        msisdn = tup.values[0]
        record_time = tup.values[1]
        date_time = record_time.split(" ")
        date = date_time[0]
        hour = date_time[1].split(":")[0]
        try:
            uplink = int(tup.values[2])
            downlink = int(tup.values[3])
        except :
            log.error("transforming tup values failed: %s", tup)
            return

        self.checkTotalList(date, hour)

        self.total_uplink[date][hour][msisdn] += uplink
        self.total_downlink[date][hour][msisdn] += downlink
        self.total_records[date][hour][msisdn].append(record_time)
        
    def toNextBolt(self):
        """
        將累計的同個msisdn的uplink & downlink emit給下個bolt
        並重新累計
        """
        for date in self.total_uplink:
            for hour in self.total_uplink[date]:
                for msisdn in self.total_uplink[date][hour]:
                    # see if we could pass list in storm tuple: False, emit members needs to be hashable
                    # so ... merge list to str
                    merged = ",".join(self.total_records[date][hour][msisdn])
                    # log.debug("%s", [msisdn, merged])
                    self.emit([date, hour, msisdn, self.total_uplink[date][hour][msisdn],
                                self.total_downlink[date][hour][msisdn], merged])

        # clear accumulator
        self.total_uplink.clear()
        self.total_downlink.clear()
        self.total_records.clear()
        self.counter = 0
    
    def checkTotalList(self, date, hour):
        if date not in self.total_uplink:
            self.total_uplink[date] = {}
            self.total_downlink[date] = {}
            self.total_records[date] = {}
        if hour not in self.total_uplink[date]:
            self.total_uplink[date][hour] = defaultdict(int)
            self.total_downlink[date][hour] = defaultdict(int)
            self.total_records[date][hour] = defaultdict(list)


if __name__ == '__main__':
    GroupBolt().run()