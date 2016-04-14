# -*- coding: utf-8 -*-
from time import time
import logging
from pyleus.storm import SimpleBolt


log = logging.getLogger("kafka_to_mongodb.split_bolt")


class SplitBolt(SimpleBolt):

    OUTPUT_FIELDS = ['msisdn', 'rec_opening_time', 'uplink', 'downlink']

    def initialize(self):
        """
        Instead of using normal python constructor, set instance member in this function
        """
        log.debug("initialize @ {0}".format(time()))
        self.counter = 0

    def process_tick(self):
        log.warning("tick @ {0}".format(time()))

    def process_tuple(self, tup):
        if self.counter == 0:
            log.warning("get flag start @ {0} |".format(time()))

        msg_dict, = tup.values  # {offset:message}
        offset = int(msg_dict.keys()[0])
        line = msg_dict.values()[0]

        if line.strip() == "end":
            log.warning("get flag end @ {0} |".format(time()))
            return

        raw_row = line.strip().split(",")
        self.emit((raw_row[6], raw_row[4], raw_row[17], raw_row[18]))
        # self.emit((raw_row[6], raw_row[4], raw_row[17], raw_row[18]), anchors=[tup])

        if offset % 10000 == 0:
            log.warning("get message #{0}".format(offset))

        self.counter += 1


if __name__ == '__main__':
    SplitBolt().run()