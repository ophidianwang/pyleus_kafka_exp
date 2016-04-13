from time import time
import logging, logging.handlers
import logging.config

from pyleus.storm import SimpleBolt

log = logging.getLogger("logging_example.logger_bolt")


class LoggerBolt(SimpleBolt):

    OUTPUT_FIELDS = ["dummy"]
    cursor = 0

    def process_tick(self):
        log.warning("tick @ {0}".format(time()))

    def process_tuple(self, tup):
        if self.cursor == 0:
            log.warning("get flag start @ {0}".format(time()))
        """
        values = tup.values
        if self.cursor % 100 == 0:
            log.warning("#{0} : {1}".format(self.cursor,values))
        """
        msg_dict, = tup.values  # {offset:message}
        offset = int(msg_dict.keys()[0])
        line = msg_dict.values()[0]
        if offset % 10000 == 0:
            log.warning("#{0} : {1}".format(offset,line))

        self.cursor += 1
        # timestamp = tup.values[0]
        # log.info("Received: %r", timestamp)
        if line.strip() == "end":
            log.warning("get flag end @ {0}".format(time()))

if __name__ == '__main__':
    """ set in pyleus_logging.conf
    formatter = logging.Formatter('[%(asctime)s][%(name)s][%(levelname)s]%(message)s')
    socketHandler = logging.handlers.SocketHandler('172.17.24.217', logging.handlers.SYSLOG_UDP_PORT)
    socketHandler.setFormatter(formatter)
    socketHandler.setLevel(logging.WARNING)
    log.addHandler(socketHandler)
    log.setLevel(logging.WARNING)
    """
    LoggerBolt().run()
