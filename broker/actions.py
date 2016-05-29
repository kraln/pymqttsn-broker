import logging
from broker import message

logger = logging.getLogger('broker.actions')

TYPE_LUT = message.TYPE_LUT

class MQTTSNActions:
    @staticmethod
    def handle(message, addr):
        """ Perform action based on message type """
        if message.message_type == TYPE_LUT['CONNECT']:
            MQTTSNActions.handle_connect(message, addr)
            pass
        elif message.message_type == TYPE_LUT['PINGREQ']:
            pass

    @staticmethod
    def handle_connect(message, addr):
        # add entry in redis

        # send back the CONNACK
        pass
