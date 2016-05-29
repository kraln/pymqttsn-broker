import logging
import redis
import time
import pickle

from config.config import config
from broker import message
from broker import broker

logger = logging.getLogger('broker.actions')
r = redis.Redis(config.get('redis', 'host'))

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
    def queue(destination, payload):
        # add to the outgoing queue for that broker
        r.rpush('%s:queue' % (destination,),  payload)

    @staticmethod
    def create_connack(message):
        result = bytes([3, TYPE_LUT['CONNACK'], 0]) # 0 means accepted
        return result

    @staticmethod
    def handle_connect(message, addr):
        keeptime = config.getint('redis', 'keepalive') + 1

        # tracking entry in redis (will be expired manually)
        r.zadd('%s:clients' % broker.myid(),
                message.client_id,
                int(time.time()) + message.duration)

        # socket for this client
        r.hmset('%s:socket' % message.client_id,
                {
                    'broker_id': broker.myid(),
                    'socket': pickle.dumps(addr),
                    'last_message': int(time.time())
                }
            )

        # be sure to clean up
        r.expire('%s:socket' % message.client_id, keeptime)

        # broker lookup entry
        r.setex('%s:broker' % message.client_id, broker.myid(), message.duration)

        # TODO: if clean session is set, expire stuff related
        # to this this client right now

        # queue the CONNACK
        MQTTSNActions.queue(message.client_id,
                MQTTSNActions.create_connack(message))
