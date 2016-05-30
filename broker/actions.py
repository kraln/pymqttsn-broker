import logging
import redis
import time
import pickle
import struct

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
        elif message.message_type == TYPE_LUT['REGISTER']:
            MQTTSNActions.handle_register(message, addr)
        elif message.message_type == TYPE_LUT['PINGREQ']:
            MQTTSNActions.handle_pingreq(message, addr)
        elif message.message_type == TYPE_LUT['DISCONNECT']:
            MQTTSNActions.handle_disconnect(message, addr)
        elif message.message_type == TYPE_LUT['PUBLISH']:
            MQTTSNActions.handle_publish(message, addr)
        elif message.message_type == TYPE_LUT['SUBSCRIBE']:
            MQTTSNActions.handle_subscribe(message, addr)

    @staticmethod
    def queue(destination, payload):
        # add to the outgoing queue for that broker
        logger.debug('Queued message for %s' % destination)
        keeptime = config.getint('redis', 'keepalive') + 1
        r.rpush('%s:queue' % (destination,),  payload)
        r.expire('%s:queue' % (destination,), int(time.time() + keeptime))

    @staticmethod
    def create_connack(message):
        result = bytes([3, TYPE_LUT['CONNACK'], 0]) # 0 means accepted
        return result

    @staticmethod
    def create_regack(message):
        topic_id = r.zrank('topics', message.topic_name)
        topic_id_bytes = struct.pack(">H", topic_id)

        result = bytes(
                [7, TYPE_LUT['REGACK'], # 7 byte response
                topic_id_bytes[0], topic_id_bytes[1], # the topic id
                message.message_id[1], message.message_id[2], # the message id
                0])
        return result

    @staticmethod
    def create_pingresp(message):
        result = bytes([2, TYPE_LUT['PINGRESP']])
        return result

    @staticmethod
    def create_disconnect(message):
        result = bytes([2, TYPE_LUT['DISCONNECT']])
        return result

    @staticmethod
    def create_puback(message):
        result = bytes(
                [7, TYPE_LUT['PUBACK'], # 7 byte response
                message.topic_id[1], message.topic_id[2], # the topic id
                message.message_id[1], message.message_id[2], # the message id
                0])
        return result

    @staticmethod
    def create_suback(message, res):
        if res is None:
            # no match topic, tell the client it failed
            result = 2 #congestion?
            topic_id = (0, 0,)
        else:
            result = 0
            topic_id = struct.pack(">H", res)

        reply = bytes(
                [9,
                 TYPE_LUT['SUBACK'],
                 0,
                 0,
                 topic_id[0],
                 topic_id[1],
                 message.message_id[1],
                 message.message_id[2],
                 result,])

        return reply

    @staticmethod
    def handle_subscribe(message, addr):
        addr_s = pickle.dumps(addr)
        wildcard = False

        # check if the subscription is to a named topic
        if hasattr(message, 'topic_name'):
            # check for wildcard characters
            if '#' in message.topic_name or '+' in message.topic_name:
            # if yes? leave it so, result is 0
                wildcard = True
                exists = True
            else:
            # if no, check to make sure topic exists
            ### get by name
                exists = r.zscore('topics', message.topic_name)
        else:
            # check to make sure topic exists
            ### get by id
            exists = r.zrange('topics', message.topic_id[0], message.topic_id[0])

        if wildcard:
            result = 0
        elif exists:
            result = exists
        else:
            result = None

        if result is not None:
        # look up the client id for this address
        # add a subscription row (topic, client id)
            pass

        # queue response
        MQTTSNActions.queue(addr_s, MQTTSNActions.create_suback(message, result))

    @staticmethod
    def handle_publish(message, addr):
        addr_s = pickle.dumps(addr)

        # TODO: scan subscriptions for the topic id

            # TODO: for each, send the publish message

        logger.debug("Got publish of '%s' to topic id %d" % (
            message.message, message.topic_id[0],))

        # queue the response
        MQTTSNActions.queue(addr_s, MQTTSNActions.create_disconnect(message))

    @staticmethod
    def handle_disconnect(message, addr):
        addr_s = pickle.dumps(addr)

        # Remove all the stuff for this client
        # If I remove the socket, I can't reply to the disconnect...
        # r.remove('%s:socket' % addr_s)
        r.zrem('%s:clients' % broker.myid(), addr_s)

        # socket for this client
        r.hmset('%s:socket' % addr_s,
                {
                    'last_message': int(time.time()),
                    'will_be_disconnected': True
                }
            )


        # TODO: handle sleepy clients (clients with duration)

        # queue the response
        MQTTSNActions.queue(addr_s, MQTTSNActions.create_disconnect(message))

    @staticmethod
    def handle_pingreq(message, addr):
        addr_s = pickle.dumps(addr)

        # socket for this client
        r.hmset('%s:socket' % addr_s,
                {
                    'last_message': int(time.time()),
                }
            )

        # queue the response
        MQTTSNActions.queue(addr_s, MQTTSNActions.create_pingresp(message))

    @staticmethod
    def handle_register(message, addr):
        logger.debug("Handling REGISTER")

        addr_s = pickle.dumps(addr)

        # upsert the topic
        r.zadd('topics', message.topic_name, int(time.time()))

        # socket for this client
        r.hmset('%s:socket' % addr_s,
                {
                    'last_message': int(time.time()),
                }
            )

        # queue the ack
        MQTTSNActions.queue(addr_s, MQTTSNActions.create_regack(message))


    @staticmethod
    def handle_connect(message, addr):
        logger.debug("Handling CONNECT")

        keeptime = config.getint('redis', 'keepalive') + 1
        addr_s = pickle.dumps(addr)

        # tracking entry in redis (will be expired manually)
        r.zadd('%s:clients' % broker.myid(),
                addr_s,
                int(time.time()) + message.duration)

        # socket for this client
        r.hmset('%s:socket' % addr_s,
                {
                    'broker_id': broker.myid(),
                    'client_id': message.client_id,
                    'last_message': int(time.time())
                }
            )

        # be sure to clean up
        r.expire('%s:socket' % addr_s, keeptime)

        # broker lookup entry
        r.setex('%s:broker' % addr_s, broker.myid(), message.duration)

        # TODO: if clean session is set, expire stuff related
        # to this this client right now

        # queue the CONNACK
        MQTTSNActions.queue(addr_s, MQTTSNActions.create_connack(message))
