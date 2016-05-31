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
        if payload is None:
            logger.info('Cowardly refusing to queue empty payload')
            return
        
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
            try:
                topic_id = struct.pack(">H", res)
            except Exception:
                logger.error(res)
                return None
       
        reply = bytes(
                [8,
                 TYPE_LUT['SUBACK'],
                 0,
                 topic_id[0],
                 topic_id[1],
                 message.message_id[1],
                 message.message_id[2],
                 result,])

        return reply
    @staticmethod
    def create_publish(message):
        content = message.message
        reply = bytes([
                7 + len(content),
                TYPE_LUT['PUBLISH'],
                0, #flags
                message.topic_id[1],
                message.topic_id[2],
                0, # message id
                0, # message id
                ]) + content.encode()
        return reply

    @staticmethod
    def handle_subscribe(message, addr):
        addr_s = pickle.dumps(addr)
        wildcard = False

        # check if the subscription is to a named topic
        if hasattr(message, 'topic_name'):
            named = True

            # check for wildcard characters
            if '#' in message.topic_name or '+' in message.topic_name:
            # if yes? leave it so, result is 0
                wildcard = True
                exists = None
            else:
            # if no, check to make sure topic exists
            ### get by name
                exists = r.zrank('topics', message.topic_name)
            name = message.topic_name
        else:
            named = False
            # check to make sure topic exists
            ### get by id
            name = r.zrange('topics', message.topic_id[0], message.topic_id[0])
            exists = message.topic_id[0]

        if wildcard:
            result = 0
        else:
            result = exists

        if result is not None:
            # look up the client id for this address
            client_id = r.hget('%s:client_info' % addr_s, 'client_id')
            client_id = client_id.decode()

            if client_id is None:
                logger.debug("Invalid client tried to subscribe?")
                result = None
            else:
                # add a subscription row (topic, client id)
                r.sadd('%s:subscriptions' % client_id, name)

        # queue response
        MQTTSNActions.queue(addr_s, MQTTSNActions.create_suback(message, result))

    @staticmethod
    def handle_publish(message, addr):
        addr_s = pickle.dumps(addr)

        # turn topic id into topic name
        name = r.zrange('topics', message.topic_id[0], message.topic_id[0],)

        # scan subscriptions for the topic id
        if name is not None:
            name = name[0].decode()
            logger.debug("Found topic %s", name) 
            for subscription in r.scan_iter(match='*:subscriptions'):
                logger.debug("Found subscription in list %s" % subscription.decode())
                for topic in r.sscan_iter(subscription, match=name):
                    client_id = subscription.decode()[:-14]
                    logger.debug('Sending to "%s"' % client_id )
                    socket = r.get('%s:socket' % client_id)
                    if socket is not None:
                        MQTTSNActions.queue(socket, MQTTSNActions.create_publish(message))
                    else:
                        logger.debug('Could not locate socket')
        else:
            logger.debug("Non-existing topic number %d" % message.topic_id[0])

        logger.debug("Got publish of '%s' to topic id %d" % (
            message.message, message.topic_id[0],))

        # queue the response
        MQTTSNActions.queue(addr_s, MQTTSNActions.create_disconnect(message))

    @staticmethod
    def handle_disconnect(message, addr):
        addr_s = pickle.dumps(addr)

        # Remove all the stuff for this client
        r.delete('%s:broker' % addr_s)
        
        client_id = r.hget('%s:client_info' % addr_s, 'client_id')
        client_id.decode()

        r.delete('%s:socket' % client_id)

        # socket for this client
        r.hmset('%s:client_info' % addr_s,
                {
                    'last_message': int(time.time()),
                    'will_be_disconnected': True
                }
            )
        # XXX can't remove from client list, or they won't get the
        # ACK!!!

        # r.zrem('%s:clients' % broker.myid(), addr_s)
        
        # TODO: handle sleepy clients (clients with duration)

        # queue the response
        MQTTSNActions.queue(addr_s, MQTTSNActions.create_disconnect(message))

    @staticmethod
    def handle_pingreq(message, addr):
        addr_s = pickle.dumps(addr)

        # socket for this client
        r.hmset('%s:client_info' % addr_s,
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
        r.hmset('%s:client_info' % addr_s,
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
        r.hmset('%s:client_info' % addr_s,
                {
                    'broker_id': broker.myid(),
                    'client_id': message.client_id,
                    'last_message': int(time.time())
                }
            )

        # be sure to clean up
        r.expire('%s:client_info' % addr_s, keeptime)

        # broker lookup entry
        r.set('%s:broker' % message.client_id, broker.myid())
    
        # socket lookup entry
        r.set('%s:socket' % message.client_id, addr_s)

        # TODO: if clean session is set, expire stuff related
        # to this this client right now

        # queue the CONNACK
        MQTTSNActions.queue(addr_s, MQTTSNActions.create_connack(message))
