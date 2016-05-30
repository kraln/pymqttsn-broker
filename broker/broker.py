import asyncio
import logging
import redis
import aioredis
import os
import time
import pickle

from config.config import config

from broker import message
from broker import actions

logger = logging.getLogger('broker.socketio')
r = redis.Redis(config.get('redis', 'host'))

class MQTTSNBrokerProtocol:

    def connection_made(self, transport):
        logger.info(('start', transport,))
        self.transport = transport

    def datagram_received(self, data, addr):
        logger.debug(('Data received:', data, addr,))
        msg = message.MQTTSNMessage()

        if not msg.parse(data):
            logger.warning('Message parse error!')
            return

        logger.debug('Parsed: %s' % (msg, ))

        actions.MQTTSNActions.handle(msg, addr)

    def error_received(self, exc):
        logger.error(('Error received:', exc,))

    def connection_lost(self, exc):
        logger.info(('stop', exc,))

def myid():
    """ How I am referred to internally """
    return 'broker:%s:%s' % (config.get('redis', 'broker_name'), os.getpid(),)

@asyncio.coroutine
def drain_queues(transport, loop):

    conn = yield from aioredis.create_connection(
                    (config.get('redis', 'host'), 6379), loop=loop)
    while True:
        # check redis for list of my clients
        clients = yield from conn.execute('zrange', '%s:clients' % myid(), 0, -1)

        # for each client, check redis if there is a message in their queue
        for client in clients:
            next_message_in_queue = yield from conn.execute('lpop', '%s:queue' % client)

            if next_message_in_queue is not None:
                # not needed but makes nice debug
                msg = message.MQTTSNMessage()
                msg.parse(next_message_in_queue)
                logger.debug("Sending %s to %s" % (msg, client,))

                # get the socket from redis
                socket = pickle.loads(client)

                # if I want more info about this, it's here
                # client_info = r.hget('%s:socket' % client, 'socket'))

                # get transport somehow
                transport.sendto(next_message_in_queue, socket)

    conn.close()


@asyncio.coroutine
def keepalive():
    """Make sure to let Redis know we're still alive"""
    while True:
        # will expire one second after the keep alive...
        keeptime = config.getint('redis', 'keepalive') + 1

        # set expirations on my data
        r.setex(myid(), int(time.time()), keeptime)
        r.expire('%s:clients' % myid(), keeptime)

        # clear any clients that are too old
        r.zremrangebyscore('%s:clients' % myid(), 0, int(time.time()))

        yield from asyncio.sleep(config.getint('redis', 'keepalive'))

def start_server(loop, addr):
    t = asyncio.Task(
            loop.create_datagram_endpoint(
                MQTTSNBrokerProtocol,
                local_addr=addr
                )
            )

    # set up the socket server
    transport, server = loop.run_until_complete(t)

    # add the keepalive
    asyncio.async(keepalive())

    # add the queue drainer
    asyncio.async(drain_queues(transport, loop))

    return transport
