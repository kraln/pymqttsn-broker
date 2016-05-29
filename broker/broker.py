import asyncio
import logging
import redis
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
def drain_queues(transport):
    while True:
        # check redis for list of my clients
        clients = r.zrange('%s:clients' % myid(), 0, -1)

        logger.debug('I have %d client(s)' % len(clients))

        # for each client, check redis if there is a message in their queue
        for client in clients:
            client = client.decode()

            next_message_in_queue = r.lpop('%s:queue' % client)

            if next_message_in_queue is not None:
                # not needed but makes nice debug
                msg = message.MQTTSNMessage()
                msg.parse(next_message_in_queue)
                logger.debug("Sending %s to %s" % (msg, client,))

                # get the socket from redis
                socket = pickle.loads(r.hget('%s:socket' % client, 'socket'))

                # get transport somehow
                transport.sendto(next_message_in_queue, socket)
            else:
                logger.debug('Client %s had no queued messages.' % client)

        # XXX any way to make this faster?
        yield from asyncio.sleep(1)

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
    asyncio.async(drain_queues(transport))

    return transport
