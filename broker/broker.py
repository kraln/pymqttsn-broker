import asyncio
import logging

logger = logging.getLogger('broker.socketio')

class MQTTSNBrokerProtocol:

    def connection_made(self, transport):
        logger.info(('start', transport,))
        self.transport = transport

    def datagram_received(self, data, addr):
        logger.debug(('Data received:', data, addr,))
        self.transport.sendto(data, addr)

    def error_received(self, exc):
        logger.error(('Error received:', exc,))

    def connection_lost(self, exc):
        logger.info(('stop', exc,))

def start_server(loop, addr):
    t = asyncio.Task(loop.create_datagram_endpoint(
        MQTTSNBrokerProtocol, local_addr=addr))
    transport, server = loop.run_until_complete(t)
    return transport
