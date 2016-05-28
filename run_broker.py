# Python 3

import logging
import argparse as ap
import sys
import asyncio
import uvloop

try:
    import signal
except ImportError:
    signal = None

from config.config import config
from broker.broker import start_server

# use uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

logger = logging.getLogger('broker')

def log_exp(excType, excValue, traceback):
    """Log exceptions also"""
    logger.error(
        "Uncaught exception!",
        exc_info=(excType, excValue, traceback))

sys.excepthook = log_exp

def main():
    """Set up logging, start the event loop"""
    # File and console
    fh = logging.FileHandler(config.get('logging', 'path'))
    ch = logging.StreamHandler()

    # Set log levels
    log_level = config.get('logging', 'level')
    logger.setLevel(log_level)
    fh.setLevel(log_level)
    ch.setLevel(log_level)

    # Set format
    formatter = logging.Formatter(config.get('logging', 'format'))
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # Add handlers
    logger.addHandler(fh)
    logger.addHandler(ch)

    # ready to go
    logger.info("pymqttsn broker started")

    # start the asyncio loop
    loop = asyncio.get_event_loop()
    if signal is not None:
        loop.add_signal_handler(signal.SIGINT, loop.stop)

    host = config.get('mqtt_sn', 'listen_host')
    port = config.getint('mqtt_sn', 'listen_port')

    server = start_server(loop, (host, port))

    try:
        loop.run_forever() # and ever and ever
    finally:
        server.close()
        loop.close()
        logger.info('Goodnight, sweet prince')

if __name__ == "__main__":

    # Read configuration provided by user
    parser = ap.ArgumentParser()
    parser.add_argument(
        '--config', '-c', type=str, help='Configuration File',
        default='')
    args = parser.parse_args()
    if args.config is not "":
        config.read(args.config)
    else:
        logger.warning("No configuration specified, using defaults.")

    # Start service
    main()
