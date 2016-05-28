import configparser
import io

# Default config values
default_config = """
[mqtt_sn]
    listen_port=1885
    listen_host=0.0.0.0
[logging]
    path=/tmp/mqttsn_broker.log
    level=INFO
    format=%%(asctime)s\t%%(name)s\t%%(levelname)s\t%%(message)s
"""
# Read config
config = configparser.ConfigParser()
config.read_string(default_config)

