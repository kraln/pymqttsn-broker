import logging

logger = logging.getLogger('broker.message')

MESSAGE_TYPES = {
    0x00: 'ADVERTISE',
    0x01: 'SEARCHGW',
    0x02: 'GWINFO',
    0x04: 'CONNECT',
    0x05: 'CONNACK',
    0x06: 'WILLTOPICREQ',
    0x07: 'WILLTOPIC',
    0x08: 'WILLMSGREQ',
    0x09: 'WILLMSG',
    0x0A: 'REGISTER',
    0x0B: 'REGACK',
    0x0C: 'PUBLISH',
    0x0D: 'PUBACK',
    0x0E: 'PUBCOMP',
    0x0F: 'PUBREC',
    0x10: 'PUBREL',
    0x12: 'SUBSCRIBE',
    0x13: 'SUBACK',
    0x14: 'UNSUBSCRIBE',
    0x15: 'UNSUBACK',
    0x16: 'PINGREQ',
    0x17: 'PINGRESP',
    0x18: 'DISCONNECT',
    0x1A: 'WILLTOPICUPD',
    0x1B: 'WILLTOPICRESP',
    0x1C: 'WILLMSGUPD',
    0x1D: 'WILLMSGRESP',
    0xFE: '(encapsulated)'
}

TYPE_LUT = { v:k for k,v in MESSAGE_TYPES.items() }

class MQTTSNFlags:
    """
    Flags class

    Represents the flag byte on messages

    fields:
        dup
        qos
        retain
        will
        clean_session
        topic_id_type
    """
    def __init__(self, flags):
        self.dup = True if flags & 0b10000000 else False
        self.qos = (flags & 0b01100000) >> 5
        self.retain = True if flags & 0b00010000 else False
        self.will = True if flags & 0b00001000 else False
        self.clean_session = True if flags & 0b00000100 else False
        self.topic_id_type = flags & 0b00000011

class MQTTSNMessage:
    """
    Message class

    Defined by Chapter 5 of the spec.

    I have a length, and a message type, and a variable part which depends
    on the message type.
    """
    def __init__(self):
        # Some defaults
        self.flags = MQTTSNFlags(0)
        self.duration = 0
        self.client_id = None
        self.message_type = 0

    def __str__(self):
        if self.message_type == TYPE_LUT['CONNECT']:
            return "%s: W?%d C?%d Keepalive %d ID: %s" % (
                    MESSAGE_TYPES[self.message_type],
                    self.flags.will,
                    self.flags.clean_session,
                    self.duration,
                    self.client_id)

        #default
        return MESSAGE_TYPES[self.message_type]

    def parse(self, data):
        """Parse bytes into a struct"""

        # Data validation?
        if len(data) < 2:
            logger.warning('Invalid packet, discarding')
            return False

        try:
            # Determine length (big message support)
            if data[0] == 1:
                self.length = data[1] << 8 | data[2]
                self.message_type = data[3]
                self.variable_index = 4
            else:
                self.length = data[0]
                self.message_type = data[1]
                self.variable_index = 2

            logger.debug('Packet %x (%s) decoded, length %x' % (
                self.message_type,
                MESSAGE_TYPES[self.message_type],
                self.length,
                )
            )

        except Exception as e:
            # should I rethrow here?
            print(e)
            return False

        # Now, message-specific parsing

        if self.message_type == TYPE_LUT['CONNECT']:
            """
            Flags:
                NOT USED: DUP, QoS, Retain, TopicIDType
                Will: If set, client requests will topic and message prompting
                CleanSession: If set, remove all subscriptions for this client
            ProtocolID:
                Must be always 0x1
            Duration:
                Keep alive timer duration
            Client ID:
                (Rest of the message) The client's ID
            """
            self.flags = MQTTSNFlags(data[2])

            # This has to be 1, basically.
            if data[3] != 1:
                logger.warning("Unknown protocol id: %d", (data[3],))
                return false

            self.duration = data[4] << 8 | data[5]
            self.client_id = data[6:].decode()

        elif self.message_type == TYPE_LUT['PUBLISH']:
            """
            Length      MsgType Flags   TopicId MsgId Data
            (octet 0)   (1)     (2)     (3-4)   (5-6) (7:n)
            """
            self.flags = MQTTSNFlags(data[2])
            self.topic_id = (data[3] << 8 | data[4], data[3], data[4],)
            self.message_id = (data[5] << 8 | data[6], data[5], data[6],)
            self.message = data[7:].decode()

        elif self.message_type == TYPE_LUT['SUBSCRIBE']:
            """
            Length      MsgType Flags   MsgId TopicName or TopicId
            (octet 0)   (1)     (2)     (3-4) (5:n)     or (5-6)
            Table 19: SUBSCRIBE and UNSUBSCRIBE Messages
            """
            self.flags = MQTTSNFlags(data[2])
            self.message_id = (data[3] << 8 | data[4], data[3], data[4],)

            if self.flags.topic_id_type == 0x0:
                # name
                self.topic_name = data[5:].decode()
            else:
                # either id or reserved -- either way, two bytes
                self.topic_id = (data[5] << 8 | data[6], data[5], data[6],)

        elif self.message_type == TYPE_LUT['REGISTER']:
            """
            Length      MsgType TopicId MsgId TopicName
            (octet 0)   (1)     (2,3)   (4:5) (6:n)

            Table 14: REGISTER Message
            """
            self.topic_id = data[2] << 8 | data[3]
            self.message_id = (data[4] << 8 | data[5], data[4], data[5],)
            self.topic_name = data[6:].decode()
        
        # I guess everything's okay
        return True


