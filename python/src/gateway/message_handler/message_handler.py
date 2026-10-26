from common import message_protocol
from uuid import uuid4

class MessageHandler:

    def __init__(self):
        self.client = str(uuid4())
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([self.client, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.client])

    def deserialize_result_message(self, message):
        client, fields = message_protocol.internal.deserialize(message)
        # Gateway chequea cliente por cliente, si no lo encontro devuelve vacio para revisar proximo cliente
        if client != self.client:
            return []
        return fields
