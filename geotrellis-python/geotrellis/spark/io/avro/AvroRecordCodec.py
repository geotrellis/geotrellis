from geotrellis.spark.io.avro.AvroCodec import AvroCodec

class AvroRecordCodec(AvroCodec):
    def __init__(self, record_type):
        AvroCodec.__init__(self, record_type)
    @property
    def schema(self):
        pass
    def _encode(self, thing, dct):
        pass
    def decode(self, dct):
        pass
    def encode(self, thing, dct = None):
        if dct is None:
            from avro.io import GenericRecord
            dct = GenericRecord(self.schema)
        self._encode(thing, dct)
        return dct
    def supported(self, thing):
        return isinstance(thing, self.thing_type)
