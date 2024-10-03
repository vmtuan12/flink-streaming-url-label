from pyflink.common.typeinfo import Types
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.datastream.formats.avro import AvroRowSerializationSchema, AvroRowDeserializationSchema

URL_TYPE_INFO = Types.ROW_NAMED(["domain", "label", "subscriberid", "count", "created_at_sec", "created_at"], [Types.STRING(), Types.STRING(), Types.INT(), Types.INT(), Types.INT(), Types.STRING()])
COUNT_ACCESS_TYPE_INFO = Types.ROW_NAMED(["subscriberid", "label", "count", "created_at", "done_at", "time_dif"], [Types.INT(), Types.STRING(), Types.INT(), Types.STRING(), Types.STRING(), Types.INT()])
TEST_FLATMAP_TYPE_INFO = Types.ROW_NAMED(["created_at", "done_at", "sum"], [Types.STRING(), Types.STRING(), Types.INT()])

class SerializationType():
    JSON = 0
    AVRO = 1

class SchemaControl():
    @classmethod
    def get_url_deserialization(cls, type: int = SerializationType.JSON):
        if type == SerializationType.JSON:
            return JsonRowDeserializationSchema.builder().type_info(type_info=URL_TYPE_INFO).build()
        else:
            return AvroRowDeserializationSchema(avro_schema_string=open("schema/avsc/url.avsc", "r").read())
        
    @classmethod
    def get_url_serialization(cls, type: int = SerializationType.JSON):
        if type == SerializationType.JSON:
            return JsonRowSerializationSchema.builder().with_type_info(type_info=URL_TYPE_INFO).build()
        else:
            return AvroRowSerializationSchema(avro_schema_string=open("schema/avsc/url.avsc", "r").read())
        
    @classmethod
    def get_flatmap_test_serialization(cls, type: int = SerializationType.JSON):
        return JsonRowSerializationSchema.builder().with_type_info(type_info=TEST_FLATMAP_TYPE_INFO).build()

    @classmethod    
    def get_access_count_serialization(cls, type: int = SerializationType.JSON):
        if type == SerializationType.JSON:
            return JsonRowSerializationSchema.builder().with_type_info(type_info=COUNT_ACCESS_TYPE_INFO).build()
        else:
            return AvroRowSerializationSchema(avro_schema_string=open("schema/avsc/access_count.avsc", "r").read())