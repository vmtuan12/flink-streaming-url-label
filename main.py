from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, FlatMapFunction, RuntimeContext
from pyflink.common import WatermarkStrategy, Configuration, Duration
from pyflink.datastream.connectors.kafka import KafkaOffsetResetStrategy, KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
from pyflink.datastream.slot_sharing_group import SlotSharingGroup, MemorySize
from dotenv import load_dotenv
import os
from functions.functions import CountAccessProcessFunction
from schema.schema_controller import SchemaControl, URL_TYPE_INFO, COUNT_ACCESS_TYPE_INFO
from pyflink.common import WatermarkStrategy, Encoder, Row
from pyflink.datastream.state import ValueStateDescriptor
from datetime import datetime
import time
load_dotenv()

kafka_host = os.getenv('KAFKA_HOST')
group_name = "group-1"

class CountWindowAverage(FlatMapFunction):
    def flat_map(self, value):
        current_timestamp = value["created_at_sec"]
        yield Row(value["subscriberid"], value["label"], value["count"], value["created_at"], str(datetime.now()), int(time.time()) - current_timestamp)

config = Configuration()
config.set_string('state.backend.type', 'hashmap')
env = StreamExecutionEnvironment.get_execution_environment(config)
#env = StreamExecutionEnvironment.get_execution_environment()
#env.set_parallelism(22)
# env.enable_checkpointing(interval=1000)
env.set_python_executable("/home/mhtuan/anaconda3/envs/flink-env/bin/python")

url_source = KafkaSource.builder() \
        .set_bootstrap_servers(f"{kafka_host}:9091") \
        .set_topics("input-1p") \
        .set_group_id(group_name) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SchemaControl.get_url_deserialization()) \
        .build()


# ds_url = env.from_source(url_source, WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(3)), "Transaction Source", type_info=URL_TYPE_INFO).set_parallelism(3)
ds_url = env.from_source(url_source, WatermarkStrategy.for_monotonous_timestamps(), "Transaction Source", type_info=URL_TYPE_INFO)
# ds_flatmap = ds_url.key_by(lambda record: record["subscriberid"], key_type=Types.INT()).flat_map(CountWindowAverage(), output_type=TEST_FLATMAP_TYPE_INFO)
ds_url.print()

key_url_ds = ds_url.key_by(lambda record: record["subscriberid"], key_type=Types.INT())
# flatmap_ds = key_url_ds.flat_map(CountWindowAverage(), output_type=COUNT_ACCESS_TYPE_INFO).set_parallelism(22)
process_ds = key_url_ds.process(CountAccessProcessFunction(), output_type=COUNT_ACCESS_TYPE_INFO)
# process_ds.print()

sink = KafkaSink.builder() \
    .set_bootstrap_servers(f"{kafka_host}:9091") \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
            .set_topic("output-1p")
            .set_value_serialization_schema(SchemaControl.get_access_count_serialization())
            .build()
    ) \
    .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
    .build()
process_ds.sink_to(sink)

env.execute("streaming_url_topic_count")
