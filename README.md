# Flink streaming url label

## Flink installation
I am using Flink 1.19.1. All releases can be found [here](https://flink.apache.org/downloads/). Download a desired version, then extract it to a directory.

## Configuration
Open `<path to Flink directory>/conf/config.yaml`, and edit the configuration as desire. Read [this](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/deployment/config/) for more detailed information.

There are some basic configuration that I am using.

Make the state backend default be hashmap, which stores states in memory
```
state:
  backend:
    type: hashmap
```

Set TaskManager number of slot and memory
```
taskmanager:
  numberOfTaskSlots: 6
  memory:
    flink:
      size: 8000m
```

## Code explaination

### main.py

These 3 below lines defines the execution environment and python execution path. The config makes sure that the state backend stores state in hashmap in memory (There is another option to store in RocksDB)
```
config = Configuration()
config.set_string('state.backend.type', 'hashmap')
env = StreamExecutionEnvironment.get_execution_environment(config)
env.set_python_executable("/home/mhtuan/anaconda3/envs/flink-env/bin/python")
```

Define Kafka source and start consuming. 
```
url_source = KafkaSource.builder() \
        .set_bootstrap_servers(f"{kafka_host}:9091") \
        .set_topics("input-1p") \
        .set_group_id(group_name) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SchemaControl.get_url_deserialization()) \
        .build()
        
ds_url = env.from_source(url_source, WatermarkStrategy.for_monotonous_timestamps(), "Transaction Source", type_info=URL_TYPE_INFO)
```
> [!TIP]
> If the topic has more than 1 partitions, set the parallelism of the `ds_url` = number of partitions. E.g. there are 3 partitions then `ds_url = env.from_source(url_source, WatermarkStrategy.for_monotonous_timestamps(), "Transaction Source", type_info=URL_TYPE_INFO).set_parallelism(3)`

Partition data by key, then apply Process function
```
key_url_ds = ds_url.key_by(lambda record: record["subscriberid"], key_type=Types.INT())
process_ds = key_url_ds.process(CountAccessProcessFunction(), output_type=COUNT_ACCESS_TYPE_INFO)
```
> [!TIP]
> Set the parallelism for faster computation if you can `process_ds = key_url_ds.process(CountAccessProcessFunction(), output_type=COUNT_ACCESS_TYPE_INFO).set_parallelism(10)`. Remember that the **maximum** number of parallelism = number of slot.

Define sink
```
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
```

### functions/functions.py

Declare state and local variables. I declare the state here as a MapState, with the key is the `label`, and the value is a dict. That dict's key is the timestamp in second when the corresponding record with that label arrives, and the value is the total access count of that label that that time. I do this to handle the situation where more than 1 records with the same user and the same topic arrive at the same time.
```
def __init__(self):
    self._window_state_desc = MapStateDescriptor(
        "count_access_state",
        Types.STRING(),
        Types.MAP(Types.INT(), Types.INT())
    )
    self._window_state = None
    self.window_duration = 604800 # seconds

def open(self, runtime_context: RuntimeContext):
    self._window_state = runtime_context.get_map_state(self._window_state_desc)
```

The function `process_element` is the main function that will run when the datastream applies a Process function. The `value` presents the input record.
```
def process_element(self, value: Any, ctx: 'KeyedProcessFunction.Context'):
    count = value["count"]
    current_timestamp = value["created_at_sec"]
    label = value["label"]
    
    # update data in state
    self._add_count_to_state(label=label, event_time=current_timestamp, count=count)
    
    # iterate the state
    accumulator = SumAccumulator()
    event_time_list = list(self._window_state.get(label).keys()).copy()
    label_dict = self._window_state.get(label)
    for event_time in event_time_list:
        # if the time is valid, then add up the count value
        if self._access_time_is_valid(access_time=event_time, start_time=(current_timestamp - self.window_duration), current_time=current_timestamp):
            access_count = label_dict.get(event_time)
            accumulator.add(access_count)

    # yield the result
    yield Row(value["subscriberid"], label, accumulator.get_local_value(), value["created_at"], str(datetime.now()), int(time.time()) - current_timestamp)
    
    # register a timer that will run to delete expired data in state
    ctx.timer_service().register_event_time_timer(current_timestamp * 1000)
```

## How to execute
To execute this application locally, first, start the Flink cluster with the command below
```
<path to Flink directory>/bin/start-cluster.sh
```

Then submit the application
```
<path to Flink directory>/bin/flink run -py main.py
```
