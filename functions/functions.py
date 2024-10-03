from pyflink.datastream import RuntimeContext, KeyedBroadcastProcessFunction, KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.common.typeinfo import Types
from pyflink.common import Row
from typing import Any
import time
from accumulator.sum_accumulator import SumAccumulator
from datetime import datetime

class CountAccessProcessFunction(KeyedProcessFunction):
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

    # todo
    def process_element(self, value: Any, ctx: 'KeyedProcessFunction.Context'):
        count = value["count"]
        current_timestamp = value["created_at_sec"]
        label = value["label"]
        
        self._add_count_to_state(label=label, event_time=current_timestamp, count=count)
        
        accumulator = SumAccumulator()
        event_time_list = list(self._window_state.get(label).keys()).copy()
        label_dict = self._window_state.get(label)
        for event_time in event_time_list:
            if self._access_time_is_valid(access_time=event_time, start_time=(current_timestamp - self.window_duration), current_time=current_timestamp):
                access_count = label_dict.get(event_time)
                accumulator.add(access_count)

        yield Row(value["subscriberid"], label, accumulator.get_local_value(), value["created_at"], str(datetime.now()), int(time.time()) - current_timestamp)
        
        ctx.timer_service().register_event_time_timer(current_timestamp * 1000)

    # todo
    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        window = self._window_state
        events = list(window.keys()).copy()
        limit_timestamp = timestamp - self.window_duration

        for label in events:
            event_time_list = list(window.get(label).keys()).copy()
            for event_time in event_time_list:
                if (event_time * 1000) < limit_timestamp:
                    self._window_state.remove(event_time)

        yield from []

    def _add_count_to_state(self, label: str, event_time: int, count: int):
        label_dict = self._window_state.get(label)
        if label_dict == None:
            self._window_state.put(label, {})

        access_count = self._window_state.get(label).get(event_time)
        if access_count != None:
            access_count += count
        else:
            access_count = count

        self._window_state.get(label).update({event_time: access_count})

    def _access_time_is_valid(self, access_time: int, start_time: int, current_time: int):
        return start_time <= access_time and access_time <= current_time
        