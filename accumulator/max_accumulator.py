from accumulator.base_accumulator import BaseAccumulator

class MaxAccumulator(BaseAccumulator):
    def __init__(self) -> None:
        super().__init__()
        self.max = 0

    def add(self, value: float):
        self.max = max(self.max, value)

    def get_local_value(self):
        return self.max

    def reset_local_value(self):
        self.max = 0

    def check_rule(self, comparison_value) -> bool:
        if self.get_local_value() >= comparison_value:
            return True
        
        return False