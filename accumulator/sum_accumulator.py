from accumulator.base_accumulator import BaseAccumulator

class SumAccumulator(BaseAccumulator):
    def __init__(self) -> None:
        super().__init__()
        self.sum = 0

    def add(self, value: int):
        self.sum += value

    def get_local_value(self):
        return self.sum

    def reset_local_value(self):
        self.sum = 0

    def check_rule(self, comparison_value) -> bool:
        if self.get_local_value() >= comparison_value:
            return True
        
        return False