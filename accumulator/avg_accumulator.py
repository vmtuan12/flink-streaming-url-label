from accumulator.base_accumulator import BaseAccumulator

class AvgAccumulator(BaseAccumulator):
    def __init__(self) -> None:
        super().__init__()
        self.count = 0
        self.sum = 0.00

    def add(self, value: float):
        self.count += 1
        self.sum += value

    def get_local_value(self):
        return self.sum / self.count

    def reset_local_value(self):
        self.count = 0
        self.sum = 0

    def check_rule(self, comparison_value) -> bool:
        if self.get_local_value() >= comparison_value:
            return True
        
        return False