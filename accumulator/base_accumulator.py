from abc import ABC, abstractmethod

class BaseAccumulator(ABC):
    @abstractmethod
    def add(self, value):
        pass

    @abstractmethod
    def get_local_value(self):
        pass

    @abstractmethod
    def reset_local_value(self):
        pass

    @abstractmethod
    def check_rule(self, comparison_value) -> bool:
        pass