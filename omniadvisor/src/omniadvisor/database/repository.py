from abc import ABC, abstractmethod
from typing import Dict


class ConfigRepository(ABC):

    @abstractmethod
    def save(self, config: Dict) -> None:
        pass

    @abstractmethod
    def find_by_id(self, config_id: str):
        pass

    @abstractmethod
    def update(self, config: Dict):
        pass
