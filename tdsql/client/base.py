from abc import ABC, abstractmethod

import pandas as pd

class BaseClient(ABC):
    @abstractmethod
    def select(self, sql: str) -> pd.DataFrame:
        pass
