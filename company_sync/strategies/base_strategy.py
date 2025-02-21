# File: company_sync/strategies/company_strategy.py
from abc import ABC, abstractmethod

class BaseStrategy(ABC):
    @abstractmethod
    def apply_logic(self, df):
        """Aplica la lógica específica de la compañía al DataFrame."""
        pass

    @abstractmethod
    def get_fields(self) -> dict:
        """Retorna el mapeo de campos para la compañía."""
        pass
