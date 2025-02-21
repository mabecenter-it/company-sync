# File: company_sync/processors/csv_processor.py
import pandas as pd
import logging
from company_sync.utils import conditional_update

class CSVProcessor:
    def __init__(self, csv_path: str, strategy):
        self.csv_path = csv_path
        self.strategy = strategy  # Estrategia que implementa CompanyStrategy
        self.logger = logging.getLogger(__name__)
    
    def read_csv(self) -> pd.DataFrame:
        df = pd.read_csv(self.csv_path)
        if df.empty:
            self.logger.info("CSV is empty")
        return df

    def process(self) -> pd.DataFrame:
        df = self.read_csv()
        if df.empty:
            return df
        df = self.strategy.apply_logic(df)
        # Aquí se podría aplicar filtrado adicional usando conditional_update si es necesario
        return df

