# File: company_sync/strategies/aetna_strategy.py
from company_sync.strategies.base_strategy import BaseStrategy
from company_sync.utils import calculate_term_date, get_fields

class AetnaStrategy(BaseStrategy):
    def __init__(self):
        self.fields = get_fields("aetna")
    
    def apply_logic(self, df):
        if 'Effective Date' in df.columns:
            df['Policy Term Date'] = df['Effective Date'].apply(lambda d: calculate_term_date(d, self.fields['format']))

        # Renombrar la columna "Member ID" a "memberID"
        if 'Member ID' in df.columns:
            df.rename(columns={"Member ID": "memberID"}, inplace=True)
        return df
    
    def get_fields(self) -> dict:
        return self.fields
