# File: company_sync/strategies/oscar_strategy.py
from company_sync.strategies.base_strategy import BaseStrategy
from company_sync.utils import calculate_paid_through_date, get_fields

class OscarStrategy(BaseStrategy):
    def __init__(self):
        self.fields = get_fields("oscar")
    
    def apply_logic(self, df):
        if 'Policy status' in df.columns:
            df['Paid Through Date'] = df['Policy status'].apply(
                lambda s: calculate_paid_through_date(s, self.fields['format'])
            )
        # Renombrar la columna "Member ID" a "memberID"
        if 'Member ID' in df.columns:
            df.rename(columns={"Member ID": "memberID"}, inplace=True)
        df_normalize = self.normalize_columns(df)
        return df_normalize

    def normalize_columns(self, df):
        # Mapeo explícito para los nombres de columnas que queremos renombrar
        mapping = {
            "Member ID": "memberID",
            "Paid Through Date": "paidThroughDate",
            "Coverage start date": "policyEffecDate",
            "Coverage end date": "policyTermDate",
            "Policy status": "policyStatus",
        }
        return df.rename(columns=mapping)

    def get_fields(self) -> dict:
        return self.fields