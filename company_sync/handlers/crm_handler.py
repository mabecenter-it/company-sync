# File: company_sync/handlers/crm_handler.py
import pandas as pd
import logging
from tqdm import tqdm
from company_sync.repositories.crm_repository import CRMRepository

class CRMHandler:
    def __init__(self, company: str, broker: str):
        self.repo = CRMRepository(company, broker)
        self.company = company
        self.broker = broker
        self.logger = logging.getLogger(__name__)
    
    def fetch_data(self) -> pd.DataFrame:
        return self.repo.fetch_sales_orders()
    
    def merge_data(self, df_crm: pd.DataFrame, df_csv: pd.DataFrame) -> pd.DataFrame:
        df_merged = pd.merge(df_crm, df_csv, on="memberID", how="outer", indicator=True)
        df_missing = df_merged[df_merged['_merge'] == 'left_only']
        for _, row in tqdm(df_missing.iterrows(), total=len(df_missing), desc="Validando Órdenes de Venta..."):
            memberID = str(row['memberID'])
            if not memberID:
                salesOrder_no = str(row['salesOrder_no'])
                self.logger.info("Se encontró una orden de venta pero no está en el portal", extra={'memberid': salesOrder_no})
            else:
                self.logger.info("Se encontró una orden de venta pero no está en el portal", extra={'memberid': memberID, 'company': self.company, 'broker': self.broker})
        return df_csv
    