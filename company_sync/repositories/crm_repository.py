# File: company_sync/repositories/crm_repository.py
import pandas as pd
from sqlalchemy import text
from company_sync.database import get_session

class CRMRepository:
    def __init__(self, company: str, broker: str):
        self.company = company
        self.broker = broker

    def fetch_sales_orders(self) -> pd.DataFrame:
        with get_session() as session:
            query = f"""
                SELECT member_id, so_no
                FROM vtigercrm_2022.calendar_2025_materialized
                WHERE Compañía = '{self.company}'
                  AND Broker = '{'BEATRIZ SIERRA' if self.broker == 'BS' else 'ANA DANIELLA CORRALES'}'
                  AND Terminación >= DATE_FORMAT(CURRENT_DATE(), '%Y-%m-%d')
                  AND Month = DATE_FORMAT(CURRENT_DATE(), '%Y-%m-01')
                  AND rn = OV_Count;
            """
            result = session.execute(text(query)).fetchall()
            return pd.DataFrame(result, columns=["memberID", "salesOrder_no"])