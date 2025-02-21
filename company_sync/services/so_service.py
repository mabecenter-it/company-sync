# File: company_sync/services/sales_order_service.py
from company_sync.processors.csv_processor import CSVProcessor
from company_sync.handlers.crm_handler import CRMHandler
from company_sync.handlers.so_updater import SOUpdater
from company_sync.utils import get_fields

class SOService:
    def __init__(self, csv_path: str, company: str, broker: str, strategy, vtiger_client, logger):
        self.csv_processor = CSVProcessor(csv_path, strategy)
        self.crm_handler = CRMHandler(company, broker)
        data_config = get_fields(company)
        self.so_updater = SOUpdater(vtiger_client, company, data_config, broker, logger=logger)
        self.logger = logger

    def process(self):
        df_csv = self.csv_processor.process()
        if df_csv.empty:
            return
        df_crm = self.crm_handler.fetch_data()
        self.crm_handler.merge_data(df_crm, df_csv)
        self.so_updater.update_orders(df_csv)
