# File: company_sync/main.py
import argparse
import config
from company_sync.logging_config import setup_logging
from WSClient import VTigerWSClient
from company_sync.services.so_service import SOService
from company_sync.strategies.aetna_strategy import AetnaStrategy
from company_sync.strategies.oscar_strategy import OscarStrategy
from company_sync.strategies.base_strategy import BaseStrategy

def main():
    parser = argparse.ArgumentParser(description='CLI Tool for VTiger Sales Order Sync')
    parser.add_argument('csv', type=str, help='Path to CSV file')
    parser.add_argument('company', type=str, help='Company name (e.g., Aetna, Oscar)')
    parser.add_argument('broker', type=str, help='Broker name')
    args = parser.parse_args()
    
    logger = setup_logging()
    
    # Selecciona la estrategia adecuada según la compañía
    if args.company.lower() == 'aetna':
        strategy = AetnaStrategy()
    elif args.company.lower() == 'oscar':
        strategy = OscarStrategy()
    else:
        # Estrategia por defecto (sin lógica especial)
        class DefaultStrategy(BaseStrategy):
            def apply_logic(self, df):
                return df
            def get_fields(self):
                from company_sync.utils import get_fields
                return get_fields(args.company)
        strategy = DefaultStrategy()
    
    vtiger_client = VTigerWSClient(config.VTIGER_HOST)
    vtiger_client.doLogin(config.VTIGER_USERNAME, config.VTIGER_TOKEN)
    
    service = SOService(args.csv, args.company, args.broker, strategy, vtiger_client, logger)
    service.process()

if __name__ == '__main__':
    main()