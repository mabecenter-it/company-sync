# File: company_sync/logging_config.py
import logging
from handlers.csv_handler import CSVHandler  # Se asume que CSVHandler está implementado

def setup_logging(log_file='problems.csv'):
    logger = logging.getLogger('company_sync')
    logger.setLevel(logging.INFO)
    fieldnames = ['company', 'broker', 'date', 'time', 'memberid', 'description']
    csv_handler = CSVHandler(log_file, fieldnames=fieldnames)
    formatter = logging.Formatter("%(message)s")
    csv_handler.setFormatter(formatter)
    logger.addHandler(csv_handler)
    return logger