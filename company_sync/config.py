import os
from dotenv import load_dotenv
import logging

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Configuración de VTiger
VTIGER_HOST = os.getenv('VTIGER_HOST')
VTIGER_USERNAME = os.getenv('VTIGER_USERNAME')
VTIGER_TOKEN = os.getenv('VTIGER_TOKEN')

# Configuración de la base de datos
DB_TYPE = os.getenv('DB_TYPE')
DB_CONNECTOR = os.getenv('DB_CONNECTOR')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')

SQLALCHEMY_DATABASE_URI = f'{DB_TYPE}+{DB_CONNECTOR}://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}'

def setup_logging():
    """
    Configura el logger global con el nivel INFO.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # Puedes agregar más handlers y formatos según necesites
    return logger