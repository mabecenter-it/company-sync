from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import config

# Crear la conexión utilizando la URI definida en config
engine = create_engine(config.SQLALCHEMY_DATABASE_URI, echo=False)
Session = sessionmaker(bind=engine)

def get_session():
    """
    Retorna una nueva sesión de base de datos.
    """
    return Session()