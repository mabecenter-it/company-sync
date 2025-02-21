import argparse
import datetime
from tqdm import tqdm
import logging
import pandas as pd
# Librerías externas y cliente de Vtiger
from WSClient import Vtiger_WSClient
from CSVHandler import CSVHandler

from sqlalchemy import create_engine 
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

#Get Enviroments from file .env
import os
from dotenv import load_dotenv
load_dotenv()

host = os.getenv('VTIGER_HOST') if os.getenv('VTIGER_HOST') else "http://192.168.99.102/vtigercrm_2022/"
username = os.getenv('VTIGER_USERNAME') if os.getenv('VTIGER_USERNAME') else "superadmin"
token = os.getenv('VTIGER_TOKEN') if os.getenv('VTIGER_TOKEN') else "MFaeyxCMTmRrUZiE"

mariadb_type = os.getenv('DB_TYPE') if os.getenv('DB_TYPE') else "mysql"
mariadb_connector = os.getenv('DB_CONNECTOR') if os.getenv('DB_CONNECTOR') else "pymysql"
mariadb_host = os.getenv('DB_HOST') if os.getenv('DB_HOST') else "192.168.99.117"
mariadb_port = os.getenv('DB_PORT') if os.getenv('DB_PORT') else "3307"
mariadb_database = os.getenv('DB_DATABASE') if os.getenv('DB_DATABASE') else "vtigercrm_2022"
mariadb_username = os.getenv('DB_USERNAME') if os.getenv('DB_USERNAME') else "root"
mariadb_password = os.getenv('DB_PASSWORD') if os.getenv('DB_PASSWORD') else "042285"

# Conectar a la API de VTigerCRM
client = Vtiger_WSClient(host)
client.doLogin(username, token)

# create_engine
SQLALCHEMY_DATABASE_URI = f'{mariadb_type}+{mariadb_connector}://{mariadb_username}:{mariadb_password}@{mariadb_host}:{mariadb_port}/{mariadb_database}'
engine = create_engine(SQLALCHEMY_DATABASE_URI, echo=False)
Session = sessionmaker()
Session.configure(bind=engine)

def conditional_update(company: str) -> dict:
    """
    Devuelve un diccionario con las condiciones para filtrar filas.
    Cada par clave:valor indica una columna y el valor que debe tener.
    """
    if company == 'Aetna':
        return {
            'Relationship': 'Self',
            'Policy Status': 'Active',
        }
    if company == 'Ambetter':
        return {
            'Payable Agent': 'Health Family Insurance',
        }
    if company == 'Molina':
        return {
            'Status': 'Active',
        }
    if company == 'Oscar':
        return {
            'cond': '!=',
            'Policy status': 'Inactive'
        }
    
    return {}

COMPANY_FIELDS = {
    'aetna': {
        'memberID': 'Issuer Assigned ID',
        'paidThroughDate': 'Paid Through Date',
        'policyTermDate': 'Broker Term Date',
        'format': '%B %d, %Y'
    },
    'ambetter': {
        'memberID': 'Policy Number',
        'paidThroughDate': 'Paid Through Date',
        'policyTermDate': 'Policy Term Date',
        'format': '%m/%d/%Y'
    },
    'oscar': {
        'memberID': 'Member ID',
        'paidThroughDate': 'Paid Through Date',
        'policyTermDate': 'Coverage end date',
        'format': '%B %d, %Y'
    },
    'molina': {
        'memberID': 'Subscriber_ID',
        'paidThroughDate': 'Paid_Through_Date',
        'policyTermDate': 'Broker_End_Date',
        'format': '%m/%d/%Y'
    },
    'fb': {
        'memberID': 'Member ID',
        'paidThroughDate': 'Paid Through Date',
        'policyTermDate': 'Policy Term Date',
        'format': '%m/%d/%Y'
    },
}

DEFAULT_FIELDS = {
    'memberID': 'Member ID',
    'paidThroughDate': 'Paid Through Date',
    # policyTermDate no aparece aquí, lo puedes agregar si quieres un default
    'format': '%m/%d/%Y'
}

def fields(company: str) -> dict:
    """
    Devuelve los nombres de las columnas relevantes según la compañía
    y el formato de fecha correspondiente.
    """
    # Normalizamos a minúsculas para hacer el match en el diccionario
    company_key = company.lower()

    # Retornamos la configuración encontrada o el DEFAULT_FIELDS si no existe
    return COMPANY_FIELDS.get(company_key, DEFAULT_FIELDS)

def last_day_of_month(any_day: datetime.date) -> str:
    """
    Retorna el último día del mes para la fecha proporcionada, en formato MM/DD/YYYY.
    """
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    date = next_month - datetime.timedelta(days=next_month.day)
    return date.strftime('%B %d, %Y')

def calculate_term_date(effective_date: str) -> str:
    """
    Devuelve el último día del mes de diciembre en el que se cumple un año desde la fecha de inicio.
    """
    effective_date = datetime.datetime.strptime(effective_date, '%B %d, %Y')
    return last_day_of_month(effective_date.replace(year=effective_date.year + 1, month=12))

def calculate_paid_through_date(status: str) -> str:
    """
    Calcula una fecha (en formato string) según el estado de la póliza.
    Devuelve una cadena en formato MM/DD/YYYY, o '' si no aplica.
    """
    today = datetime.date.today()
    if status == 'Active':
        # Último día del mes actual
        return last_day_of_month(today)
    elif status == 'Delinquent':
        # Último día de hace dos meses
        two_months_ago = (today.replace(day=1) - datetime.timedelta(days=1))  # Mes anterior
        two_months_ago = (two_months_ago.replace(day=1) - datetime.timedelta(days=1))  # Otro mes atrás
        return last_day_of_month(two_months_ago)
    elif status == 'Grace period':
        # Último día del mes anterior
        last_month = today.replace(day=1) - datetime.timedelta(days=1)
        return last_day_of_month(last_month)
    else:
        return ''  # Sin fecha si no coincide con los estados

def update_sales_order(memberID: str, paidThroughDate: str, salesOrderData: dict):
    """
    Lógica de actualización para la orden de venta en Vtiger.
    Llama al método 'client.doUpdate(salesOrderData)' si hace falta.
    """
    try:
        if salesOrderData['cf_2261'] != paidThroughDate:
            # Actualiza campos
            salesOrderData['cf_2261'] = paidThroughDate
            salesOrderData['productid'] = '14x29415'
            salesOrderData['assigned_user_id'] = '19x113'
            salesOrderData['LineItems'] = {
                'productid': '14x29415',
                'listprice': '0',
                'quantity': '1'
            }
            # Llamada a la API real
            return client.doUpdate(salesOrderData)
    except Exception as e:
        print(f"info actualizando memberID: {e}")
        return None

def main():
    current_date = datetime.datetime.now().strftime("%B %dth, %Y")

    # Configuración del logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers = []  # Limpiar handlers previos

    # Definir las columnas que tendrá el CSV
    fieldnames = ['date', 'time', 'memberid', 'description']
    csv_handler = CSVHandler('problems.csv', fieldnames=fieldnames)
    # Configuramos un formato (solo se utiliza para obtener el mensaje, por ejemplo)
    formatter = logging.Formatter("%(message)s")
    csv_handler.setFormatter(formatter)
    logger.addHandler(csv_handler)

    parser = argparse.ArgumentParser(description='CLI Tool for import in VTigerCRM 6.X')
    parser.add_argument('csv', type=str, help='CSV for import')
    parser.add_argument('company', type=str, help='Company for import')
    parser.add_argument('broker', type=str, help='Broker for import')
    args = parser.parse_args()

    # 1) Leer el CSV con cuDF (transformación en GPU).
    df = pd.read_csv(args.csv, delimiter=',')

    if df.empty:
        print("No hay filas que cumplan la condición para actualizar.")
        return
    
    data = fields(args.company)

    # 2) Calcular Paid Through Date si la compañía es Oscar (ejemplo).
    if args.company == 'Oscar':
        df['Paid Through Date'] = df['Policy status'].apply(calculate_paid_through_date)

    if args.company == 'Aetna':
        df['Policy Term Date'] = df['Effective Date'].apply(calculate_term_date)

    # 3) Filtrar filas según las condiciones definidas para la compañía.
    conditions = conditional_update(args.company)

    cond = '=='

    for key, value in conditions.items():
        if key == 'cond':
            cond = value
            continue
        
        if cond == '==':
            df = df[df[key] == value]
        else:
            df = df[df[key] != value]

    # 4) Aquí ya tenemos df filtrado. Si el DataFrame está vacío, no hay nada que actualizar.

    # 6) Opcional: Convertir 'Paid Through Date' a datetime en GPU (si es que necesitamos manipularla).
    #    En este ejemplo, la usaremos como string para la API; lo dejamos como está.
    #    Si quisieras validarla, podrías hacer:
    # df[data['paidThroughDate']] = cudf.to_datetime(
    #     df[data['paidThroughDate']], 
    #     format=data['format'],
    #     infos='coerce'
    # )

    # 7) Convertir a pandas ÚNICAMENTE el subconjunto que necesitamos
    #    para iterar fila por fila e invocar la API.

    if args.company != 'Aetna':
        df_pd = df[[data['memberID'], data['paidThroughDate'], data['policyTermDate']]]
    else:
        df_pd = df[[data['memberID'], data['paidThroughDate']]]
        df_pd['Broker Term Date'] = '12/31/2025'

    # 8) Iterar en CPU (pandas) y llamar a la API
    #    (No podemos usar GPU para llamadas HTTP fila por fila).
    
    #Rename colums
    new_columns = ['memberID', 'paidThroughDate', 'policyTermDate']
    mapping = dict(zip(df_pd.columns, new_columns))
    df_pd.rename(columns=mapping, inplace=True)
    df_in_df_pd = None
    with Session() as session:
        result = session.execute(
            text(f"""
                SELECT member_id, so_no
                FROM vtigercrm_2022.calendar_2025_materialized AS a
                WHERE a.Compañía = '{args.company}'
                    AND a.Broker = '{'BEATRIZ SIERRA' if args.broker == 'BS' else 'ANA DANIELLA CORRALES'}'
                    AND a.Terminación >= DATE_FORMAT(CURRENT_DATE(), '%Y-%m-%d')
                    AND a.Month = DATE_FORMAT(CURRENT_DATE(), '%Y-%m-01')
                    AND a.rn = a.OV_Count;
            """)
        ).fetchall()
        df_crm = pd.DataFrame(result, columns=["memberID", "salesOrder_no"])
        df_merged = pd.merge(df_crm, df_pd, on="memberID", how="outer", indicator=True)
        df_not_in_df_pd = df_merged[df_merged['_merge'] == 'left_only']
        df_in_df_pd = df_merged[df_merged['_merge'] != 'left_only']
        for _, row in tqdm(df_not_in_df_pd.iterrows(), total=len(df_not_in_df_pd), desc="Validando Órdenes de Venta..."):
            memberID = str(row['memberID'])
            if memberID == '':
                salesOrder_no = str(row['salesOrder_no'])
                logger.info(f"Se encontró una orden de venta pero no está en el portal", extra={'memberid': salesOrder_no})
            else:
                logger.info(f"Se encontró una orden de venta pero no está en el portal", extra={'memberid': memberID})

    for _, row in tqdm(df_pd.iterrows(), total=len(df_pd), desc="Actualizando Órdenes de Venta..."):
        memberID = str(row['memberID'])
        paidThroughDateString = str(row['paidThroughDate'])  # Formato original, e.g. '04/30/2024'  
        policyTermDateString = str(row['policyTermDate'])
        paidThroughDate = None
        policyTermDate = None

        if not paidThroughDateString in ('None', '', 'nan'):
            paidThroughDate = datetime.datetime.strptime(paidThroughDateString, data['format']).date()
        
        if not policyTermDateString in ('None', 'nan'):
            policyTermDate = datetime.datetime.strptime(policyTermDateString, '%m/%d/%Y').date()

        if policyTermDate and args.company == 'Molina':
            policyTermDate = datetime.datetime.strptime('12/31/2025', '%m/%d/%Y').date()

            # Simula búsqueda/obtención de la orden de venta (ejemplo).
            # Reemplaza con tu lógica de query a Vtiger.
            # Nota: Al usar .doQuery puedes recuperar un dict con la info del SalesOrder.
            # En este ejemplo, estamos suponiendo un solo resultado.
        
        if (policyTermDate and policyTermDate > datetime.date(2025, 1, 1)) or (paidThroughDate and paidThroughDate > datetime.date(2025, 1, 1)):
            try:
                # Hacemos la consulta ordenada por 'cf_2193' de manera descendente
                results = None
                with Session() as session:
                    results = session.execute(
                        text(f"""
                            SELECT *
                            FROM vtigercrm_2022.calendar_2025_materialized AS a
                            WHERE a.member_id = '{memberID}'
                                AND a.Terminación >= DATE_FORMAT(CURRENT_DATE(), '%Y-%m-%d')
                                AND a.Month >= DATE_FORMAT(CURRENT_DATE(), '%Y-%m-01')
                            LIMIT 1;
                        """)
                    ).fetchone()
                
                if results:
                    problem = results[10]
                    paidThroughDateCRM = results[12]
                    salesOrderTermDateCRM = results[13]
                    salesorder_no = results[1]

                    if problem == 'Problema Pago':
                        pass
                    elif salesOrderTermDateCRM:
                        if salesOrderTermDateCRM < datetime.date(2025, 1, 1) and salesOrderTermDateCRM != policyTermDate:
                            logger.info(f"La póliza está en crm con una fecha inferior al 2025-01-01 o tiene mal el policy status", extra={'memberid': memberID})
                        else:
                            if paidThroughDate and paidThroughDate >= datetime.datetime.strptime(last_day_of_month(datetime.date.today()), '%B %d, %Y').date():
                                [salesOrderData] = client.doQuery(f"""
                                    SELECT * 
                                    FROM SalesOrder
                                    WHERE salesorder_no = '{salesorder_no}'
                                    LIMIT 1
                                """)
                                if paidThroughDateCRM and paidThroughDate < paidThroughDateCRM:
                                    if not (args.company == 'Oscar' and paidThroughDateCRM >= paidThroughDate):                                     
                                        logger.info(f"A la póliza le rebotó la fecha de pago", extra={'memberid': memberID})
                                elif not paidThroughDateCRM or paidThroughDate > paidThroughDateCRM:
                                    response = update_sales_order(memberID, paidThroughDate.strftime('%Y-%m-%d'), salesOrderData)
                                    if response and not response['success']:
                                        logger.info(f"info actualizando la orden de venta: {response['error']}", extra={'memberid': memberID})
                                else:
                                    pass
                            else:
                                logger.info(f"Se encontró una orden de venta pero no está paga al {datetime.datetime.strptime(last_day_of_month(datetime.date.today()), '%B %d, %Y').date().strftime('%Y-%m-%d')}", extra={'memberid': memberID})
                    else:
                        logger.info(f"No se encontró una orden de venta pero si está en el portal", extra={'memberid': memberID})

                elif (policyTermDate and policyTermDate > datetime.date(2025, 1, 1)) or (paidThroughDate and paidThroughDate > datetime.date(2025, 1, 1)):
                    logger.info(f"La póliza no está en el crm", extra={'memberid': memberID})
            except Exception as e:
                # Si no hay resultado o ocurre algún info
                continue



if __name__ == '__main__':
    main()