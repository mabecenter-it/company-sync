import cudf
import argparse
import datetime
from tqdm import tqdm

# Librerías externas y cliente de Vtiger
from WSClient import Vtiger_WSClient

# Conectar a la API de VTigerCRM
client = Vtiger_WSClient('')
client.doLogin('', '')

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
    if company == 'Oscar':
        return {}  # Sin filtros
    if company == 'Molina':
        return {
            'Policy status': 'Active',
        }
    return {}

def fields(company: str) -> dict:
    """
    Devuelve los nombres de las columnas relevantes según la compañía
    y el formato de fecha correspondiente.
    """
    if company == 'Aetna':
        return {
            'memberID': 'Issuer Assigned ID',
            'paidThroughDate': 'Paid Through Date',
            'format': '%B %d, %Y'
        }
    if company == 'Ambetter':
        return {
            'memberID': 'Policy Number',
            'paidThroughDate': 'Paid Through Date',
            'format': '%m/%d/%Y'
        }
    if company == 'Oscar':
        return {
            'memberID': 'Member ID',
            'paidThroughDate': 'Paid Through Date',
            'format': '%m/%d/%Y'
        }
    if company == 'Molina':
        return {
            'memberID': 'Subscriber_ID',
            'paidThroughDate': 'Paid_Through_Date',
            'format': '%m/%d/%Y'
        }
    # Ajusta según tus otras compañías
    return {
        'memberID': 'Member ID',
        'paidThroughDate': 'Paid Through Date',
        'format': '%m/%d/%Y'
    }

def last_day_of_month(any_day: datetime.date) -> str:
    """
    Retorna el último día del mes para la fecha proporcionada, en formato MM/DD/YYYY.
    """
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    date = next_month - datetime.timedelta(days=next_month.day)
    return date.strftime('%m/%d/%Y')

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
            client.doUpdate(salesOrderData)
    except Exception as e:
        print(f"Error actualizando {memberID}: {e}")

def main():
    parser = argparse.ArgumentParser(description='CLI Tool for import in VTigerCRM 6.X')
    parser.add_argument('csv', type=str, help='CSV for import')
    parser.add_argument('company', type=str, help='Company for import')
    args = parser.parse_args()

    # 1) Leer el CSV con cuDF (transformación en GPU).
    df = cudf.read_csv(args.csv, delimiter=',')

    # 2) Calcular Paid Through Date si la compañía es Oscar (ejemplo).
    if args.company == 'Oscar':
        df['Paid Through Date'] = df['Policy status'].applymap(calculate_paid_through_date)

    # 3) Filtrar filas según las condiciones definidas para la compañía.
    conditions = conditional_update(args.company)
    for key, value in conditions.items():
        df = df[df[key] == value]

    # 4) Aquí ya tenemos df filtrado. Si el DataFrame está vacío, no hay nada que actualizar.
    if df.empty:
        print("No hay filas que cumplan la condición para actualizar.")
        return

    # 5) Preparar la información de campos.
    data = fields(args.company)

    # 6) Opcional: Convertir 'Paid Through Date' a datetime en GPU (si es que necesitamos manipularla).
    #    En este ejemplo, la usaremos como string para la API; lo dejamos como está.
    #    Si quisieras validarla, podrías hacer:
    # df[data['paidThroughDate']] = cudf.to_datetime(
    #     df[data['paidThroughDate']], 
    #     format=data['format'],
    #     errors='coerce'
    # )

    # 7) Convertir a pandas ÚNICAMENTE el subconjunto que necesitamos
    #    para iterar fila por fila e invocar la API.
    df_pd = df[[data['memberID'], data['paidThroughDate']]].to_pandas()

    # 8) Iterar en CPU (pandas) y llamar a la API
    #    (No podemos usar GPU para llamadas HTTP fila por fila).
    for _, row in tqdm(df_pd.iterrows(), total=len(df_pd), desc="Actualizando Órdenes de Venta..."):
        memberID = str(row[data['memberID']])
        paidThroughDateString = str(row[data['paidThroughDate']])  # Formato original, e.g. '04/30/2024'  

        if paidThroughDateString != 'None':
            paidThroughDate = datetime.datetime.strptime(paidThroughDateString, data['format']).date()

            # Simula búsqueda/obtención de la orden de venta (ejemplo).
            # Reemplaza con tu lógica de query a Vtiger.
            # Nota: Al usar .doQuery puedes recuperar un dict con la info del SalesOrder.
            # En este ejemplo, estamos suponiendo un solo resultado.
            try:
                # Hacemos la consulta ordenada por 'cf_2193' de manera descendente
                results = client.doQuery(f"""
                    SELECT * 
                    FROM SalesOrder
                    WHERE cf_2119 = '{memberID}' AND cf_2141 = 'Active'
                    OR cf_2119 = '{memberID}' AND cf_2141 = 'Initial Enrollment'
                    ORDER BY cf_2193 DESC
                    LIMIT 1
                """)
                
                if results:
                    # Si solo quieres el primer resultado (más reciente o más grande en cf_2193):
                    [salesOrderData] = results
                elif datetime.datetime(str(row['cf_2261']), data['format']).date() != 'None':
                    # Implement logging
                    logging.error(f"No se encontró una orden de venta para {memberID}")
            except Exception as e:
                # Si no hay resultado o ocurre algún error
                continue

            # Configurar valor inicial en salesOrderData
            if 'cf_2261' not in salesOrderData:
                salesOrderData['cf_2261'] = ''

            # Llamar a la función de actualización
            update_sales_order(memberID, paidThroughDate.strftime('%Y-%m-%d'), salesOrderData)

if __name__ == '__main__':
    main()