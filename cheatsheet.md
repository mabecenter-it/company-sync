# Consultar el nombre del campo en la base de datos, de un campo en el CRM
fields = client.doDescribe("SalesOrder")['fields']
CFCRMMemberID = [field for field in fields if field['label'] == 'Número de Miembro']

# Realizar una consulta SQL a la API de VTigerCRM
memberID = df.iloc[0]['Issuer Assigned ID']
salesOrder = client.doQuery("SELECT * FROM SalesOrder WHERE cf_2119 = '" + memberID + "'")