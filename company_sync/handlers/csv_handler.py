import logging
import csv
from datetime import datetime

class CSVHandler(logging.Handler):
    def __init__(self, filename, fieldnames, mode='a', encoding='utf-8'):
        super().__init__()
        self.filename = filename
        self.fieldnames = fieldnames
        self.mode = mode
        self.encoding = encoding
        self.file = open(self.filename, self.mode, newline='', encoding=self.encoding)
        self.writer = csv.DictWriter(self.file, fieldnames=self.fieldnames)
        # Escribir el encabezado si el archivo está vacío
        if self.file.tell() == 0:
            self.writer.writeheader()

    def emit(self, record):
        try:
            # Convertir el timestamp del registro en datetime y formatear la fecha y hora
            record_time = datetime.fromtimestamp(record.created)
            log_entry = {
                'company': getattr(record, 'company', ''),
                'broker': getattr(record, 'broker', ''),
                'date': record_time.strftime('%Y-%m-%d'),
                'time': record_time.strftime('%H:%M:%S'),
                'memberid': getattr(record, 'memberid', ''),
                'description': record.getMessage()
            }
            self.writer.writerow(log_entry)
            self.file.flush()
        except Exception:
            self.handleError(record)

    def close(self):
        self.file.close()
        super().close()