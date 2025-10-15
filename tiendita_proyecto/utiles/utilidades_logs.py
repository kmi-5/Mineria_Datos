import os
from datetime import datetime
from config.database import BASE_AUXILIAR

def registrar_cambio(nombre_tabla, cambios):
    log_path = os.path.join(BASE_AUXILIAR, "log_cambios.txt")
    with open(log_path, "a", encoding="utf-8") as log:
        log.write(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {nombre_tabla}\n")
        for linea in cambios:
            log.write(f"   - {linea}\n")