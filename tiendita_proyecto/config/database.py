import os
from dotenv import load_dotenv

load_dotenv()

# Datos de conexión PostgreSQL
USER = os.getenv('DB_USER', 'postgres')
PASSWORD = os.getenv('DB_PASSWORD', '')
HOST = os.getenv('DB_HOST', 'localhost')      
PORT = os.getenv('DB_PORT', '5432')           
DB = os.getenv('DB_NAME', 'tiendita_auxiliar')

# Carga de base de datos
BASE_ORIGINAL = "tiendita_csv/"               
BASE_AUXILIAR = "tiendita_auxiliar_csv/"      