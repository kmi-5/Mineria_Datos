import psycopg2
from psycopg2 import sql
import os
from config.database import USER, PASSWORD, HOST, PORT

def conectar_postgres(db_name):
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )
        return conn
    except Exception as e:
        print(f"❌ Error conectando a PostgreSQL: {e}")
        return None

def actualizar_tabla_postgres(conn, df, tabla):
    try:
        cursor = conn.cursor()
        
        # Desactivar constraints
        cursor.execute("SET session_replication_role = 'replica';")
        
        # Truncar tabla
        cursor.execute(sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE;").format(sql.Identifier(tabla)))
        conn.commit()

        # Exportar a temporal y cargar
        temp_path = f"temp/temp_{tabla}.csv"
        os.makedirs("temp", exist_ok=True)
        df.to_csv(temp_path, index=False)

        with open(temp_path, 'r', encoding='utf-8') as f:
            cursor.copy_expert(sql.SQL(f"COPY {tabla} FROM STDIN WITH CSV HEADER DELIMITER ','"), f)
        conn.commit()

        # Reactivar constraints
        cursor.execute("SET session_replication_role = 'origin';")
        conn.commit()

        os.remove(temp_path)
        print(f"📤 '{tabla}' actualizada en PostgreSQL.")
        return True
        
    except Exception as e:
        print(f"❌ Error actualizando {tabla} en PostgreSQL: {e}")
        conn.rollback()
        return False