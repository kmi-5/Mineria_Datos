import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from datetime import datetime
import glob
import json

# Cargar variables de entorno
load_dotenv()

# =============================================================================
# CONFIGURACIÓN - CONEXIÓN POSTGRESQL
# =============================================================================

# Datos de conexión PostgreSQL con valores por defecto
USER = os.getenv('DB_USER', 'postgres')
PASSWORD = os.getenv('DB_PASSWORD', '55.kmi')  
HOST = os.getenv('DB_HOST', 'localhost')      
PORT = os.getenv('DB_PORT', '5432')           
DB = os.getenv('DB_NAME', 'tiendita_auxiliar')

# Rutas de archivos CSV
BASE_ORIGINAL = "D:/Proyectos/SQL/Mineria_Datos/tiendita_proyecto/tiendita_csv"        
BASE_AUXILIAR = "D:/Proyectos/SQL/Mineria_Datos/tiendita_proyecto/tiendita_auxiliar_csv"    

# Variable global para referencia de tablas
tablas_referencia = None
# Variable global para conexión persistente
postgres_conn = None

# =============================================================================
# FUNCIONES DE MANEJO DE ARCHIVOS CSV
# =============================================================================

def cargar_tablas_desde_auxiliar():
    """Carga las tablas desde la carpeta AUXILIAR sin modificar los datos"""
    dataframes = {}
    
    # Cargar directamente desde auxiliar (asume que existe)
    for ruta in glob.glob(os.path.join(BASE_AUXILIAR, "*.csv")):
        nombre = os.path.basename(ruta).replace(".csv", "")
        # Excluir el archivo de log
        if nombre != "log_cambios":
            try:
                # Cargar sin modificar - mantener todos los datos originales
                dataframes[nombre] = pd.read_csv(ruta, keep_default_na=False)
            except Exception as e:
                print(f"❌ Error cargando {nombre}: {e}")
    
    return dataframes

def guardar_tabla_individual(df, nombre_tabla):
    """Guarda una tabla individual en la carpeta auxiliar sin limpiar automáticamente"""
    try:
        os.makedirs(BASE_AUXILIAR, exist_ok=True)
        ruta_salida = os.path.join(BASE_AUXILIAR, f"{nombre_tabla}.csv")
        
        # Guardar el archivo SIN limpiar automáticamente
        df.to_csv(ruta_salida, index=False, encoding='utf-8')
        print(f"💾 Guardado: {nombre_tabla} ({len(df)} registros)")
        
    except Exception as e:
        print(f"❌ Error guardando {nombre_tabla}: {e}")

def conectar_postgres_persistente():
    """Conecta a PostgreSQL y mantiene la conexión abierta"""
    global postgres_conn
    try:
        if postgres_conn is None or postgres_conn.closed:
            postgres_conn = psycopg2.connect(
                dbname=DB,
                user=USER,
                password=PASSWORD,
                host=HOST,
                port=PORT
            )
            print("🔌 Conexión PostgreSQL establecida")
        return postgres_conn
    except Exception as e:
        print(f"❌ Error conectando a PostgreSQL: {e}")
        return None

def cerrar_conexion_postgres():
    """Cierra la conexión a PostgreSQL"""
    global postgres_conn
    if postgres_conn and not postgres_conn.closed:
        postgres_conn.close()
        print("🔌 Conexión PostgreSQL cerrada")
        postgres_conn = None

def guardar_y_sincronizar(df, nombre_tabla):
    """Guarda automáticamente en CSV y sincroniza con PostgreSQL usando conexión persistente"""
    # 1. Guardar en CSV auxiliar
    guardar_tabla_individual(df, nombre_tabla)

    # 2. Intentar sincronizar con PostgreSQL usando conexión persistente
    conn = conectar_postgres_persistente()
    if conn:
        try:
            print(f"🔄 Sincronizando '{nombre_tabla}' con PostgreSQL...")
            if actualizar_tabla_postgres(conn, df, nombre_tabla):
                conn.commit()
                print(f"✅ '{nombre_tabla}' sincronizada correctamente en PostgreSQL.")
            else:
                conn.rollback()
                print(f"❌ Error sincronizando '{nombre_tabla}' en PostgreSQL")
        except Exception as e:
            conn.rollback()
            print(f"❌ Error sincronizando '{nombre_tabla}' en PostgreSQL: {e}")
    else:
        print("⚠️ No se pudo conectar a PostgreSQL. Se guardó solo el CSV auxiliar.")

def registrar_cambio(nombre_tabla, cambios):
    """Registras cambios en el archivo log"""
    log_path = os.path.join(BASE_AUXILIAR, "log_cambios.txt")
    with open(log_path, "a", encoding="utf-8") as log:
        log.write(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {nombre_tabla}\n")
        for linea in cambios:
            log.write(f"   - {linea}\n")

# =============================================================================
# FUNCIONES PARA DETECCIÓN Y GESTIÓN DE IDs AUTOINCREMENTABLES
# =============================================================================

def detectar_columna_id(df):
    """Detectar automáticamente la columna que funciona como ID"""
    # Patrones comunes para columnas de ID
    patrones_id = [
        'id', 'ID', 'Id', 'codigo', 'cod', 'codigo', 'numero', 'nro', 
        'clave', 'key', 'identificador'
    ]
    
    # Buscar en nombres de columnas
    for col in df.columns:
        col_lower = col.lower()
        
        # Verificar si el nombre de la columna contiene algún patrón de ID
        for patron in patrones_id:
            if patron in col_lower:
                return col
        
        # Verificar si la columna tiene valores únicos y secuenciales (como un ID)
        if (df[col].dtype in ['int64', 'float64'] and 
            df[col].is_monotonic_increasing and 
            len(df[col].unique()) == len(df)):
            return col
    
    # Si no encuentra, usar la primera columna numérica
    columnas_numericas = df.select_dtypes(include=['int64', 'float64']).columns
    if len(columnas_numericas) > 0:
        return columnas_numericas[0]
    
    # Si no hay columnas numéricas, usar la primera columna
    return df.columns[0] if len(df.columns) > 0 else None

def obtener_siguiente_id(df, columna_id):
    """Obtener el siguiente ID automáticamente"""
    if df.empty or columna_id is None:
        return 1
    
    if columna_id in df.columns:
        try:
            # Convertir a numérico y ignorar valores no numéricos
            ids_numericos = pd.to_numeric(df[columna_id], errors='coerce')
            ids_numericos = ids_numericos.dropna()
            
            if len(ids_numericos) > 0:
                siguiente = int(ids_numericos.max()) + 1
            else:
                siguiente = len(df) + 1
        except:
            siguiente = len(df) + 1
    else:
        siguiente = len(df) + 1
    
    return siguiente

def reindexar_ids(df, columna_id):
    """Reindexa los IDs de forma secuencial después de eliminar registros"""
    if columna_id is None or columna_id not in df.columns or df.empty:
        return df
    
    try:
        # Crear nueva columna con IDs secuenciales empezando desde 1
        df_reindexado = df.copy()
        df_reindexado[columna_id] = range(1, len(df) + 1)
        return df_reindexado
    except Exception as e:
        print(f"❌ Error reindexando IDs: {e}")
        return df

def es_columna_id(columna):
    """Determina si una columna es un ID que debe ser ocultada"""
    palabras_id = ['id', 'codigo', 'cod', 'key', 'clave', 'numero', 'nro']
    return any(palabra in columna.lower() for palabra in palabras_id)

def obtener_columnas_visibles(df):
    """Obtiene las columnas que deben mostrarse al usuario (ocultando IDs)"""
    return [col for col in df.columns if not es_columna_id(col)]

def obtener_columnas_ocultas(df):
    """Obtiene las columnas ID que están ocultas"""
    return [col for col in df.columns if es_columna_id(col)]

def obtener_vista_usuario(df):
    """Retorna una vista del DataFrame sin las columnas ID"""
    columnas_visibles = obtener_columnas_visibles(df)
    return df[columnas_visibles] if columnas_visibles else df

def limpiar_y_convertir_ids(df):
    """Limpia y convierte las columnas ID a numéricas, eliminando duplicados - SOLO CUANDO SE SOLICITA"""
    df_limpio = df.copy()
    columna_id_principal = detectar_columna_id(df_limpio)
    columnas_ocultas = obtener_columnas_ocultas(df_limpio)
    
    for columna in columnas_ocultas:
        if columna in df_limpio.columns:
            # Convertir a numérico, los no convertibles se convierten en NaN
            df_limpio[columna] = pd.to_numeric(df_limpio[columna], errors='coerce')
    
    # Eliminar filas con IDs duplicados, manteniendo la primera ocurrencia
    if columna_id_principal and columna_id_principal in df_limpio.columns:
        df_limpio = df_limpio.drop_duplicates(subset=[columna_id_principal], keep='first')
    
    # Reindexar IDs después de limpiar duplicados
    if columna_id_principal and columna_id_principal in df_limpio.columns:
        df_limpio = reindexar_ids(df_limpio, columna_id_principal)
    
    return df_limpio

def limpiar_tabla_manual(df, nombre_tabla):
    """Función para limpiar manualmente una tabla (opción del menú)"""
    print(f"🧹 Limpiando tabla {nombre_tabla}...")
    df_limpio = limpiar_y_convertir_ids(df)
    cambios = len(df) - len(df_limpio)
    if cambios > 0:
        print(f"✅ Se limpiaron {cambios} registros duplicados/erróneos")
    else:
        print("✅ No se encontraron registros duplicados/erróneos")
    return df_limpio

def generar_ids_automaticos(df_original, nuevo_registro):
    """Genera IDs automáticos para las columnas ocultas de forma ordenada"""
    global tablas_referencia
    
    # Usar el DataFrame original sin limpiar automáticamente
    df_trabajo = df_original.copy()
    columnas_ocultas = obtener_columnas_ocultas(df_trabajo)
    
    for columna in columnas_ocultas:
        # Si la columna no está en el nuevo registro o está vacía
        if columna not in nuevo_registro or nuevo_registro[columna] is None or nuevo_registro[columna] == '':
            
            # Detectar si es la columna ID principal
            columna_id_principal = detectar_columna_id(df_trabajo)
            
            if columna == columna_id_principal:
                # Para la columna ID principal, usar el siguiente ID disponible
                siguiente_id = obtener_siguiente_id(df_trabajo, columna)
                nuevo_registro[columna] = siguiente_id
            
            # Para claves foráneas (id_otra_tabla)
            elif columna.startswith('id_'):
                tabla_referenciada = columna[3:]  # Remover 'id_' del inicio
                
                # Usar la referencia global a las tablas
                if tablas_referencia and tabla_referenciada in tablas_referencia:
                    df_referencia = tablas_referencia[tabla_referenciada]
                    
                    # Buscar la columna ID principal de la tabla referenciada
                    columna_id_referencia = detectar_columna_id(df_referencia)
                    
                    if columna_id_referencia and len(df_referencia) > 0:
                        # Tomar el primer ID disponible de la tabla referenciada
                        try:
                            # Buscar primer valor numérico válido
                            primer_id = 1
                            for val in df_referencia[columna_id_referencia]:
                                try:
                                    if pd.notna(val) and str(val).strip() != '':
                                        primer_id = int(float(val))
                                        break
                                except (ValueError, TypeError):
                                    continue
                            
                            nuevo_registro[columna] = primer_id
                        except Exception as e:
                            nuevo_registro[columna] = 1
                    else:
                        nuevo_registro[columna] = 1
                else:
                    nuevo_registro[columna] = 1
            
            # Para otros tipos de IDs
            else:
                # Para otras columnas ID, usar el siguiente ID disponible
                siguiente_valor = obtener_siguiente_id(df_trabajo, columna)
                nuevo_registro[columna] = siguiente_valor
    
    return nuevo_registro

# =============================================================================
# FUNCIONES DE CONEXIÓN Y SINCRONIZACIÓN CON POSTGRESQL
# =============================================================================

def test_conexion_postgres():
    """Función para probar la conexión independientemente"""
    print("\n🧪 TEST DE CONEXIÓN A POSTGRESQL")
    print("="*40)
    
    conn = conectar_postgres_persistente()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            print(f"✅ PostgreSQL version: {version[0]}")
            
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            tablas = cursor.fetchall()
            print(f"📊 Tablas disponibles: {[tabla[0] for tabla in tablas]}")
            
            return True
        except Exception as e:
            print(f"❌ Error en test: {e}")
            return False
    return False

def actualizar_tabla_postgres(conn, df, tabla):
    """Actualiza una tabla en PostgreSQL manejando conflictos de duplicados"""
    if len(df) == 0:
        print(f"⚠️  DataFrame vacío para {tabla}, saltando...")
        return True
        
    try:
        cursor = conn.cursor()
        
        # Verificar si la tabla existe
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            );
        """, (tabla,))
        
        if not cursor.fetchone()[0]:
            print(f"❌ La tabla '{tabla}' no existe en PostgreSQL")
            return False
        
        # Obtener las columnas reales de la tabla en PostgreSQL
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s AND table_schema = 'public'
            ORDER BY ordinal_position;
        """, (tabla,))
        columnas_postgres = [row[0] for row in cursor.fetchall()]
        
        # Filtrar el DataFrame para que solo contenga columnas que existen en PostgreSQL
        columnas_comunes = [col for col in df.columns if col in columnas_postgres]
        
        if not columnas_comunes:
            print(f"❌ No hay columnas comunes entre CSV y PostgreSQL para {tabla}")
            return False
            
        df_filtrado = df[columnas_comunes]
        
        # Para tablas con claves foráneas, desactivar temporalmente las constraints
        if tabla in ['facturas_encabezado', 'facturas_detalle']:
            cursor.execute("SET CONSTRAINTS ALL DEFERRED;")
        
        # Verificar clave primaria
        try:
            cursor.execute("""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass AND i.indisprimary
            """, (tabla,))
            primary_key_result = cursor.fetchone()
            primary_key = primary_key_result[0] if primary_key_result else None
        except Exception as e:
            primary_key = None
        
        if not primary_key or primary_key not in columnas_comunes:
            return _actualizar_con_truncate(conn, df_filtrado, tabla)
        
        return _actualizar_con_upsert(conn, df_filtrado, tabla, primary_key)
        
    except Exception as e:
        print(f"❌ Error actualizando {tabla} en PostgreSQL: {e}")
        conn.rollback()
        return False

def _actualizar_con_upsert(conn, df, tabla, primary_key):
    """Actualiza tabla usando UPSERT para evitar duplicados"""
    try:
        cursor = conn.cursor()
        
        columnas = df.columns.tolist()
        
        # Verificar que la primary key esté en las columnas
        if primary_key not in columnas:
            return _actualizar_con_truncate(conn, df, tabla)
        
        placeholders = ', '.join(['%s'] * len(columnas))
        columnas_str = ', '.join([f'"{col}"' for col in columnas])
        
        set_clause = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in columnas if col != primary_key])
        
        query = f"""
            INSERT INTO "{tabla}" ({columnas_str}) 
            VALUES ({placeholders})
            ON CONFLICT ("{primary_key}") 
            DO UPDATE SET {set_clause}
        """
        
        registros_procesados = 0
        errores = 0
        
        for idx, row in df.iterrows():
            try:
                cursor.execute(query, tuple(row))
                registros_procesados += 1
            except Exception as row_error:
                errores += 1
                if errores <= 3:
                    print(f"⚠️  Error en fila {idx} de {tabla}: {row_error}")
                continue
        
        if errores > 0:
            print(f"⚠️  '{tabla}': {registros_procesados}/{len(df)} registros procesados, {errores} errores")
        else:
            print(f"✅ '{tabla}' actualizada en PostgreSQL. Registros procesados: {registros_procesados}/{len(df)}")
        
        return registros_procesados > 0
        
    except Exception as e:
        print(f"❌ Error en UPSERT para {tabla}: {e}")
        conn.rollback()
        return False

def _actualizar_con_truncate(conn, df, tabla):
    """Método con TRUNCATE para tablas sin clave primaria clara"""
    try:
        cursor = conn.cursor()
        
        cursor.execute("SET session_replication_role = 'replica';")
        cursor.execute(sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE;").format(sql.Identifier(tabla)))
        conn.commit()

        temp_path = f"temp_{tabla}.csv"
        df.to_csv(temp_path, index=False)

        columnas = df.columns.tolist()
        columnas_str = ', '.join([f'"{col}"' for col in columnas])
        
        with open(temp_path, 'r', encoding='utf-8') as f:
            cursor.copy_expert(sql.SQL(f'COPY "{tabla}" ({columnas_str}) FROM STDIN WITH CSV HEADER DELIMITER \',\''), f)
        conn.commit()

        cursor.execute("SET session_replication_role = 'origin';")
        conn.commit()

        os.remove(temp_path)
        print(f"📤 '{tabla}' actualizada en PostgreSQL. Registros: {len(df)}")
        return True
        
    except Exception as e:
        print(f"❌ Error en TRUNCATE para {tabla}: {e}")
        conn.rollback()
        return False

def sincronizar_postgresql(tablas):
    """Sincroniza todas las tablas con PostgreSQL usando conexión persistente"""
    global postgres_conn
    try:
        print("\n🔄 Sincronizando con PostgreSQL...")
        
        conn = conectar_postgres_persistente()
        
        if not conn:
            print("❌ No se pudo conectar a PostgreSQL")
            return False
        
        # Verificar que la conexión es válida
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        
        # Orden correcto considerando dependencias
        orden_tablas = [
            'provincias', 'localidades', 'condicion_iva', 'rubros', 
            'proveedores', 'sucursales', 'clientes', 'productos', 
            'facturas_encabezado', 'facturas_detalle', 'ventas'
        ]
        
        resultados = []
        exitosas = 0
        
        # PRIMERA PASADA: Tablas sin dependencias complejas
        tablas_primera_pasada = ['provincias', 'localidades', 'condicion_iva', 'rubros', 
                                'proveedores', 'sucursales', 'clientes', 'productos']
        
        for tabla in tablas_primera_pasada:
            if tabla in tablas:
                try:
                    df = tablas[tabla]
                    if len(df) == 0:
                        print(f"⚠️  Tabla {tabla} está vacía, saltando...")
                        continue
                        
                    print(f"📊 Procesando {tabla} ({len(df)} registros)...")
                    
                    if actualizar_tabla_postgres(conn, df, tabla):
                        # Hacer commit explícito después de cada tabla
                        conn.commit()
                        resultados.append(f"✅ '{tabla}' sincronizada")
                        exitosas += 1
                        print(f"✅ '{tabla}' sincronizada exitosamente")
                    else:
                        conn.rollback()  # Revertir cambios si hay error
                        resultados.append(f"❌ Falló la sincronización de {tabla}")
                        print(f"❌ Falló la sincronización de {tabla}")
                        
                except Exception as e:
                    print(f"❌ Error actualizando {tabla} en PostgreSQL: {e}")
                    if conn:
                        conn.rollback()
                    resultados.append(f"❌ '{tabla}' error: {str(e)}")
        
        # SEGUNDA PASADA: Tablas con dependencias - con verificación de integridad
        tablas_segunda_pasada = ['facturas_encabezado', 'facturas_detalle', 'ventas']
        
        for tabla in tablas_segunda_pasada:
            if tabla in tablas:
                try:
                    df = tablas[tabla]
                    if len(df) == 0:
                        print(f"⚠️  Tabla {tabla} está vacía, saltando...")
                        continue
                        
                    print(f"📊 Procesando {tabla} ({len(df)} registros)...")
                    
                    # Para facturas_encabezado, verificar que los clientes existan
                    if tabla == 'facturas_encabezado' and 'clientes' in tablas:
                        cursor = conn.cursor()
                        cursor.execute("""
                            SELECT column_name 
                            FROM information_schema.columns 
                            WHERE table_name = 'clientes' 
                            AND table_schema = 'public'
                            ORDER BY ordinal_position LIMIT 1;
                        """)
                        result = cursor.fetchone()
                        if result:
                            id_cliente_col = result[0]
                            
                            cursor.execute(f"SELECT {id_cliente_col} FROM clientes")
                            clientes_existentes = set([row[0] for row in cursor.fetchall()])
                            
                            columna_cliente_factura = None
                            for col in df.columns:
                                if 'cliente' in col.lower():
                                    columna_cliente_factura = col
                                    break
                            
                            if columna_cliente_factura:
                                df_filtrado = df[df[columna_cliente_factura].isin(clientes_existentes)]
                                
                                if len(df_filtrado) < len(df):
                                    perdidos = len(df) - len(df_filtrado)
                                    print(f"⚠️  Se omitieron {perdidos} registros por clientes inexistentes")
                                
                                success = actualizar_tabla_postgres(conn, df_filtrado, tabla)
                            else:
                                success = actualizar_tabla_postgres(conn, df, tabla)
                        else:
                            success = actualizar_tabla_postgres(conn, df, tabla)
                        cursor.close()
                    
                    # Para facturas_detalle, verificar que las facturas existan
                    elif tabla == 'facturas_detalle' and 'facturas_encabezado' in tablas:
                        cursor = conn.cursor()
                        cursor.execute("""
                            SELECT column_name 
                            FROM information_schema.columns 
                            WHERE table_name = 'facturas_encabezado' 
                            AND table_schema = 'public'
                            ORDER BY ordinal_position LIMIT 1;
                        """)
                        result = cursor.fetchone()
                        if result:
                            id_factura_col = result[0]
                            
                            cursor.execute(f"SELECT {id_factura_col} FROM facturas_encabezado")
                            facturas_existentes = set([row[0] for row in cursor.fetchall()])
                            
                            columna_factura_detalle = None
                            for col in df.columns:
                                if 'factura' in col.lower():
                                    columna_factura_detalle = col
                                    break
                            
                            if columna_factura_detalle:
                                df_filtrado = df[df[columna_factura_detalle].isin(facturas_existentes)]
                                
                                if len(df_filtrado) < len(df):
                                    perdidos = len(df) - len(df_filtrado)
                                    print(f"⚠️  Se omitieron {perdidos} registros por facturas inexistentes")
                                
                                success = actualizar_tabla_postgres(conn, df_filtrado, tabla)
                            else:
                                success = actualizar_tabla_postgres(conn, df, tabla)
                        else:
                            success = actualizar_tabla_postgres(conn, df, tabla)
                        cursor.close()
                    
                    else:
                        success = actualizar_tabla_postgres(conn, df, tabla)
                    
                    if success:
                        conn.commit()  # Commit explícito
                        resultados.append(f"✅ '{tabla}' sincronizada")
                        exitosas += 1
                        print(f"✅ '{tabla}' sincronizada exitosamente")
                    else:
                        conn.rollback()
                        resultados.append(f"❌ Falló la sincronización de {tabla}")
                        print(f"❌ Falló la sincronización de {tabla}")
                        
                except Exception as e:
                    print(f"❌ Error actualizando {tabla} en PostgreSQL: {e}")
                    if conn:
                        conn.rollback()
                    resultados.append(f"❌ '{tabla}' error: {str(e)}")
        
        print("\n" + "="*50)
        print("📊 RESUMEN DE SINCRONIZACIÓN")
        print("="*50)
        for resultado in resultados:
            print(resultado)
        
        total_tablas_procesar = len([t for t in orden_tablas if t in tablas])
        print(f"\n✅ Tablas exitosas: {exitosas}/{total_tablas_procesar}")
        
        if exitosas == total_tablas_procesar:
            print("🎉 ¡Todas las tablas se sincronizaron correctamente!")
        else:
            print("⚠️  Algunas tablas tuvieron problemas.")
        
        # NO cerramos la conexión aquí para mantenerla persistente
        return exitosas > 0
        
    except Exception as e:
        print(f"❌ Error general en sincronización: {e}")
        if conn:
            conn.rollback()
        return False

# =============================================================================
# MENÚ INTERACTIVO COMPLETO - CON GENERACIÓN AUTOMÁTICA DE IDs
# =============================================================================

def menu_interactivo(df_original, nombre_tabla, todas_las_tablas):
    """
    Menú interactivo completo para modificar tablas
    Retorna: (dataframe_modificado, lista_cambios, volver)
    """
    global tablas_referencia
    cambios = []
    # Crear una copia profunda para trabajar SIN limpiar automáticamente
    df_trabajo = df_original.copy(deep=True)
    
    # NO limpiar automáticamente - mantener datos originales
    df_visible = obtener_vista_usuario(df_trabajo)
    
    # Detectar columna ID principal
    columna_id_principal = detectar_columna_id(df_trabajo)
    if columna_id_principal:
        print(f"🔍 Columna ID detectada: {columna_id_principal}")
    
    # Actualizar la referencia global
    tablas_referencia = todas_las_tablas
    
    while True:
        print("\n" + "="*50)
        print(f"📋 EDITOR DE TABLA: {nombre_tabla.upper()}")
        print("="*50)
        print(f"📊 Registros: {len(df_visible)} | Columnas: {len(df_visible.columns)}")
        if columna_id_principal:
            print(f"🔑 ID principal: {columna_id_principal}")
        if cambios:
            print(f"📝 Cambios pendientes: {len(cambios)}")
        print("\n--- VISUALIZACIÓN ---")
        print("1.  Ver las primeras filas")
        print("2.  Ver información del dataset")
        print("3.  Ver estadísticas descriptivas")
        print("4.  Ver columnas disponibles")

        print("\n--- EDICIÓN DE DATOS ---")
        print("5.  Agregar nueva columna")
        print("6.  Modificar columna existente")
        print("7.  Eliminar columna")
        print("8.  Agregar nuevo registro/fila")
        print("9.  Eliminar registro/fila")
        print("10. Filtrar datos")
        print("11. Buscar valores específicos")
        print("12. Limpiar tabla (eliminar duplicados y normalizar IDs)")

        print("\n--- EXPORTACIÓN Y GESTIÓN ---")
        print("13. Exportar tabla a JSON")
        print("14. Exportar tabla a Excel")
        print("15. Guardar cambios y volver")
        print("16. Volver sin guardar cambios")
        print("17. Salir del programa")

        print("\n" + "="*50)
        
        try:
            opcion = input("Selecciona una opción (1-17): ").strip()
            
            if opcion == "1":
                print(f"\nPrimeras 10 filas de '{nombre_tabla}':")
                print(df_visible.head(10).to_string())
                
            elif opcion == "2":
                print(f"\n📊 Información de '{nombre_tabla}':")
                print(f"Dimensiones: {df_visible.shape[0]} filas x {df_visible.shape[1]} columnas")
                print(f"\nTipos de datos:")
                print(df_visible.dtypes)
                print(f"\nValores nulos por columna:")
                print(df_visible.isnull().sum())
                if columna_id_principal:
                    print(f"\n🔑 Columna ID: {columna_id_principal}")
                    if columna_id_principal in df_trabajo.columns:
                        siguiente_id = obtener_siguiente_id(df_trabajo, columna_id_principal)
                        print(f"Siguiente ID disponible: {siguiente_id}")
                
            elif opcion == "3":
                print(f"\n📈 Estadísticas descriptivas de '{nombre_tabla}':")
                print(df_visible.describe(include='all').to_string())
                
            elif opcion == "4":
                print(f"\n📝 Columnas disponibles en '{nombre_tabla}':")
                for i, columna in enumerate(df_visible.columns, 1):
                    print(f"  {i}. {columna} ({df_visible[columna].dtype})")
                if columna_id_principal and columna_id_principal not in df_visible.columns:
                    print(f"  🔑 {columna_id_principal} (columna ID oculta)")
                    
            elif opcion == "5":
                nueva_columna = input("Nombre de la nueva columna: ").strip()
                if nueva_columna and nueva_columna not in df_trabajo.columns:
                    valor_default = input("Valor por defecto (dejar vacío para NaN): ").strip()
                    if valor_default:
                        try:
                            # Intentar convertir a número si es posible
                            if '.' in valor_default:
                                valor_default = float(valor_default)
                            else:
                                valor_default = int(valor_default)
                        except ValueError:
                            # Mantener como string si no se puede convertir
                            pass
                    df_trabajo[nueva_columna] = valor_default if valor_default else None
                    df_visible = obtener_vista_usuario(df_trabajo)
                    cambios.append(f"Agregada columna '{nueva_columna}'")
                    print(f"✅ Columna '{nueva_columna}' agregada")
                    guardar_y_sincronizar(df_trabajo, nombre_tabla)
                else:
                    print("❌ Nombre no válido o columna ya existe")
                    
            elif opcion == "6":
                print("\n📝 Columnas disponibles:")
                for i, columna in enumerate(df_visible.columns, 1):
                    print(f"  {i}. {columna}")
                
                try:
                    col_idx = int(input("Número de columna a modificar: ")) - 1
                    if 0 <= col_idx < len(df_visible.columns):
                        columna_visible = df_visible.columns[col_idx]
                        columna_original = columna_visible
                        
                        print(f"\n🛠️  Modificando columna: {columna_visible}")
                        print("1. Renombrar columna")
                        print("2. Cambiar tipo de datos")
                        
                        sub_opcion = input("Selecciona opción: ").strip()
                        
                        if sub_opcion == "1":
                            nuevo_nombre = input("Nuevo nombre: ").strip()
                            if nuevo_nombre and nuevo_nombre not in df_trabajo.columns:
                                df_trabajo.rename(columns={columna_original: nuevo_nombre}, inplace=True)
                                df_visible = obtener_vista_usuario(df_trabajo)
                                cambios.append(f"Renombrada columna '{columna_visible}' a '{nuevo_nombre}'")
                                print("✅ Columna renombrada")
                                guardar_y_sincronizar(df_trabajo, nombre_tabla)
                            else:
                                print("❌ Nombre no válido o ya existe")
                                
                        elif sub_opcion == "2":
                            print("Tipos disponibles: int, float, str")
                            nuevo_tipo = input("Nuevo tipo: ").strip().lower()
                            try:
                                if nuevo_tipo == 'int':
                                    df_trabajo[columna_original] = pd.to_numeric(df_trabajo[columna_original], errors='coerce').fillna(0).astype(int)
                                elif nuevo_tipo == 'float':
                                    df_trabajo[columna_original] = pd.to_numeric(df_trabajo[columna_original], errors='coerce').fillna(0.0).astype(float)
                                elif nuevo_tipo == 'str':
                                    df_trabajo[columna_original] = df_trabajo[columna_original].astype(str)
                                else:
                                    print("❌ Tipo no válido")
                                    continue
                                
                                df_visible = obtener_vista_usuario(df_trabajo)
                                cambios.append(f"Cambiado tipo de '{columna_original}' a {nuevo_tipo}")
                                print("✅ Tipo de columna cambiado")
                                guardar_y_sincronizar(df_trabajo, nombre_tabla)
                            except Exception as e:
                                print(f"❌ Error cambiando tipo: {e}")
                        else:
                            print("❌ Opción no válida")
                    else:
                        print("❌ Número de columna no válido")
                except ValueError:
                    print("❌ Ingresa un número válido")
                    
            elif opcion == "7":
                print("\n🗑️  Columnas disponibles:")
                for i, columna in enumerate(df_visible.columns, 1):
                    print(f"  {i}. {columna}")
                
                try:
                    col_idx = int(input("Número de columna a eliminar: ")) - 1
                    if 0 <= col_idx < len(df_visible.columns):
                        columna_visible = df_visible.columns[col_idx]
                        columna_original = columna_visible
                        
                        confirmar = input(f"¿Estás seguro de eliminar la columna '{columna_original}'? (s/n): ").strip().lower()
                        if confirmar == 's':
                            df_trabajo.drop(columns=[columna_original], inplace=True)
                            df_visible = obtener_vista_usuario(df_trabajo)
                            cambios.append(f"Eliminada columna '{columna_original}'")
                            print("✅ Columna eliminada")
                            guardar_y_sincronizar(df_trabajo, nombre_tabla)
                    else:
                        print("❌ Número de columna no válido")
                except ValueError:
                    print("❌ Ingresa un número válido")
                    
            elif opcion == "8":
                print(f"\n➕ AGREGAR NUEVO REGISTRO A '{nombre_tabla.upper()}'")
                print("="*40)
                
                # Mostrar columnas y tipos
                print("📝 Columnas y tipos:")
                for col in df_trabajo.columns:
                    tipo = df_trabajo[col].dtype
                    print(f"  - {col}: {tipo}")
                
                nuevo_registro = {}
                
                # Generar IDs automáticos para las columnas ocultas
                nuevo_registro = generar_ids_automaticos(df_trabajo, nuevo_registro)
                
                # Solicitar valores para las columnas visibles
                columnas_visibles = obtener_columnas_visibles(df_trabajo)
                for columna in columnas_visibles:
                    valor = input(f"Valor para '{columna}' ({df_trabajo[columna].dtype}): ").strip()
                    
                    if valor == '':
                        nuevo_registro[columna] = None
                    else:
                        try:
                            # Intentar convertir al tipo original de la columna
                            if df_trabajo[columna].dtype == 'int64':
                                nuevo_registro[columna] = int(valor)
                            elif df_trabajo[columna].dtype == 'float64':
                                nuevo_registro[columna] = float(valor)
                            else:
                                nuevo_registro[columna] = valor
                        except ValueError:
                            nuevo_registro[columna] = valor
                
                # Verificar que todos los campos requeridos estén presentes
                columnas_faltantes = set(df_trabajo.columns) - set(nuevo_registro.keys())
                for columna in columnas_faltantes:
                    nuevo_registro[columna] = None
                
                # Convertir a DataFrame y concatenar
                nuevo_df = pd.DataFrame([nuevo_registro])
                df_trabajo = pd.concat([df_trabajo, nuevo_df], ignore_index=True)
                
                # Reindexar IDs después de agregar nuevo registro
                if columna_id_principal and columna_id_principal in df_trabajo.columns:
                    df_trabajo = reindexar_ids(df_trabajo, columna_id_principal)
                
                df_visible = obtener_vista_usuario(df_trabajo)
                
                cambios.append(f"Agregado nuevo registro")
                print("✅ Nuevo registro agregado")
                
                # Mostrar el registro agregado (solo columnas visibles)
                print("\n📋 Registro agregado:")
                registro_visible = {k: v for k, v in nuevo_registro.items() if k in columnas_visibles}
                for k, v in registro_visible.items():
                    print(f"  {k}: {v}")
                
                guardar_y_sincronizar(df_trabajo, nombre_tabla)
                
            elif opcion == "9":
                print(f"\n🗑️  ELIMINAR REGISTRO DE '{nombre_tabla.upper()}'")
                print("="*40)
                
                if len(df_visible) == 0:
                    print("❌ No hay registros para eliminar")
                    continue
                
                print("Primeras 10 filas:")
                print(df_visible.head(10).to_string())
                
                try:
                    fila_idx = int(input(f"\nNúmero de fila a eliminar (0-{len(df_visible)-1}): "))
                    if 0 <= fila_idx < len(df_visible):
                        # Mostrar registro a eliminar
                        registro = df_visible.iloc[fila_idx]
                        print(f"\n📋 Registro a eliminar:")
                        for col, val in registro.items():
                            print(f"  {col}: {val}")
                        
                        confirmar = input("¿Estás seguro de eliminar este registro? (s/n): ").strip().lower()
                        if confirmar == 's':
                            # Eliminar del DataFrame de trabajo
                            df_trabajo = df_trabajo.drop(df_trabajo.index[fila_idx]).reset_index(drop=True)
                            
                            # Reindexar IDs después de eliminar
                            if columna_id_principal and columna_id_principal in df_trabajo.columns:
                                df_trabajo = reindexar_ids(df_trabajo, columna_id_principal)
                            
                            df_visible = obtener_vista_usuario(df_trabajo)
                            cambios.append(f"Eliminado registro en posición {fila_idx}")
                            print("✅ Registro eliminado")
                            guardar_y_sincronizar(df_trabajo, nombre_tabla)
                        else:
                            print("❌ Eliminación cancelada")
                    else:
                        print("❌ Número de fila no válido")
                except ValueError:
                    print("❌ Ingresa un número válido")
                    
            elif opcion == "10":
                print(f"\n🔍 FILTRAR DATOS EN '{nombre_tabla.upper()}'")
                print("="*40)
                
                print("Columnas disponibles para filtrar:")
                for i, columna in enumerate(df_visible.columns, 1):
                    print(f"  {i}. {columna}")
                
                try:
                    col_idx = int(input("Número de columna para filtrar: ")) - 1
                    if 0 <= col_idx < len(df_visible.columns):
                        columna_filtro = df_visible.columns[col_idx]
                        
                        print(f"\nFiltrar por columna: {columna_filtro}")
                        print("Ejemplos de filtros:")
                        print("  - Texto: 'valor_exacto' o 'parte_del_texto'")
                        print("  - Números: '>100', '<50', '==25'")
                        print("  - Fechas: '>2023-01-01'")
                        
                        filtro = input("Condición de filtro: ").strip()
                        
                        if filtro:
                            try:
                                # Intentar evaluar como expresión
                                df_filtrado = df_visible.query(f"{columna_filtro} {filtro}")
                                print(f"\n📊 Resultados del filtro ({len(df_filtrado)} registros):")
                                print(df_filtrado.to_string())
                            except:
                                # Filtro por texto
                                df_filtrado = df_visible[df_visible[columna_filtro].astype(str).str.contains(filtro, case=False, na=False)]
                                print(f"\n📊 Resultados del filtro ({len(df_filtrado)} registros):")
                                print(df_filtrado.to_string())
                        else:
                            print("❌ Filtro vacío")
                    else:
                        print("❌ Número de columna no válido")
                except ValueError:
                    print("❌ Ingresa un número válido")
                    
            elif opcion == "11":
                print(f"\n🔎 BUSCAR VALORES EN '{nombre_tabla.upper()}'")
                print("="*40)
                
                termino = input("Término a buscar: ").strip()
                if termino:
                    # Buscar en todas las columnas
                    mascara = pd.Series([False] * len(df_visible))
                    for col in df_visible.columns:
                        mascara = mascara | df_visible[col].astype(str).str.contains(termino, case=False, na=False)
                    
                    resultados = df_visible[mascara]
                    print(f"\n📊 Resultados de búsqueda ({len(resultados)} registros):")
                    if len(resultados) > 0:
                        print(resultados.to_string())
                    else:
                        print("❌ No se encontraron resultados")
                else:
                    print("❌ Término de búsqueda vacío")
                    
            elif opcion == "12":
                print(f"\n🧹 LIMPIAR TABLA '{nombre_tabla.upper()}'")
                print("="*40)
                print("Esta operación:")
                print("  - Eliminará registros duplicados")
                print("  - Normalizará y reindexará los IDs")
                print("  - Convertirá columnas ID a numéricas")
                
                confirmar = input("¿Continuar? (s/n): ").strip().lower()
                if confirmar == 's':
                    df_original = df_trabajo.copy()
                    df_trabajo = limpiar_tabla_manual(df_trabajo, nombre_tabla)
                    df_visible = obtener_vista_usuario(df_trabajo)
                    
                    cambios_count = len(df_original) - len(df_trabajo)
                    if cambios_count > 0:
                        cambios.append(f"Limpieza: eliminados {cambios_count} registros duplicados")
                    
                    print("✅ Tabla limpiada exitosamente")
                    guardar_y_sincronizar(df_trabajo, nombre_tabla)
                else:
                    print("❌ Limpieza cancelada")
                    
            elif opcion == "13":
                ruta_json = os.path.join(BASE_AUXILIAR, f"{nombre_tabla}.json")
                df_visible.to_json(ruta_json, orient='records', indent=2, force_ascii=False)
                print(f"✅ Tabla exportada a: {ruta_json}")
                
            elif opcion == "14":
                ruta_excel = os.path.join(BASE_AUXILIAR, f"{nombre_tabla}.xlsx")
                df_visible.to_excel(ruta_excel, index=False)
                print(f"✅ Tabla exportada a: {ruta_excel}")
                
            elif opcion == "15":
                if cambios:
                    print(f"\n💾 Guardando {len(cambios)} cambios...")
                    registrar_cambio(nombre_tabla, cambios)
                return df_trabajo, cambios, False
                
            elif opcion == "16":
                if cambios:
                    confirmar = input("⚠️  Tienes cambios sin guardar. ¿Seguro que quieres volver? (s/n): ").strip().lower()
                    if confirmar != 's':
                        continue
                return df_original, [], False
                
            elif opcion == "17":
                if cambios:
                    confirmar = input("⚠️  Tienes cambios sin guardar. ¿Seguro que quieres salir? (s/n): ").strip().lower()
                    if confirmar != 's':
                        continue
                print("👋 ¡Hasta luego!")
                return df_original, [], True
                
            else:
                print("❌ Opción no válida. Por favor, selecciona 1-17.")
                
        except KeyboardInterrupt:
            print("\n\n⚠️  Operación interrumpida por el usuario")
            confirmar = input("¿Quieres salir? (s/n): ").strip().lower()
            if confirmar == 's':
                return df_original, [], True
        except Exception as e:
            print(f"❌ Error: {e}")

# =============================================================================
# FUNCIÓN PRINCIPAL
# =============================================================================

def main():
    """Función principal del programa"""
    global tablas_referencia
    
    print("🚀 INICIANDO SISTEMA DE GESTIÓN DE DATOS")
    print("="*50)
    
    # Cargar tablas desde la carpeta auxiliar
    print("\n📂 Cargando tablas desde carpeta auxiliar...")
    tablas = cargar_tablas_desde_auxiliar()
    
    if not tablas:
        print("❌ No se encontraron tablas en la carpeta auxiliar")
        return
    
    print(f"✅ Tablas cargadas: {list(tablas.keys())}")
    
    # Actualizar referencia global
    tablas_referencia = tablas
    
    # Probar conexión a PostgreSQL
    print("\n🔌 Probando conexión a PostgreSQL...")
    test_conexion_postgres()
    
    # Menú principal
    while True:
        print("\n" + "="*50)
        print("🏠 MENÚ PRINCIPAL")
        print("="*50)
        print("📊 Tablas disponibles:")
        
        for i, nombre_tabla in enumerate(tablas.keys(), 1):
            df = tablas[nombre_tabla]
            print(f"  {i}. {nombre_tabla} ({len(df)} registros)")
        
        print("\n🔧 Herramientas:")
        print(f"  {len(tablas) + 1}. Sincronizar todas las tablas con PostgreSQL")
        print(f"  {len(tablas) + 2}. Probar conexión PostgreSQL")
        print(f"  {len(tablas) + 3}. Salir")
        
        print("\n" + "="*50)
        
        try:
            opcion = input("Selecciona una opción: ").strip()
            
            if opcion.isdigit():
                opcion_num = int(opcion)
                
                if 1 <= opcion_num <= len(tablas):
                    # Editar tabla específica
                    nombre_tabla = list(tablas.keys())[opcion_num - 1]
                    df_modificado, cambios, salir = menu_interactivo(
                        tablas[nombre_tabla], nombre_tabla, tablas
                    )
                    
                    # Actualizar la tabla en el diccionario
                    tablas[nombre_tabla] = df_modificado
                    tablas_referencia = tablas  # Actualizar referencia global
                    
                    if salir:
                        print("👋 ¡Hasta luego!")
                        break
                        
                elif opcion_num == len(tablas) + 1:
                    # Sincronizar con PostgreSQL
                    sincronizar_postgresql(tablas)
                    
                elif opcion_num == len(tablas) + 2:
                    # Probar conexión
                    test_conexion_postgres()
                    
                elif opcion_num == len(tablas) + 3:
                    print("👋 ¡Hasta luego!")
                    break
                    
                else:
                    print("❌ Opción no válida")
            else:
                print("❌ Ingresa un número válido")
                
        except KeyboardInterrupt:
            print("\n\n⚠️  Programa interrumpido por el usuario")
            confirmar = input("¿Quieres salir? (s/n): ").strip().lower()
            if confirmar == 's':
                break
        except Exception as e:
            print(f"❌ Error: {e}")
    
    # Cerrar conexión a PostgreSQL al salir
    cerrar_conexion_postgres()

if __name__ == "__main__":
    main()