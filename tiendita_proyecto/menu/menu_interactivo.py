import pandas as pd
from utiles.manejadores_archivos import guardar_tabla_individual  
from utiles.utilidades_logs import registrar_cambio

"""
utiles es el módulo del proyecto con herramientas auxiliares.
    manejadores_archivos.py maneja archivos (ene este caso, guardar tablas).
    utilidades_logs.py registra cambios realizados.
        Las funciones guardar_tabla_individual y registrar_cambio son del proyecto, no de Python ni librerías externas.
"""

def menu_interactivo(df_original, nombre):
    """Permite al usuario aplicar transformaciones de forma interactiva."""
    df = df_original.copy()
    cambios = []
    print(f"\n🧩 Modificando tabla: {nombre}")
    print(f"Columnas disponibles: {list(df.columns)}")
    print(f"Filas actuales: {len(df)}")

    while True:
        print("\nOpciones disponibles:")
        print("1️⃣ Ver las primeras filas")
        print("2️⃣ Agregar una nueva columna calculada")
        print("3️⃣ Modificar una columna existente")
        print("4️⃣ Eliminar una columna")
        print("5️⃣ Agregar un nuevo registro/fila")
        print("6️⃣ Eliminar un registro/fila")  # NUEVA OPCIÓN
        print("7️⃣ Finalizar y guardar cambios")
        print("8️⃣ Volver sin guardar cambios")
        print("9️⃣ Salir del programa")

        opcion = input("Seleccioná una opción (1-9): ").strip()

        if opcion == "1":
            print(f"\n📊 Primeras 5 filas de '{nombre}':")
            print(df.head())
            print(f"... Total de filas: {len(df)}")

        elif opcion == "2":
            nueva_col = input("Nombre de la nueva columna: ")
            expresion = input("Ingresá una expresión (ej: df['precio'] * 1.21): ")
            try:
                df[nueva_col] = eval(expresion)
                cambios.append(f"Agregada columna '{nueva_col}' con: {expresion}")
                print(f"✅ Columna '{nueva_col}' creada.")
            except Exception as e:
                print(f"❌ Error: {e}")

        elif opcion == "3":
            col = input("Nombre de la columna a modificar: ")
            if col in df.columns:
                expresion = input(f"Expresión para modificar '{col}' (ej: df['{col}'] * 2): ")
                try:
                    df[col] = eval(expresion)
                    cambios.append(f"Modificada columna '{col}' con: {expresion}")
                    print(f"✅ Columna '{col}' modificada.")
                except Exception as e:
                    print(f"❌ Error: {e}")
            else:
                print("❌ Columna no encontrada.")

        elif opcion == "4":
            col = input("Nombre de la columna a eliminar: ")
            if col in df.columns:
                df.drop(columns=[col], inplace=True)
                cambios.append(f"Eliminada columna '{col}'")
                print(f"✅ Columna '{col}' eliminada.")
            else:
                print("❌ Columna no encontrada.")

        elif opcion == "5":
            print("📥 Agregando nuevo registro:")
            nueva_fila = {}
            for col in df.columns:
                valor = input(f"Valor para '{col}': ")
                nueva_fila[col] = valor
            
            # Convertir a tipos de datos apropiados
            for col, valor in nueva_fila.items():
                try:
                    if df[col].dtype == 'int64':
                        nueva_fila[col] = int(valor)
                    elif df[col].dtype == 'float64':
                        nueva_fila[col] = float(valor)
                except:
                    pass  # Mantener como string si no se puede convertir
            
            df = pd.concat([df, pd.DataFrame([nueva_fila])], ignore_index=True)
            cambios.append(f"Agregado registro: {nueva_fila}")
            print("✅ Registro agregado.")

        elif opcion == "6":  
            print("🗑️ Eliminar registro/fila")
            print(f"📊 Tabla actual tiene {len(df)} filas")
            
            # Mostrar filas con índices
            print("\n📋 Filas disponibles (primeras 10):")
            print(df.head(10).to_string())
            
            try:
                indice = int(input(f"\nIngresá el número de índice de la fila a eliminar (0-{len(df)-1}): "))
                
                if 0 <= indice < len(df):
                    # Mostrar qué se va a eliminar
                    fila_a_eliminar = df.iloc[indice]
                    print(f"\n⚠️  Se eliminará esta fila:")
                    print(fila_a_eliminar.to_string())
                    
                    confirmar = input("\n¿Confirmar eliminación? (si/no): ").lower().strip()
                    
                    if confirmar == "si":
                        df = df.drop(indice).reset_index(drop=True)
                        cambios.append(f"Eliminada fila con índice {indice}: {fila_a_eliminar.to_dict()}")
                        print("✅ Fila eliminada correctamente.")
                    else:
                        print("❌ Eliminación cancelada.")
                else:
                    print(f"❌ Índice fuera de rango. Debe estar entre 0 y {len(df)-1}")
                    
            except ValueError:
                print("❌ Error: Debes ingresar un número válido.")
            except Exception as e:
                print(f"❌ Error inesperado: {e}")

        elif opcion == "7":  # Antes era "6" - finalizar y guardar
            break

        elif opcion == "8":  # Antes era "7" - volver sin guardar
            print("🔙 Descartando cambios y volviendo al menú...")
            return None, [], True

        elif opcion == "9":  # Antes era "8" - salir del programa
            print("👋 Saliendo del programa...")
            exit()
        else:
            print("❌ Opción inválida.")

    # mostrar cambios
    if cambios:
        print(f"\n📋 RESUMEN de cambios en '{nombre}':")
        for i, cambio in enumerate(cambios, 1):
            print(f"   {i}. {cambio}")
        
        confirmar = input("\n¿Aplicar estos cambios? (si/no): ").lower().strip()
        if confirmar == "si":
            # guardar en auxiliar
            guardar_tabla_individual(df, nombre)
            registrar_cambio(nombre, cambios)
            print("✅ Cambios guardados en archivo auxiliar.")
            return df, cambios, False
        else:
            print("❌ Cambios descartados.")
            return None, [], True
    else:
        print("ℹ️ No se realizaron cambios.")
        return None, [], True