import os
import sys

# cambiar al directorio del script
# esto asegura que el script siempre trabaje desde su propia carpeta, independientemente de donde se ejecute.
os.chdir(os.path.dirname(os.path.abspath(__file__)))

from utiles.manejadores_archivos import cargar_tablas_desde_auxiliar
from servicios.postgres_service import conectar_postgres, actualizar_tabla_postgres
from menu.menu_interactivo import menu_interactivo

def main():
    print("SISTEMA DE MODIFICACIÓN DE TABLAS")
    print("\n" + "="*50)
    
    # CARGAR DESDE AUXILIAR
    tablas = cargar_tablas_desde_auxiliar()
    print(f"📂 Tablas cargadas desde AUXILIAR: {list(tablas.keys())}")

    tablas_modificadas = {}

    while True:
        print("\n" + "-"*50)
        print("📋 MENÚ PRINCIPAL - Tablas disponibles:")
        print("-"*50)
        
        nombres_tablas = list(tablas.keys())
        for i, nombre in enumerate(nombres_tablas, 1):
            print(f"{i}. {nombre}")

        print(f"{len(nombres_tablas) + 1}. 📤 Sincronizar con PostgreSQL")
        print(f"{len(nombres_tablas) + 2}. ❌ Salir del programa")

        try:
            opcion = input(f"\nSeleccioná opción (1-{len(nombres_tablas) + 2}): ").strip()
            opcion = int(opcion)

            if opcion == len(nombres_tablas) + 1:
                # sincronizar con PostgreSQL
                print("\n🔄 Sincronizando con PostgreSQL...")
                conn = conectar_postgres("tiendita_auxiliar")
                if conn:
                    orden_tablas = ['provincias', 'localidades', 'condicion_iva', 'proveedores', 
                                  'sucursales', 'clientes', 'productos', 'factura_enunciado', 
                                  'factura_detalle', 'ventas', 'recursos']
                    
                    for nombre in orden_tablas:
                        if nombre in tablas:
                            success = actualizar_tabla_postgres(conn, tablas[nombre], nombre)
                            if not success:
                                print(f"❌ Falló la sincronización de {nombre}")
                    conn.close()
                    print("✅ Sincronización completada.")
                break

            elif opcion == len(nombres_tablas) + 2:
                print("👋 Saliendo del programa...")
                break

            elif 1 <= opcion <= len(nombres_tablas):
                nombre_tabla = nombres_tablas[opcion - 1]
                df_mod, cambios, volver = menu_interactivo(tablas[nombre_tabla], nombre_tabla)
                
                if not volver and df_mod is not None:
                    tablas[nombre_tabla] = df_mod
                    tablas_modificadas[nombre_tabla] = True
                    print(f"✅ Tabla '{nombre_tabla}' actualizada en memoria.")

            else:
                print("❌ Opción inválida.")

        except (ValueError, IndexError):
            print("❌ Opción inválida.")
        except Exception as e:
            print(f"❌ Error inesperado: {e}")

    print("\n🎯 Programa finalizado.")

if __name__ == "__main__":
    main()