
## Sistema de Modificación de Tablas

- Desarrollado como proyecto educativo de práctica en la tecnicatura de Ciencia de Datos e Inteligencia Artificial
Proyecto educativo interactivo con la finalidad de modificar y gestionar datos tabulares con sincronización automática a PostgreSQL.  
Una herramienta que permite transformar, editar y administrar tablas CSV mediante una interfaz interactiva, manteniendo sincronización bidireccional con la base de datos PostgreSQL.


## Características

- **Interfaz interactiva** para modificar tablas CSV  
- **Sincronización automática** con PostgreSQL  
- **Estructura modular** y mantenible  
- **Registro de cambios** con timestamp  
- **Gestión segura** de credenciales  

## Objetivos Principales

- Desarrollar un sistema interactivo para modificación de datos tabulares  
- Implementar sincronización bidireccional con PostgreSQL  
- Crear una arquitectura modular y mantenible  
- Garantizar la seguridad de credenciales sensibles  


## Tecnologías Utilizadas

**Lenguajes y Frameworks**
```bash
Python 3.11        - Lenguaje principal del sistema
Pandas 1.5+        - Manipulación y transformación de datos
PostgreSQL 13+     - Sistema de base de datos
Psycopg2           - Conector Python-PostgreSQL
Python-dotenv      - Gestión de configuraciones seguras
```

**Estándares y Protocolos**

```bash
CSV                - Formato de intercambio de datos
SQL                - Lenguaje de consultas para PostgreSQL
Environment Variables - Manejo de configuraciones
OS: Windows / Linux compatible
```


## Instalación

1. **Clonar el repositorio**

   ```bash
   git clone [url-del-repositorio]
   cd tiendita_proyecto
   ```

2. **Instalar dependencias**

   ```bash
   pip install -r requirements.txt
   ```

3. **Configurar variables de entorno**

   ```bash
   # Copiar el archivo de ejemplo
   cp config/.env.example .env
   ```

   Luego, editar `.env` con tus credenciales:

   ```env
   DB_USER=postgres
   DB_PASSWORD=tu_contraseña
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=tiendita_auxiliar
   ```

4. **Preparar las carpetas de datos**

   * Asegurarse de que existan `tiendita_csv/` y `tiendita_auxiliar_csv/`
   * Colocar los archivos CSV en `tiendita_auxiliar_csv/`


## Uso

1. **Ejecutar el sistema**

   ```bash
   python main.py
   ```

2. **Flujo de trabajo**

   * Seleccionar una tabla para modificar
   * Aplicar transformaciones interactivas sobre los datos
   * Guardar los cambios en la carpeta auxiliar (`tiendita_auxiliar_csv/`)
   * Sincronizar los cambios con PostgreSQL cuando esté listo

3. **Operaciones disponibles**

   * Agregar, modificar o eliminar columnas
   * Agregar nuevos registros
   * Aplicar transformaciones con expresiones Python
   * Sincronizar los cambios con PostgreSQL


## Tablas Soportadas

* `provincias`, `localidades`, `condicion_iva`
* `proveedores`, `sucursales`, `clientes`
* `productos`, `factura_enunciado`, `factura_detalle`
* `ventas`, `recursos`, `rubros`


## Configuración de PostgreSQL

1. Crear la base de datos:

   ```sql
   CREATE DATABASE tiendita_auxiliar;
   ```

2. Las tablas se crean automáticamente durante la primera sincronización.


## Registro de Cambios

Todos los cambios realizados por el usuario se registran en:

* `tiendita_auxiliar_csv/log_cambios.txt`


## Configuración del entorno

Copiá el archivo `.env.example`, renombralo como `.env` y completá tus datos de conexión a PostgreSQL antes de ejecutar el proyecto.


## Dependencias

* **pandas** — Manipulación de datos
* **psycopg2-binary** — Conexión a PostgreSQL
* **python-dotenv** — Manejo de variables de entorno


## Problemas Comunes

**Problema:** Error de conexión a PostgreSQL
**Solución:** Verificar credenciales en `.env` y que el servicio esté corriendo.

**Problema:** No se aplican los cambios

**Solución:** Asegurarse de confirmar los cambios en el menú interactivo.
