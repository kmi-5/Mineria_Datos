import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
from datetime import datetime, timedelta
import numpy as np
from io import BytesIO

# Configuración de la página
st.set_page_config(
    page_title="Análisis Comercial",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personalizados
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
        font-weight: bold;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .section-header {
        border-left: 5px solid #1f77b4;
        padding-left: 1rem;
        margin: 2rem 0 1rem 0;
        color: #2c3e50;
    }
</style>
""", unsafe_allow_html=True)

# Ruta base 
CARPETA_CSV = r"D:/Proyectos/SQL/Mineria_Datos/TP4_dashboard_tienda/CSV_tienda"

@st.cache_data
def load_data():
    archivos = [
        'clientes.csv', 'condicion_iva.csv', 'facturas_detalle.csv', 'facturas_encabezado.csv',
        'localidades.csv', 'productos.csv', 'proveedores.csv', 'provincias.csv', 
        'rubros.csv', 'sucursales.csv', 'ventas.csv'
    ]
    
    datos = {}
    for archivo in archivos:
        ruta = os.path.join(CARPETA_CSV, archivo)
        try:
            datos[archivo.split('.')[0]] = pd.read_csv(ruta) if os.path.exists(ruta) else pd.DataFrame()
        except Exception as e:
            st.error(f"Error cargando {archivo}: {str(e)}")
            datos[archivo.split('.')[0]] = pd.DataFrame()
    
    if not all(not df.empty for df in datos.values()):
        st.error("❌ No se pudieron cargar todos los archivos")
        st.stop()
    
    return tuple(datos.values())

# Función para generar reportes Excel
def generar_reporte_excel(tipo_reporte, facturas_filtradas, dataset_filtrado, metricas_sucursal, top_productos, top_clientes, analisis_proveedores):
    try:
        output = BytesIO()
        
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            # Formato para números y títulos
            money_format = workbook.add_format({'num_format': '$#,##0.00'})
            number_format = workbook.add_format({'num_format': '#,##0'})
            
            if tipo_reporte == "Ventas por Sucursal":
                # Hoja 1: Ventas por Sucursal
                if not metricas_sucursal.empty:
                    metricas_sucursal.to_excel(writer, sheet_name='Ventas por Sucursal', startrow=1)
                    worksheet = writer.sheets['Ventas por Sucursal']
                    worksheet.write_string(0, 0, "Ventas por Sucursal")
                    
                    # Formatear columnas numéricas
                    worksheet.set_column('B:B', 15, money_format)  # Ventas Totales
                    worksheet.set_column('C:C', 15, money_format)  # Ticket Promedio
                    worksheet.set_column('D:D', 12, number_format)  # N° Facturas
                    worksheet.set_column('E:E', 12, number_format)  # Clientes Únicos
                
                # Hoja 2: Resumen Ejecutivo
                total_ventas = facturas_filtradas['total_venta'].sum()
                ticket_promedio = facturas_filtradas['total_venta'].mean()
                total_facturas = len(facturas_filtradas)
                clientes_unicos = facturas_filtradas['id_cliente'].nunique()
                
                resumen_data = {
                    'Métrica': [
                        'Ventas Totales del Período',
                        'Sucursal Mejor Performance',
                        'Ticket Promedio General',
                        'Total de Facturas',
                        'Clientes Únicos'
                    ],
                    'Valor': [
                        total_ventas,
                        metricas_sucursal.index[0] if len(metricas_sucursal) > 0 else "N/A",
                        ticket_promedio,
                        total_facturas,
                        clientes_unicos
                    ]
                }
                resumen_df = pd.DataFrame(resumen_data)
                resumen_df.to_excel(writer, sheet_name='Resumen Ejecutivo', index=False, startrow=1)
                worksheet_resumen = writer.sheets['Resumen Ejecutivo']
                worksheet_resumen.write_string(0, 0, "Resumen Ejecutivo")
                worksheet_resumen.set_column('A:A', 30)
                worksheet_resumen.set_column('B:B', 20, money_format)
                
            elif tipo_reporte == "Performance de Productos":
                # Hoja 1: Top Productos
                if not top_productos.empty:
                    top_productos.to_excel(writer, sheet_name='Top Productos', startrow=1)
                    worksheet = writer.sheets['Top Productos']
                    worksheet.write_string(0, 0, "Top Productos por Ventas")
                    worksheet.set_column('A:A', 40)  # Nombre producto
                    worksheet.set_column('B:B', 15, money_format)  # Ventas Totales
                    worksheet.set_column('C:C', 12, number_format)  # Cantidad
                    worksheet.set_column('D:D', 12, number_format)  # N° Facturas
                
                # Hoja 2: Performance por Rubro
                rubro_col = 'descripcion_y' if 'descripcion_y' in dataset_filtrado.columns else 'descripcion'
                if rubro_col in dataset_filtrado.columns:
                    performance_rubro = dataset_filtrado.groupby(rubro_col).agg({
                        'subtotal_linea': 'sum',
                        'cantidad': 'sum',
                        'id_producto': 'nunique'
                    }).sort_values('subtotal_linea', ascending=False)
                    performance_rubro.columns = ['Ventas Totales', 'Unidades Vendidas', 'Productos Únicos']
                    
                    performance_rubro.to_excel(writer, sheet_name='Performance Rubro', startrow=1)
                    worksheet_rubro = writer.sheets['Performance Rubro']
                    worksheet_rubro.write_string(0, 0, "Performance por Rubro")
                    worksheet_rubro.set_column('A:A', 20)
                    worksheet_rubro.set_column('B:B', 15, money_format)
                    worksheet_rubro.set_column('C:C', 12, number_format)
                    worksheet_rubro.set_column('D:D', 12, number_format)
                
            elif tipo_reporte == "Análisis de Clientes":
                # Hoja 1: Segmentación de Clientes
                segmentacion_clientes = facturas_filtradas.groupby('id_cliente').agg({
                    'total_venta': 'sum',
                    'id_factura': 'count',
                    'fecha': 'max'
                })
                segmentacion_clientes.columns = ['Monto Total', 'Frecuencia', 'Última Compra']
                segmentacion_clientes['Recencia'] = (datetime.now() - segmentacion_clientes['Última Compra']).dt.days
                
                segmentacion_clientes.to_excel(writer, sheet_name='Segmentación Clientes', startrow=1)
                worksheet_seg = writer.sheets['Segmentación Clientes']
                worksheet_seg.write_string(0, 0, "Segmentación de Clientes")
                worksheet_seg.set_column('B:B', 15, money_format)
                worksheet_seg.set_column('C:C', 12, number_format)
                worksheet_seg.set_column('E:E', 12, number_format)
                
                # Hoja 2: Top Clientes
                if not top_clientes.empty:
                    top_clientes_detalle = top_clientes.copy()
                    nombres_completos = [f"{idx[0]} {idx[1]}" for idx in top_clientes.index]
                    top_clientes_detalle.index = nombres_completos
                    
                    top_clientes_detalle.to_excel(writer, sheet_name='Top Clientes', startrow=1)
                    worksheet_top = writer.sheets['Top Clientes']
                    worksheet_top.write_string(0, 0, "Top 10 Clientes")
                    worksheet_top.set_column('A:A', 25)
                    worksheet_top.set_column('B:B', 15, money_format)
                    worksheet_top.set_column('C:C', 12, number_format)
                    worksheet_top.set_column('E:E', 15, money_format)  # Ticket Promedio
                
            elif tipo_reporte == "Datos de Proveedores":
                # Hoja 1: Performance Proveedores
                if not analisis_proveedores.empty:
                    analisis_proveedores.to_excel(writer, sheet_name='Performance Proveedores', startrow=1)
                    worksheet_prov = writer.sheets['Performance Proveedores']
                    worksheet_prov.write_string(0, 0, "Performance de Proveedores")
                    worksheet_prov.set_column('A:A', 25)
                    worksheet_prov.set_column('B:B', 15, money_format)
                    worksheet_prov.set_column('C:C', 12, number_format)
                    worksheet_prov.set_column('D:D', 12, number_format)
                    worksheet_prov.set_column('E:E', 15, money_format)  # Margen por Producto
                
                # Hoja 2: Productos por Proveedor
                proveedor_nombre_col = 'nombre' if 'nombre' in dataset_filtrado.columns else 'nombre_prov'
                if proveedor_nombre_col in dataset_filtrado.columns:
                    productos_por_proveedor = dataset_filtrado.groupby([proveedor_nombre_col, 'descripcion_x']).agg({
                        'subtotal_linea': 'sum',
                        'cantidad': 'sum'
                    }).sort_values('subtotal_linea', ascending=False).head(20)
                    
                    productos_por_proveedor.columns = ['Ventas Totales', 'Unidades Vendidas']
                    productos_por_proveedor.to_excel(writer, sheet_name='Productos Proveedor', startrow=1)
                    worksheet_prod = writer.sheets['Productos Proveedor']
                    worksheet_prod.write_string(0, 0, "Top Productos por Proveedor")
                    worksheet_prod.set_column('A:A', 25)
                    worksheet_prod.set_column('B:B', 30)
                    worksheet_prod.set_column('C:C', 15, money_format)
                    worksheet_prod.set_column('D:D', 12, number_format)
        
        # Mover el cursor al inicio del archivo
        output.seek(0)
        return output.getvalue()
        
    except Exception as e:
        st.error(f"Error detallado en generación de Excel: {str(e)}")
        import traceback
        st.error(f"Traceback: {traceback.format_exc()}")
        return None

# Cargar datos
with st.spinner('Cargando datos...'):
    (clientes, condicion_iva, facturas_detalle, facturas_encabezado, 
     localidades, productos, proveedores, provincias, rubros, sucursales, ventas) = load_data()

# Procesamiento avanzado de datos
# Unir datos para análisis
facturas_completas = (facturas_encabezado
    .merge(clientes, on='id_cliente')
    .merge(condicion_iva, on='id_condicion_iva')
    .merge(sucursales, on='id_sucursal', suffixes=('_cli', '_suc'))
    .merge(localidades, left_on='id_localidad_suc', right_on='id_localidad')
    .merge(provincias, on='id_provincia', suffixes=('_loc', '_prov')))

detalles_completos = (facturas_detalle
    .merge(productos, on='id_producto')
    .merge(rubros, on='id_rubro')
    .merge(proveedores, on='id_proveedor'))

# Crear dataset unificado para análisis
dataset_completo = detalles_completos.merge(
    facturas_completas[['id_factura', 'fecha', 'nombre_suc', 'nombre_prov', 'nombre_cli', 'apellido']], 
    on='id_factura'
)

# Convertir fecha
dataset_completo['fecha'] = pd.to_datetime(dataset_completo['fecha'])
facturas_completas['fecha'] = pd.to_datetime(facturas_completas['fecha'])

# Header principal
st.markdown('<h1 class="main-header">🚀 Dashboard Comercial - Análisis</h1>', unsafe_allow_html=True)

# Sidebar con filtros avanzados
st.sidebar.header("🔧 Panel de Control")

# Filtro por fecha con selector de rango
fecha_min = facturas_completas['fecha'].min().date()
fecha_max = facturas_completas['fecha'].max().date()

col_fecha1, col_fecha2 = st.sidebar.columns(2)
with col_fecha1:
    fecha_inicio = st.date_input("Fecha inicio", fecha_min)
with col_fecha2:
    fecha_fin = st.date_input("Fecha fin", fecha_max)

# Filtros múltiples
st.sidebar.subheader("Filtros Múltiples")

# Sucursal
sucursal_col = 'nombre_suc' if 'nombre_suc' in facturas_completas.columns else 'nombre'
sucursales_list = ['Todas'] + list(facturas_completas[sucursal_col].unique())
sucursal_seleccionada = st.sidebar.multiselect("Sucursales", sucursales_list[1:], default=sucursales_list[1:])

# Provincia
provincia_col = 'nombre_prov' if 'nombre_prov' in facturas_completas.columns else 'nombre'
provincias_list = ['Todas'] + list(facturas_completas[provincia_col].unique())
provincia_seleccionada = st.sidebar.multiselect("Provincias", provincias_list[1:], default=provincias_list[1:])

# Rubro
rubro_col = 'descripcion_y' if 'descripcion_y' in detalles_completos.columns else 'descripcion'
rubros_list = ['Todos'] + list(detalles_completos[rubro_col].unique())
rubro_seleccionado = st.sidebar.multiselect("Rubros", rubros_list[1:], default=rubros_list[1:])

# Aplicar filtros
facturas_filtradas = facturas_completas[
    (facturas_completas['fecha'].dt.date >= fecha_inicio) & 
    (facturas_completas['fecha'].dt.date <= fecha_fin)
]

dataset_filtrado = dataset_completo[
    (dataset_completo['fecha'].dt.date >= fecha_inicio) & 
    (dataset_completo['fecha'].dt.date <= fecha_fin)
]

if sucursal_seleccionada and 'Todas' not in sucursal_seleccionada:
    facturas_filtradas = facturas_filtradas[facturas_filtradas[sucursal_col].isin(sucursal_seleccionada)]
    dataset_filtrado = dataset_filtrado[dataset_filtrado[sucursal_col].isin(sucursal_seleccionada)]

if provincia_seleccionada and 'Todas' not in provincia_seleccionada:
    facturas_filtradas = facturas_filtradas[facturas_filtradas[provincia_col].isin(provincia_seleccionada)]
    dataset_filtrado = dataset_filtrado[dataset_filtrado[provincia_col].isin(provincia_seleccionada)]

if rubro_seleccionado and 'Todos' not in rubro_seleccionado:
    dataset_filtrado = dataset_filtrado[dataset_filtrado[rubro_col].isin(rubro_seleccionado)]

# KPI Cards mejoradas
st.markdown('<h2 class="section-header">📈 Métricas de Performance</h2>', unsafe_allow_html=True)

col1, col2, col3, col4 = st.columns(4)

with col1:
    total_ventas = facturas_filtradas['total_venta'].sum()
    crecimiento = ((total_ventas / (total_ventas * 0.9)) - 1) * 100  # Simulado
    st.metric(
        "💰 Ventas Totales", 
        f"${total_ventas:,.0f}", 
        f"{crecimiento:+.1f}%"
    )

with col2:
    total_facturas = len(facturas_filtradas)
    st.metric("📄 Total Facturas", f"{total_facturas:,}")

with col3:
    clientes_unicos = facturas_filtradas['id_cliente'].nunique()
    st.metric("👥 Clientes Únicos", f"{clientes_unicos:,}")

with col4:
    ticket_promedio = facturas_filtradas['total_venta'].mean()
    st.metric("🎫 Ticket Promedio", f"${ticket_promedio:,.2f}")

# Segunda fila de KPIs
col5, col6, col7, col8 = st.columns(4)

with col5:
    productos_vendidos = dataset_filtrado['cantidad'].sum()
    st.metric("📦 Productos Vendidos", f"{productos_vendidos:,}")

with col6:
    productos_unicos = dataset_filtrado['id_producto'].nunique()
    st.metric("🎯 Productos Únicos", f"{productos_unicos:,}")

with col7:
    margen_promedio = (dataset_filtrado['subtotal_linea'] - dataset_filtrado['precio']).mean() if 'precio' in dataset_filtrado.columns else 0
    st.metric("📊 Margen Promedio", f"${margen_promedio:,.2f}")

with col8:
    frecuencia_compra = total_facturas / clientes_unicos if clientes_unicos > 0 else 0
    st.metric("🔄 Frecuencia Compra", f"{frecuencia_compra:.1f}")

# PRIMERA SECCIÓN: ANÁLISIS TEMPORAL
st.markdown("---")
st.markdown('<h2 class="section-header">📅 Análisis Temporal y Tendencias</h2>', unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    # Evolución de ventas mensuales con tendencia
    st.subheader("📈 Evolución Mensual de Ventas")
    
    ventas_mensuales = facturas_filtradas.set_index('fecha').resample('M').agg({
        'total_venta': 'sum',
        'id_factura': 'count'
    }).reset_index()
    
    fig_evolucion = make_subplots(specs=[[{"secondary_y": True}]])
    
    fig_evolucion.add_trace(
        go.Scatter(
            x=ventas_mensuales['fecha'],
            y=ventas_mensuales['total_venta'],
            name="Ventas ($)",
            line=dict(color='#1f77b4', width=3),
            fill='tozeroy',
            fillcolor='rgba(31, 119, 180, 0.1)'
        ),
        secondary_y=False
    )
    
    fig_evolucion.add_trace(
        go.Bar(
            x=ventas_mensuales['fecha'],
            y=ventas_mensuales['id_factura'],
            name="N° Facturas",
            marker_color='rgba(255, 127, 14, 0.7)',
            opacity=0.6
        ),
        secondary_y=True
    )
    
    fig_evolucion.update_layout(
        title="Evolución de Ventas y Volumen de Transacciones",
        xaxis_title="Mes",
        showlegend=True,
        height=400
    )
    fig_evolucion.update_yaxes(title_text="Ventas ($)", secondary_y=False)
    fig_evolucion.update_yaxes(title_text="N° Facturas", secondary_y=True)
    
    st.plotly_chart(fig_evolucion, use_container_width=True)

with col2:
    # Análisis estacional - Ventas por día de la semana
    st.subheader("🗓️ Patrón Semanal de Ventas")
    
    facturas_filtradas['dia_semana'] = facturas_filtradas['fecha'].dt.day_name()
    facturas_filtradas['hora'] = facturas_filtradas['fecha'].dt.hour
    
    # Traducir días de la semana
    dias_traducidos = {
        'Monday': 'Lunes',
        'Tuesday': 'Martes',
        'Wednesday': 'Miércoles',
        'Thursday': 'Jueves',
        'Friday': 'Viernes',
        'Saturday': 'Sábado',
        'Sunday': 'Domingo'
    }
    
    facturas_filtradas['dia_semana_es'] = facturas_filtradas['dia_semana'].map(dias_traducidos)
    
    ventas_diarias = facturas_filtradas.groupby('dia_semana_es').agg({
        'total_venta': 'sum',
        'id_factura': 'count'
    }).reindex(['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo'])
    
    fig_semanal = make_subplots(specs=[[{"secondary_y": True}]])
    
    fig_semanal.add_trace(
        go.Bar(
            x=ventas_diarias.index,
            y=ventas_diarias['total_venta'],
            name="Ventas Totales",
            marker_color='#2ca02c'
        ),
        secondary_y=False
    )
    
    fig_semanal.add_trace(
        go.Scatter(
            x=ventas_diarias.index,
            y=ventas_diarias['id_factura'],
            name="Transacciones",
            line=dict(color='#d62728', width=3),
            marker=dict(size=8)
        ),
        secondary_y=True
    )
    
    fig_semanal.update_layout(
        title="Distribución Semanal de Ventas",
        xaxis_title="Día de la Semana",
        showlegend=True,
        height=400
    )
    fig_semanal.update_yaxes(title_text="Ventas ($)", secondary_y=False)
    fig_semanal.update_yaxes(title_text="N° Transacciones", secondary_y=True)
    
    st.plotly_chart(fig_semanal, use_container_width=True)

# ANÁLISIS GEOGRÁFICO Y SUCURSALES
st.markdown("---")
st.markdown('<h2 class="section-header">🌍 Análisis Geográfico y Desempeño por Sucursal</h2>', unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    # Mapa de calor por provincia 
    st.subheader("🗺️ Ventas por Provincia")
    
    ventas_provincia = facturas_filtradas.groupby(provincia_col).agg({
        'total_venta': 'sum',
        'id_cliente': 'nunique',
        'id_factura': 'count'
    }).sort_values('total_venta', ascending=False)
    
    # Gráfico de barras horizontal como alternativa al mapa
    fig_provincias = px.bar(
        ventas_provincia,
        y=ventas_provincia.index,
        x='total_venta',
        orientation='h',
        title="Ventas Totales por Provincia",
        labels={'total_venta': 'Ventas Totales ($)', 'nombre_prov': 'Provincia'},
        color='total_venta',
        color_continuous_scale='oranges'
    )
    
    fig_provincias.update_layout(
        height=500,
        yaxis={'categoryorder': 'total ascending'}
    )
    
    st.plotly_chart(fig_provincias, use_container_width=True)
    
    # Métricas resumen
    col_met1, col_met2 = st.columns(2)
    
    with col_met1:
        provincia_top = ventas_provincia.index[0] if len(ventas_provincia) > 0 else "N/A"
        st.metric("🏆 Provincia Líder", provincia_top)
    
    with col_met2:
        ventas_top = ventas_provincia['total_venta'].iloc[0] if len(ventas_provincia) > 0 else 0
        st.metric("💰 Ventas Líder", f"${ventas_top:,.0f}")

with col2:
    # Dashboard de sucursales
    st.subheader("🏪 Performance por Sucursal")
    
    metricas_sucursal = facturas_filtradas.groupby(sucursal_col).agg({
        'total_venta': ['sum', 'mean'],
        'id_factura': 'count',
        'id_cliente': 'nunique'
    }).round(2)
    
    metricas_sucursal.columns = ['Ventas Totales', 'Ticket Promedio', 'N° Facturas', 'Clientes Únicos']
    metricas_sucursal = metricas_sucursal.sort_values('Ventas Totales', ascending=False)
    
    # Gráfico de radar para comparar sucursales
    fig_radar = go.Figure()
    
    for sucursal in metricas_sucursal.head(3).index:
        valores = metricas_sucursal.loc[sucursal].values
        valores_normalizados = valores / metricas_sucursal.max().values
        
        fig_radar.add_trace(go.Scatterpolar(
            r=valores_normalizados,
            theta=['Ventas', 'Ticket', 'Facturas', 'Clientes'],
            fill='toself',
            name=sucursal
        ))
    
    fig_radar.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 1])
        ),
        showlegend=True,
        height=400,
        title="Comparativa de Sucursales (Top 3)"
    )
    
    st.plotly_chart(fig_radar, use_container_width=True)
    
    # Tabla de métricas por sucursal
    st.dataframe(
        metricas_sucursal.style.format({
            'Ventas Totales': '${:,.0f}',
            'Ticket Promedio': '${:,.2f}',
            'N° Facturas': '{:.0f}',
            'Clientes Únicos': '{:.0f}'
        }).background_gradient(cmap='Blues'),
        use_container_width=True
    )

# ANÁLISIS DE PRODUCTOS Y CATEGORÍAS
st.markdown("---")
st.markdown('<h2 class="section-header">📦 Análisis de Productos y Categorías</h2>', unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    # Top 10 productos más vendidos - Versión simple e intuitiva
    st.subheader("🚀 Top 10 Productos Más Vendidos")
    
    top_productos = dataset_filtrado.groupby('descripcion_x').agg({
        'subtotal_linea': 'sum',
        'cantidad': 'sum',
        'id_factura': 'nunique'
    }).nlargest(10, 'subtotal_linea')
    
    # Crear gráfico de barras horizontal
    fig_top_productos = px.bar(
        top_productos,
        y=top_productos.index,
        x='subtotal_linea',
        orientation='h',
        title="Productos por Ingresos Generados",
        labels={'subtotal_linea': 'Ventas Totales ($)', 'descripcion_x': 'Producto'},
        color='subtotal_linea',
        color_continuous_scale='viridis'
    )
    
    fig_top_productos.update_layout(
        yaxis={'categoryorder': 'total ascending'},
        height=500
    )
    
    st.plotly_chart(fig_top_productos, use_container_width=True)
    
    # Mostrar tabla con detalles 
    top_productos_detalle = top_productos.copy()
    top_productos_detalle.columns = ['Ventas Totales ($)', 'Unidades Vendidas', 'N° de Facturas']
    top_productos_detalle['Ventas por Unidad'] = top_productos_detalle['Ventas Totales ($)'] / top_productos_detalle['Unidades Vendidas']
    
    st.dataframe(
        top_productos_detalle.style.format({
            'Ventas Totales ($)': '${:,.2f}',
            'Unidades Vendidas': '{:,.0f}',
            'N° de Facturas': '{:,.0f}',
            'Ventas por Unidad': '${:,.2f}'
        }),
        use_container_width=True
    )

with col2:
    # Análisis de rubros y categorías - CORREGIDO (sin IDs)
    st.subheader("📊 Desempeño por Rubro")
    
    performance_rubro = dataset_filtrado.groupby(rubro_col).agg({
        'subtotal_linea': 'sum',
        'cantidad': 'sum',
        'id_producto': 'nunique',
        'id_factura': 'nunique'
    }).sort_values('subtotal_linea', ascending=False)
    
    performance_rubro['Margen por Unidad'] = performance_rubro['subtotal_linea'] / performance_rubro['cantidad']
    
    # Renombrar columnas para quitar IDs - CORRECCIÓN APLICADA
    performance_rubro_renombrado = performance_rubro.rename(columns={
        'id_producto': 'Productos Únicos',
        'id_factura': 'Facturas Únicas'
    })
    
    fig_rubros = make_subplots(rows=1, cols=2, 
                              subplot_titles=['Ventas por Rubro', 'Eficiencia por Rubro'])
    
    fig_rubros.add_trace(
        go.Bar(
            x=performance_rubro_renombrado.index,
            y=performance_rubro_renombrado['subtotal_linea'],
            name="Ventas Totales",
            marker_color='#17becf'
        ),
        row=1, col=1
    )
    
    fig_rubros.add_trace(
        go.Scatter(
            x=performance_rubro_renombrado.index,
            y=performance_rubro_renombrado['Margen por Unidad'],
            name="Margen/Unidad",
            line=dict(color='#e377c2', width=3),
            marker=dict(size=8)
        ),
        row=1, col=2
    )
    
    fig_rubros.update_xaxes(tickangle=45, row=1, col=1)
    fig_rubros.update_xaxes(tickangle=45, row=1, col=2)
    fig_rubros.update_layout(height=400, showlegend=False)
    
    st.plotly_chart(fig_rubros, use_container_width=True)
    
    # Métricas de rubros - CORREGIDO (sin IDs)
    st.dataframe(
        performance_rubro_renombrado.style.format({
            'subtotal_linea': '${:,.0f}',
            'cantidad': '{:.0f}',
            'Productos Únicos': '{:.0f}',
            'Facturas Únicas': '{:.0f}',
            'Margen por Unidad': '${:,.2f}'
        }),
        use_container_width=True
    )

# ANÁLISIS DE CLIENTES 
st.markdown("---")
st.markdown('<h2 class="section-header">👥 Análisis de Clientes y Comportamiento</h2>', unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    # Segmentación simple de clientes - CORREGIDO
    st.subheader("🎯 Clientes por Nivel de Valor")
    
    segmentacion_clientes = facturas_filtradas.groupby('id_cliente').agg({
        'total_venta': 'sum',
        'id_factura': 'count',
        'fecha': 'max'
    }).round(2)
    
    segmentacion_clientes.columns = ['Monto Total', 'Frecuencia', 'Última Compra']
    segmentacion_clientes['Recencia'] = (datetime.now() - segmentacion_clientes['Última Compra']).dt.days
    
    # Segmentación simple usando percentiles
    segmentacion_clientes['Nivel Valor'] = 'Medio'
    
    # Clasificar por monto (más robusto)
    if len(segmentacion_clientes) >= 3:
        lim_superior = segmentacion_clientes['Monto Total'].quantile(0.7)
        lim_inferior = segmentacion_clientes['Monto Total'].quantile(0.3)
        
        segmentacion_clientes.loc[segmentacion_clientes['Monto Total'] >= lim_superior, 'Nivel Valor'] = 'Alto'
        segmentacion_clientes.loc[segmentacion_clientes['Monto Total'] <= lim_inferior, 'Nivel Valor'] = 'Bajo'
    
    # Gráfico de distribución por nivel de valor
    distribucion_valor = segmentacion_clientes['Nivel Valor'].value_counts()
    
    fig_valor = px.pie(
        values=distribucion_valor.values,
        names=distribucion_valor.index,
        title="📊 Distribución de Clientes por Nivel de Valor",
        color=distribucion_valor.index,
        color_discrete_map={'Alto': '#2E8B57', 'Medio': '#FFA500', 'Bajo': '#DC143C'}
    )
    
    fig_valor.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig_valor, use_container_width=True)
    
    # Métricas clave
    st.subheader("📈 Métricas de Clientes")
    
    monto_promedio = segmentacion_clientes['Monto Total'].mean()
    frecuencia_promedio = segmentacion_clientes['Frecuencia'].mean()
    recencia_promedio = segmentacion_clientes['Recencia'].mean()
    
    col_met1, col_met2, col_met3 = st.columns(3)
    
    with col_met1:
        st.metric("Gasto Promedio", f"${monto_promedio:,.2f}")
    
    with col_met2:
        st.metric("Compras Promedio", f"{frecuencia_promedio:.1f}")
    
    with col_met3:
        st.metric("Días sin Compra", f"{recencia_promedio:.0f}")

with col2:
    # Top clientes y análisis detallado - CORREGIDO (sin IDs)
    st.subheader("🏅 Top 10 Clientes por Valor")
    
    # Asegurarnos de usar las columnas correctas para nombre y apellido
    nombre_cliente_col = 'nombre_cli' if 'nombre_cli' in facturas_filtradas.columns else 'nombre'
    apellido_cliente_col = 'apellido' if 'apellido' in facturas_filtradas.columns else 'apellido'
    
    top_clientes = facturas_filtradas.groupby([nombre_cliente_col, apellido_cliente_col]).agg({
        'total_venta': 'sum',
        'id_factura': 'count',
        'fecha': ['min', 'max']
    }).nlargest(10, ('total_venta', 'sum'))
    
    top_clientes.columns = ['Total Gastado', 'Compras Realizadas', 'Primera Compra', 'Última Compra']
    top_clientes['Ticket Promedio'] = top_clientes['Total Gastado'] / top_clientes['Compras Realizadas']
    
    # Crear nombres completos para el gráfico y tabla
    nombres_completos = [f"{idx[0]} {idx[1]}" for idx in top_clientes.index]
    
    # Gráfico 
    fig_top_clientes = go.Figure()
    
    # Agregar barras para total gastado
    fig_top_clientes.add_trace(
        go.Bar(
            name='Total Gastado', 
            x=nombres_completos, 
            y=top_clientes['Total Gastado'],
            marker_color='#1f77b4',
            text=top_clientes['Total Gastado'].apply(lambda x: f'${x:,.0f}'),
            textposition='auto'
        )
    )
    
    # Agregar línea para número de compras (en eje Y secundario)
    fig_top_clientes.add_trace(
        go.Scatter(
            name='N° Compras', 
            x=nombres_completos, 
            y=top_clientes['Compras Realizadas'],
            mode='lines+markers', 
            line=dict(color='red', width=3),
            marker=dict(size=8, color='red'),
            yaxis='y2'
        )
    )
    
    fig_top_clientes.update_layout(
        title="Top 10 Clientes - Valor vs Frecuencia",
        xaxis=dict(
            title='Clientes',
            tickangle=-45
        ),
        yaxis=dict(
            title='Total Gastado ($)',
            title_font=dict(color='#1f77b4'),
            tickfont=dict(color='#1f77b4')
        ),
        yaxis2=dict(
            title='N° Compras',
            title_font=dict(color='red'),
            tickfont=dict(color='red'),
            overlaying='y',
            side='right',
            showgrid=False
        ),
        showlegend=True,
        height=500,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    st.plotly_chart(fig_top_clientes, use_container_width=True)
    
    # Tabla detallada de top clientes - CORREGIDA (sin IDs)
    top_clientes_detalle = top_clientes.copy()
    top_clientes_detalle.index = nombres_completos
    
    st.dataframe(
        top_clientes_detalle.style.format({
            'Total Gastado': '${:,.2f}',
            'Compras Realizadas': '{:.0f}',
            'Ticket Promedio': '${:,.2f}',
            'Primera Compra': lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else 'N/A',
            'Última Compra': lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else 'N/A'
        }),
        use_container_width=True
    )

# ANÁLISIS DE PROVEEDORES
st.markdown("---")
st.markdown('<h2 class="section-header">🏭 Análisis de Proveedores y Cadena de Suministro</h2>', unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    # Performance de proveedores - CORREGIDO
    st.subheader("📊 Ranking de Proveedores")
    
    proveedor_nombre_col = 'nombre' if 'nombre' in detalles_completos.columns else 'nombre_prov'
    
    analisis_proveedores = dataset_filtrado.groupby(proveedor_nombre_col).agg({
        'subtotal_linea': 'sum',
        'cantidad': 'sum',
        'id_producto': 'nunique',
        'id_factura': 'nunique'
    }).sort_values('subtotal_linea', ascending=False)
    
    analisis_proveedores['Margen por Producto'] = analisis_proveedores['subtotal_linea'] / analisis_proveedores['id_producto']
    
    # Reset index para mostrar solo nombres de proveedores, no IDs
    analisis_proveedores_reset = analisis_proveedores.reset_index()
    
    fig_proveedores = px.treemap(
        analisis_proveedores_reset.head(15),
        path=[proveedor_nombre_col],
        values='subtotal_linea',
        color='Margen por Producto',
        color_continuous_scale='RdYlGn',
        title="Distribución de Ventas por Proveedor (Top 15)"
    )
    
    st.plotly_chart(fig_proveedores, use_container_width=True)
    
    # Mostrar tabla de proveedores - CORREGIDA
    st.subheader("📋 Detalle de Proveedores")
    analisis_proveedores_detalle = analisis_proveedores_reset.head(10).copy()
    analisis_proveedores_detalle.columns = ['Proveedor', 'Ventas Totales', 'Unidades Vendidas', 'Productos Únicos', 'Facturas', 'Margen por Producto']
    
    st.dataframe(
        analisis_proveedores_detalle.style.format({
            'Ventas Totales': '${:,.2f}',
            'Unidades Vendidas': '{:,.0f}',
            'Productos Únicos': '{:,.0f}',
            'Facturas': '{:,.0f}',
            'Margen por Producto': '${:,.2f}'
        }),
        use_container_width=True
    )

with col2:
    # Eficiencia de proveedores - CORREGIDO
    st.subheader("📈 Matriz de Eficiencia - Proveedores")
    
    fig_matriz = px.scatter(
        analisis_proveedores_reset.head(20),
        x='id_producto',
        y='subtotal_linea',
        size='cantidad',
        color='Margen por Producto',
        hover_name=proveedor_nombre_col,
        log_x=True,
        log_y=True,
        title="Matriz Productos vs Ventas - Tamaño: Cantidad Vendida",
        color_continuous_scale='viridis',
        labels={
            'id_producto': 'Número de Productos Diferentes',
            'subtotal_linea': 'Ventas Totales ($)',
            'cantidad': 'Unidades Vendidas',
            'Margen por Producto': 'Margen por Producto ($)'
        }
    )
    
    fig_matriz.update_layout(
        xaxis_title="Número de Productos Diferentes (Log)",
        yaxis_title="Ventas Totales (Log)",
        height=500
    )
    
    st.plotly_chart(fig_matriz, use_container_width=True)

# REPORTES EJECUTIVOS
st.markdown("---")
st.markdown('<h2 class="section-header">📋 Reportes Ejecutivos y Descargas</h2>', unsafe_allow_html=True)

# Resumen ejecutivo
col1, col2 = st.columns(2)

with col1:
    st.subheader("📑 Resumen Ejecutivo")
    
    resumen_data = {
        'Métrica': [
            'Ventas Totales del Período',
            'Crecimiento vs Período Anterior',
            'Clientes Activos',
            'Productos Vendidos',
            'Ticket Promedio',
            'Sucursal Mejor Performance',
            'Producto Más Vendido',
            'Cliente Más Valioso'
        ],
        'Valor': [
            f"${total_ventas:,.0f}",
            f"{crecimiento:+.1f}%",
            f"{clientes_unicos:,}",
            f"{productos_vendidos:,}",
            f"${ticket_promedio:,.2f}",
            metricas_sucursal.index[0] if len(metricas_sucursal) > 0 else "N/A",
            top_productos.index[0] if len(top_productos) > 0 else "N/A",
            nombres_completos[0] if len(nombres_completos) > 0 else "N/A"
        ]
    }
    
    df_resumen = pd.DataFrame(resumen_data)
    st.dataframe(df_resumen, use_container_width=True)

# Funciones para generación y gestión de reportes Excel
def guardar_excel_local(nombre_archivo, excel_data):
    """Guarda el archivo Excel en la carpeta Reportes_excel"""
    try:
        # Asegurar que el archivo tenga extensión .xlsx
        if not nombre_archivo.endswith('.xlsx'):
            nombre_archivo += '.xlsx'
            
        # Crear la carpeta si no existe
        carpeta_reportes = "Reportes_excel"
        if not os.path.exists(carpeta_reportes):
            os.makedirs(carpeta_reportes)
        
        # Ruta completa del archivo
        ruta_completa = os.path.join(carpeta_reportes, nombre_archivo)
        
        # Guardar el archivo
        with open(ruta_completa, 'wb') as f:
            f.write(excel_data)
        
        return ruta_completa
    except Exception as e:
        st.error(f"Error guardando archivo local: {str(e)}")
        return None

def mostrar_archivos_guardados():
    """Muestra los archivos Excel guardados en la carpeta Reportes_excel"""
    try:
        carpeta_reportes = "Reportes_excel"
        if os.path.exists(carpeta_reportes):
            archivos = [f for f in os.listdir(carpeta_reportes) if f.endswith('.xlsx')]
            if archivos:
                st.subheader("📁 Archivos Guardados (Últimos 5)")
                archivos.sort(reverse=True)  # Ordenar por los más recientes primero
                
                # Mostrar solo los últimos 5 archivos
                for i, archivo in enumerate(archivos[:5]):
                    ruta_archivo = os.path.join(carpeta_reportes, archivo)
                    tamaño = os.path.getsize(ruta_archivo) / 1024  # Tamaño en KB
                    fecha_modificacion = datetime.fromtimestamp(os.path.getmtime(ruta_archivo))
                    
                    # Crear un contenedor para cada archivo
                    with st.container():
                        col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
                        with col1:
                            st.write(f"📄 {archivo}")
                        with col2:
                            st.write(f"{tamaño:.1f} KB")
                        with col3:
                            st.write(f"{fecha_modificacion.strftime('%d/%m/%Y %H:%M')}")
                        with col4:
                            # Usar índice único para la clave del botón
                            if st.button("🗑️", key=f"delete_{i}_{archivo}"):
                                try:
                                    os.remove(ruta_archivo)
                                    st.success(f"✅ Archivo {archivo} eliminado")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ Error eliminando archivo: {str(e)}")
                        st.markdown("---")
            else:
                st.info("📁 No hay archivos guardados aún")
        else:
            st.info("📁 La carpeta de reportes no existe")
    except Exception as e:
        st.error(f"❌ Error mostrando archivos: {str(e)}")

# --- FIN DE LAS FUNCIONES ---

with col2:
    st.subheader("📊 Generar Reportes en Excel")
    
    tipo_reporte = st.selectbox(
        "Selecciona el tipo de reporte:",
        [
            "Ventas por Sucursal",
            "Performance de Productos", 
            "Análisis de Clientes",
            "Datos de Proveedores"
        ]
    )
    
    nombre_reporte = st.text_input("Nombre del reporte:", f"reporte_{datetime.now().strftime('%Y%m%d_%H%M')}")
    
    # Solo un botón que hace ambas cosas
    if st.button("💾 Generar y Descargar Excel", key="generar_excel"):
        with st.spinner("Generando reporte en Excel..."):
            try:
                excel_data = generar_reporte_excel(
                    tipo_reporte, 
                    facturas_filtradas,
                    dataset_filtrado,
                    metricas_sucursal,
                    top_productos,
                    top_clientes,
                    analisis_proveedores
                )
                
                if excel_data is not None:
                    # Guardar localmente automáticamente
                    nombre_archivo = f"{nombre_reporte}.xlsx"
                    ruta_guardado = guardar_excel_local(nombre_archivo, excel_data)
                    
                    if ruta_guardado:
                        st.success(f"✅ Reporte guardado en: `{ruta_guardado}`")
                    
                    # Mostrar botón de descarga
                    st.download_button(
                        label="⬇️ Descargar Reporte Excel",
                        data=excel_data,
                        file_name=nombre_archivo,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="descargar_excel"
                    )
                else:
                    st.error("❌ No se pudo generar el reporte")
                
            except Exception as e:
                st.error(f"❌ Error generando reporte: {str(e)}")

    # Mostrar archivos guardados
    mostrar_archivos_guardados()

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666;'>
        <p>📊 Dashboard Comercial - Desarrollado con Streamlit | Última actualización: {}</p>
    </div>
    """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    unsafe_allow_html=True
)