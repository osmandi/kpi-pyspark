import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px

st.title("Cuadro de Mando Integral")

#https://www.datacamp.com/tutorial/streamlit

# Métricas financieras
st.header("Métricas Financieras")
## Ingresos totales por canal de marketing
ingresos_totales_por_canal_marketing = pd.read_csv("/app/reports/ingresos_totales_por_marketing.csv")
st.write("Ingresos totales por canal de marketing")
st.bar_chart(ingresos_totales_por_canal_marketing, x="channelGrouping", y="totalRevenue")

## Ingresos totales por dispositivo
ingresos_totales_por_dispositivo = pd.read_csv("/app/reports/ingresos_totales_por_dispositivo.csv")
st.write("Ingresos totales por dispositivo")
st.bar_chart(ingresos_totales_por_dispositivo, x="deviceName", y="totalRevenue", horizontal=True)

## Ingresos totales por usuario
ingresos_totales_usuario = pd.read_csv("/app/reports/ingresos_totales_usuario.csv")[["totalRevenue"]]
st.write("Resumen estadístico de ingresos por usuario")
col1, col2 = st.columns(2, vertical_alignment="center")
col1.write(ingresos_totales_usuario.describe())
fig = px.box(ingresos_totales_usuario, y="totalRevenue")
col2.plotly_chart(fig)

# Métricas de clientes
st.header("Métricas Clientes")
col1, col2 = st.columns(2, vertical_alignment="center")
with col1:
    ## Tasa de conversión por canal
    st.write("Tasa de conversión por canal")
    tasa_conversion_por_canal = pd.read_csv("/app/reports/tasa_conversion_por_canal.csv")
    st.bar_chart(tasa_conversion_por_canal, x="channelGrouping", y="conversion_rate")
with col2:
    ## Número de visitas por año
    st.write("Número de visitas por año")
    numero_visitas_por_anio = pd.read_csv("/app/reports/numero_visitas_por_anio.csv")
    numero_visitas_por_anio["year"] = numero_visitas_por_anio["year"].apply(str)
    st.bar_chart(numero_visitas_por_anio, x="year", y="total_visits")

# Procesos internos
st.header("Métricas Procesos Internos")
## Tiempo medio de visita por año
st.write("Tiempo medio de visita por año")
tiempo_medio_visita_anio = pd.read_csv("/app/reports/tiempo_medio_visita_anio.csv")
tiempo_medio_visita_anio["year"] = tiempo_medio_visita_anio["year"].apply(str)
st.bar_chart(tiempo_medio_visita_anio, x="year", y="avg_visit_time")

## Compaña con más visitas
campanas_mas_visitas = pd.read_csv("/app/reports/camapnas_mas_visitas.csv")
st.write("Campaña con más visitas")
st.bar_chart(campanas_mas_visitas, x="campaign", y="count", horizontal=True)

# Aprendizaje y crecimiento
st.header("Métricas Aprendizaje y Crecimiento")
col1, col2 = st.columns(2, vertical_alignment="center")
with col1:
    ## Tasa de retención de usuarios
    st.write("Tasa de retención de usuarios")
    tasa_retencion_usuarios_anio = pd.read_csv("/app/reports/tasa_retencion_usuarios_anio.csv")
    tasa_retencion_usuarios_anio["year"] = tasa_retencion_usuarios_anio["year"].apply(str)
    st.bar_chart(tasa_retencion_usuarios_anio, x="year", y="retention_rate")

with col2:
    ## Bounc rate por año
    st.write("Bounce rate por año")
    bounce_rate_por_anio = pd.read_csv("/app/reports/bounce_rate_por_anio.csv")
    bounce_rate_por_anio["year"] = bounce_rate_por_anio["year"].apply(str)
    st.bar_chart(bounce_rate_por_anio, x="year", y="bounce_rate")
     