import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

st.title("Cuadro de Mando Integral")


st.header("Métricas Financieras")
st.header("Métricas Clientes")
st.header("Métricas Procesos Internos")
st.header("Métricas Aprendizaje y Crecimiento")
st.caption("---------")
st.header("Sample")
st.header("This is the header")
st.markdown("This is the markdown")
st.subheader("This is the subheader")
st.caption("This is the caption")
st.code("x = 2021")
st.latex(r''' a+a r^1+a r^2+a r^3 ''')


df = pd.read_csv("/app/reports/bounce_rate_por_anio.csv")
tiempo_medio_visita_anio = pd.read_csv("/app/reports/tiempo_medio_visita_anio.csv")[["year", "avg_visit_time"]]
tiempo_medio_visita_anio["year"] = tiempo_medio_visita_anio["year"].apply(str)

# Growing tiempo_medio_visita
tiempo_medio_visita_anio_2018 = tiempo_medio_visita_anio[tiempo_medio_visita_anio["year"] == "2018"].values[0][1]

col1, col2 = st.columns(2)
col1.metric(label="Tiempo medio vista 2018", value=f"{tiempo_medio_visita_anio_2018}", delta="-1")
col2.metric(label="Tiempo medio vista 2018", value=f"{tiempo_medio_visita_anio_2018}", delta="324")
st.write(tiempo_medio_visita_anio)
st.bar_chart(tiempo_medio_visita_anio, x="year", y="avg_visit_time")

ingresos_totales_usuario = pd.read_csv("/app/reports/ingresos_totales_usuario.csv")[["totalRevenue"]]
fig, ax = plt.subplots()
ingresos_totales_usuario["totalRevenue"].plot.box(ax=ax)
st.pyplot(fig)