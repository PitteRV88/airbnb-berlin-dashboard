# Airbnb Berlin Analytics Dashboard

Dashboard interactivo para analizar datos de Airbnb Berlin, construido con Streamlit y conectado a Snowflake.

![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=Streamlit&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![Plotly](https://img.shields.io/badge/Plotly-3F4F75?style=for-the-badge&logo=plotly&logoColor=white)

## Caracteristicas

- **6 KPIs principales**: Total Listings, Hosts, Reviews, Precio Promedio, Mediana, Superhosts
- **4 secciones de analisis**:
  - Propiedades: Distribucion por tipo, precios
  - Sentimiento: Analisis de reviews (positivo/neutral/negativo)
  - Tendencias: Evolucion temporal de reviews
  - Hosts: Top 15 anfitriones

## Demo

[Ver aplicacion en Streamlit Cloud](https://your-app-url.streamlit.app)

## Requisitos

- Python 3.10+
- Cuenta de Snowflake con acceso a la base de datos AIRBNB

## Instalacion Local

1. Clona el repositorio:
```bash
git clone https://github.com/tu-usuario/airbnb-berlin-dashboard.git
cd airbnb-berlin-dashboard
```

2. Instala las dependencias:
```bash
pip install -r requirements.txt
```

3. Configura las credenciales de Snowflake:
```bash
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
# Edita secrets.toml con tus credenciales
```

4. Ejecuta la aplicacion:
```bash
streamlit run airbnb_dashboard.py
```

## Despliegue en Streamlit Cloud

1. Fork este repositorio
2. Ve a [share.streamlit.io](https://share.streamlit.io)
3. Conecta tu repositorio de GitHub
4. Configura los secrets en la seccion "Advanced settings":

```toml
[connections.snowflake]
account = "TU_CUENTA"
user = "TU_USUARIO"
password = "TU_PASSWORD"
warehouse = "COMPUTE_WH"
database = "AIRBNB"
schema = "RAW"
```

## Estructura del Proyecto

```
airbnb-berlin-dashboard/
├── airbnb_dashboard.py          # Aplicacion principal
├── requirements.txt             # Dependencias Python
├── README.md                    # Este archivo
├── DOCUMENTACION_PROYECTO.txt   # Documentacion tecnica detallada
└── .streamlit/
    └── secrets.toml.example     # Plantilla de configuracion
```

## Datos

El dashboard consume datos de 3 tablas en Snowflake:

| Tabla | Registros | Descripcion |
|-------|-----------|-------------|
| RAW_LISTINGS | 17,499 | Propiedades de Airbnb |
| RAW_HOSTS | 14,111 | Informacion de anfitriones |
| RAW_REVIEWS | 410,284 | Resenas con analisis de sentimiento |

## Autor

Pedro Ulloa - Desarrollado con asistencia de Cortex Code CLI

## Licencia

MIT License
