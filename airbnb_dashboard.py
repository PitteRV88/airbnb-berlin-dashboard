"""
Airbnb Analytics Dashboard
Dashboard interactivo para analizar datos de Airbnb Berlin.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(
    page_title="Airbnb Berlin Analytics",
    page_icon=":house:",
    layout="wide",
)

AIRBNB_COLORS = {
    "primary": "#FF5A5F",
    "secondary": "#00A699",
    "accent": "#FC642D",
    "dark": "#484848",
    "light": "#767676",
}

COLOR_SEQUENCE = ["#FF5A5F", "#00A699", "#FC642D", "#767676", "#484848"]

ALL_ROOM_TYPES = ["Entire home/apt", "Hotel room", "Private room", "Shared room"]

with st.sidebar:
    st.header("Filtros")
    
    room_types = st.multiselect(
        "Tipo de Habitacion",
        options=ALL_ROOM_TYPES,
        default=ALL_ROOM_TYPES,
        help="Selecciona uno o mas tipos de propiedad"
    )
    
    price_range = st.slider(
        "Rango de Precio ($)",
        min_value=0,
        max_value=500,
        value=(0, 500),
        step=10,
        help="Filtra propiedades por rango de precio"
    )
    
    year_range = st.slider(
        "Periodo de Reviews",
        min_value=2009,
        max_value=2021,
        value=(2009, 2021),
        help="Filtra reviews por rango de anios"
    )
    
    st.divider()
    st.caption("Ajusta los filtros para actualizar el dashboard")

if not room_types:
    st.warning("Selecciona al menos un tipo de habitacion")
    st.stop()

filters_applied = {
    "room_types": room_types,
    "price_range": price_range,
    "year_range": year_range
}


@st.cache_resource
def get_snowflake_connection():
    try:
        return st.connection("snowflake")
    except Exception as e:
        st.error(f"Error conectando a Snowflake: {e}")
        st.info("Configura la conexion en .streamlit/secrets.toml")
        st.stop()


@st.cache_data(ttl=3600, show_spinner="Calculando estadisticas...")
def load_aggregated_stats(room_types: tuple, price_range: tuple):
    conn = get_snowflake_connection()
    room_filter = "', '".join(room_types)
    listing_stats = conn.query(f"""
        SELECT COUNT(*) as total_listings, COUNT(DISTINCT HOST_ID) as unique_hosts,
            AVG(CAST(REPLACE(PRICE, '$', '') AS DECIMAL(10,2))) as avg_price,
            MEDIAN(CAST(REPLACE(PRICE, '$', '') AS DECIMAL(10,2))) as median_price
        FROM AIRBNB.RAW.RAW_LISTINGS 
        WHERE PRICE IS NOT NULL 
            AND ROOM_TYPE IN ('{room_filter}')
            AND CAST(REPLACE(PRICE, '$', '') AS DECIMAL(10,2)) BETWEEN {price_range[0]} AND {price_range[1]}
    """)
    host_stats = conn.query(f"""
        SELECT COUNT(DISTINCT h.ID) as total_hosts,
            SUM(CASE WHEN h.IS_SUPERHOST = 't' THEN 1 ELSE 0 END) as superhosts
        FROM AIRBNB.RAW.RAW_HOSTS h
        INNER JOIN AIRBNB.RAW.RAW_LISTINGS l ON h.ID = l.HOST_ID
        WHERE l.PRICE IS NOT NULL 
            AND l.ROOM_TYPE IN ('{room_filter}')
            AND CAST(REPLACE(l.PRICE, '$', '') AS DECIMAL(10,2)) BETWEEN {price_range[0]} AND {price_range[1]}
    """)
    review_stats = conn.query(f"""
        SELECT COUNT(*) as total_reviews 
        FROM AIRBNB.RAW.RAW_REVIEWS r
        INNER JOIN AIRBNB.RAW.RAW_LISTINGS l ON r.LISTING_ID = l.ID
        WHERE l.ROOM_TYPE IN ('{room_filter}')
            AND l.PRICE IS NOT NULL
            AND CAST(REPLACE(l.PRICE, '$', '') AS DECIMAL(10,2)) BETWEEN {price_range[0]} AND {price_range[1]}
    """)
    return {
        "total_listings": int(listing_stats["TOTAL_LISTINGS"].iloc[0]),
        "unique_hosts": int(listing_stats["UNIQUE_HOSTS"].iloc[0]),
        "avg_price": float(listing_stats["AVG_PRICE"].iloc[0]) if listing_stats["AVG_PRICE"].iloc[0] else 0.0,
        "median_price": float(listing_stats["MEDIAN_PRICE"].iloc[0]) if listing_stats["MEDIAN_PRICE"].iloc[0] else 0.0,
        "total_hosts": int(host_stats["TOTAL_HOSTS"].iloc[0]),
        "superhosts": int(host_stats["SUPERHOSTS"].iloc[0]),
        "total_reviews": int(review_stats["TOTAL_REVIEWS"].iloc[0]),
    }


@st.cache_data(ttl=3600)
def load_room_type_distribution(room_types: tuple, price_range: tuple):
    conn = get_snowflake_connection()
    room_filter = "', '".join(room_types)
    df = conn.query(f"""
        SELECT ROOM_TYPE, COUNT(*) as count,
            AVG(CAST(REPLACE(PRICE, '$', '') AS DECIMAL(10,2))) as avg_price
        FROM AIRBNB.RAW.RAW_LISTINGS 
        WHERE PRICE IS NOT NULL
            AND ROOM_TYPE IN ('{room_filter}')
            AND CAST(REPLACE(PRICE, '$', '') AS DECIMAL(10,2)) BETWEEN {price_range[0]} AND {price_range[1]}
        GROUP BY ROOM_TYPE ORDER BY count DESC
    """)
    df.columns = df.columns.str.lower()
    return df


@st.cache_data(ttl=3600)
def load_sentiment_distribution(room_types: tuple, price_range: tuple, year_range: tuple):
    conn = get_snowflake_connection()
    room_filter = "', '".join(room_types)
    df = conn.query(f"""
        SELECT COALESCE(r.SENTIMENT, 'unknown') as sentiment, COUNT(*) as count
        FROM AIRBNB.RAW.RAW_REVIEWS r
        INNER JOIN AIRBNB.RAW.RAW_LISTINGS l ON r.LISTING_ID = l.ID
        WHERE l.ROOM_TYPE IN ('{room_filter}')
            AND l.PRICE IS NOT NULL
            AND CAST(REPLACE(l.PRICE, '$', '') AS DECIMAL(10,2)) BETWEEN {price_range[0]} AND {price_range[1]}
            AND YEAR(r.DATE) BETWEEN {year_range[0]} AND {year_range[1]}
        GROUP BY r.SENTIMENT ORDER BY count DESC
    """)
    df.columns = df.columns.str.lower()
    return df


@st.cache_data(ttl=3600)
def load_reviews_by_year(room_types: tuple, price_range: tuple, year_range: tuple):
    conn = get_snowflake_connection()
    room_filter = "', '".join(room_types)
    df = conn.query(f"""
        SELECT YEAR(r.DATE) as year, COUNT(*) as reviews_count,
            SUM(CASE WHEN r.SENTIMENT = 'positive' THEN 1 ELSE 0 END) as positive,
            SUM(CASE WHEN r.SENTIMENT = 'neutral' THEN 1 ELSE 0 END) as neutral,
            SUM(CASE WHEN r.SENTIMENT = 'negative' THEN 1 ELSE 0 END) as negative
        FROM AIRBNB.RAW.RAW_REVIEWS r
        INNER JOIN AIRBNB.RAW.RAW_LISTINGS l ON r.LISTING_ID = l.ID
        WHERE r.DATE IS NOT NULL
            AND l.ROOM_TYPE IN ('{room_filter}')
            AND l.PRICE IS NOT NULL
            AND CAST(REPLACE(l.PRICE, '$', '') AS DECIMAL(10,2)) BETWEEN {price_range[0]} AND {price_range[1]}
            AND YEAR(r.DATE) BETWEEN {year_range[0]} AND {year_range[1]}
        GROUP BY YEAR(r.DATE) ORDER BY year
    """)
    df.columns = df.columns.str.lower()
    return df


@st.cache_data(ttl=3600)
def load_top_hosts(room_types: tuple, price_range: tuple):
    conn = get_snowflake_connection()
    room_filter = "', '".join(room_types)
    df = conn.query(f"""
        SELECT l.HOST_ID, h.NAME as host_name, h.IS_SUPERHOST, COUNT(*) as listing_count,
            AVG(CAST(REPLACE(l.PRICE, '$', '') AS DECIMAL(10,2))) as avg_price
        FROM AIRBNB.RAW.RAW_LISTINGS l
        LEFT JOIN AIRBNB.RAW.RAW_HOSTS h ON l.HOST_ID = h.ID
        WHERE l.PRICE IS NOT NULL
            AND l.ROOM_TYPE IN ('{room_filter}')
            AND CAST(REPLACE(l.PRICE, '$', '') AS DECIMAL(10,2)) BETWEEN {price_range[0]} AND {price_range[1]}
        GROUP BY l.HOST_ID, h.NAME, h.IS_SUPERHOST
        ORDER BY listing_count DESC LIMIT 15
    """)
    df.columns = df.columns.str.lower()
    return df


@st.cache_data(ttl=3600)
def load_price_by_room_type(room_types: tuple, price_range: tuple):
    conn = get_snowflake_connection()
    room_filter = "', '".join(room_types)
    df = conn.query(f"""
        SELECT ROOM_TYPE, CAST(REPLACE(PRICE, '$', '') AS DECIMAL(10,2)) as price
        FROM AIRBNB.RAW.RAW_LISTINGS
        WHERE PRICE IS NOT NULL 
            AND ROOM_TYPE IN ('{room_filter}')
            AND CAST(REPLACE(PRICE, '$', '') AS DECIMAL(10,2)) BETWEEN {price_range[0]} AND {price_range[1]}
    """)
    df.columns = df.columns.str.lower()
    return df


def render_header():
    col1, col2 = st.columns([4, 1])
    with col1:
        st.markdown("# :house: Airbnb Berlin Analytics")
        st.caption("Dashboard interactivo de analisis de datos de Airbnb")
    with col2:
        if st.button("Actualizar", type="secondary"):
            st.cache_data.clear()
            st.rerun()


def generate_kpi_narrative(metric_name: str, value, stats: dict, filters: dict) -> str:
    """Genera descripcion en lenguaje natural para cada KPI."""
    room_types_str = ", ".join(filters["room_types"]) if len(filters["room_types"]) <= 2 else f"{len(filters['room_types'])} tipos"
    price_str = f"${filters['price_range'][0]}-${filters['price_range'][1]}"
    
    narratives = {
        "total_listings": f"""
**{value:,} propiedades** estan listadas en Airbnb Berlin para los filtros seleccionados.

- **Tipos incluidos:** {room_types_str}
- **Rango de precio:** {price_str}

Este numero representa las propiedades activas que cumplen con los criterios de filtro. 
El mercado de Airbnb en Berlin es uno de los mas grandes de Europa.
        """,
        "total_hosts": f"""
**{value:,} anfitriones** gestionan propiedades en Berlin.

- **Promedio de propiedades por host:** {stats['total_listings']/value:.1f} listings
- **Superhosts:** {stats['superhosts']:,} ({stats['superhosts']/value*100:.1f}% del total)

Los superhosts son anfitriones con excelentes calificaciones y experiencia comprobada.
        """,
        "total_reviews": f"""
**{value:,} reviews** han sido publicadas por huespedes.

Las reviews son fundamentales para:
- Evaluar la calidad de las propiedades
- Generar confianza entre huespedes y anfitriones
- Analizar el sentimiento general del mercado

El analisis de sentimiento muestra la proporcion de experiencias positivas, neutrales y negativas.
        """,
        "avg_price": f"""
**${value:.2f}** es el precio promedio por noche.

- **Precio mediana:** ${stats['median_price']:.2f}
- **Rango filtrado:** {price_str}

La diferencia entre promedio y mediana indica la presencia de propiedades de lujo que elevan el promedio.
Una mediana menor sugiere que la mayoria de propiedades tienen precios mas accesibles.
        """,
        "median_price": f"""
**${value:.2f}** es el precio mediana por noche.

La mediana es mas representativa que el promedio porque:
- No se ve afectada por valores extremos
- Representa el precio "tipico" del mercado
- Es mejor referencia para presupuestar un viaje

El 50% de las propiedades cuestan menos de ${value:.2f}/noche.
        """,
        "superhosts": f"""
**{value:,} superhosts** operan en Berlin ({stats['superhosts']/stats['total_hosts']*100:.1f}% del total).

Requisitos para ser Superhost:
- Minimo 10 estancias completadas al anio
- Tasa de respuesta del 90%+
- Calificacion de 4.8+ estrellas
- Sin cancelaciones

Los superhosts suelen tener mejores reviews y precios ligeramente mas altos.
        """
    }
    return narratives.get(metric_name, "Informacion no disponible")


def render_kpis(stats, filters):
    superhost_pct = (stats["superhosts"] / stats["total_hosts"] * 100) if stats["total_hosts"] > 0 else 0
    cols = st.columns(6)
    
    with cols[0]:
        st.metric(label="Total Listings", value=f"{stats['total_listings']:,}", border=True)
        with st.popover("Ver detalle"):
            st.markdown(generate_kpi_narrative("total_listings", stats['total_listings'], stats, filters))
    
    with cols[1]:
        st.metric(label="Total Hosts", value=f"{stats['total_hosts']:,}", border=True)
        with st.popover("Ver detalle"):
            st.markdown(generate_kpi_narrative("total_hosts", stats['total_hosts'], stats, filters))
    
    with cols[2]:
        st.metric(label="Total Reviews", value=f"{stats['total_reviews']:,}", border=True)
        with st.popover("Ver detalle"):
            st.markdown(generate_kpi_narrative("total_reviews", stats['total_reviews'], stats, filters))
    
    with cols[3]:
        st.metric(label="Precio Promedio", value=f"${stats['avg_price']:.2f}", border=True)
        with st.popover("Ver detalle"):
            st.markdown(generate_kpi_narrative("avg_price", stats['avg_price'], stats, filters))
    
    with cols[4]:
        st.metric(label="Precio Mediana", value=f"${stats['median_price']:.2f}", border=True)
        with st.popover("Ver detalle"):
            st.markdown(generate_kpi_narrative("median_price", stats['median_price'], stats, filters))
    
    with cols[5]:
        st.metric(label="Superhosts", value=f"{stats['superhosts']:,}", delta=f"{superhost_pct:.1f}%", border=True)
        with st.popover("Ver detalle"):
            st.markdown(generate_kpi_narrative("superhosts", stats['superhosts'], stats, filters))


# Cargar datos con filtros
stats = load_aggregated_stats(tuple(room_types), price_range)
room_type_df = load_room_type_distribution(tuple(room_types), price_range)
sentiment_df = load_sentiment_distribution(tuple(room_types), price_range, year_range)
reviews_by_year = load_reviews_by_year(tuple(room_types), price_range, year_range)
top_hosts_df = load_top_hosts(tuple(room_types), price_range)
price_by_room = load_price_by_room_type(tuple(room_types), price_range)

# Header y KPIs
render_header()
st.divider()
render_kpis(stats, filters_applied)
st.divider()

# Tabs
tab1, tab2, tab3, tab4 = st.tabs([":house: Propiedades", ":star: Sentimiento", ":chart_with_upwards_trend: Tendencias", ":bust_in_silhouette: Hosts"])

with tab1:
    col1, col2 = st.columns(2)
    with col1:
        with st.container(border=True):
            st.subheader("Distribucion por Tipo de Habitacion")
            fig = px.pie(room_type_df, values="count", names="room_type", color_discrete_sequence=COLOR_SEQUENCE, hole=0.4)
            fig.update_traces(textposition="inside", textinfo="percent+label")
            fig.update_layout(showlegend=False, margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig, use_container_width=True)
            with st.expander("Ver analisis"):
                if not room_type_df.empty:
                    dominant = room_type_df.iloc[0]
                    st.markdown(f"""
**Analisis de tipos de propiedad:**

El tipo mas comun es **{dominant['room_type']}** con {dominant['count']:,} propiedades 
({dominant['count']/room_type_df['count'].sum()*100:.1f}% del total).

- Los apartamentos completos suelen ser preferidos por familias y estancias largas
- Las habitaciones privadas son populares entre viajeros individuales con presupuesto limitado
- Las habitaciones compartidas representan la opcion mas economica
                    """)
    with col2:
        with st.container(border=True):
            st.subheader("Precio Promedio por Tipo")
            fig = px.bar(room_type_df, x="room_type", y="avg_price", color="room_type", color_discrete_sequence=COLOR_SEQUENCE)
            fig.update_layout(showlegend=False, margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig, use_container_width=True)
            with st.expander("Ver analisis"):
                if not room_type_df.empty:
                    most_expensive = room_type_df.loc[room_type_df['avg_price'].idxmax()]
                    cheapest = room_type_df.loc[room_type_df['avg_price'].idxmin()]
                    st.markdown(f"""
**Analisis de precios por tipo:**

- **Mas caro:** {most_expensive['room_type']} (${most_expensive['avg_price']:.2f}/noche)
- **Mas economico:** {cheapest['room_type']} (${cheapest['avg_price']:.2f}/noche)
- **Diferencia:** ${most_expensive['avg_price'] - cheapest['avg_price']:.2f}

Los precios reflejan la privacidad y espacio ofrecido. Los apartamentos completos 
justifican precios mas altos por ofrecer cocina, bano privado y mayor comodidad.
                    """)
    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        with st.container(border=True):
            st.subheader("Distribucion de Precios por Tipo")
            fig = px.box(price_by_room, x="room_type", y="price", color="room_type", color_discrete_sequence=COLOR_SEQUENCE)
            fig.update_layout(showlegend=False, margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig, use_container_width=True)
            with st.expander("Ver analisis"):
                st.markdown("""
**Como leer el boxplot:**

- **Linea central:** Mediana (50% de propiedades cuestan menos)
- **Caja:** Rango intercuartil (25% - 75% de los precios)
- **Bigotes:** Rango tipico de precios
- **Puntos:** Valores atipicos (propiedades de lujo o muy economicas)

Una caja mas ancha indica mayor variabilidad de precios en esa categoria.
                """)
    with col2:
        with st.container(border=True):
            st.subheader("Histograma de Precios")
            fig = px.histogram(price_by_room, x="price", nbins=50, color_discrete_sequence=[AIRBNB_COLORS["primary"]])
            fig.update_layout(margin=dict(t=20, b=20, l=20, r=20), bargap=0.1)
            st.plotly_chart(fig, use_container_width=True)
            with st.expander("Ver analisis"):
                if not price_by_room.empty:
                    median_price = price_by_room['price'].median()
                    st.markdown(f"""
**Distribucion de precios:**

La mayoria de propiedades se concentran en el rango de precios bajos, 
con una mediana de **${median_price:.2f}/noche**.

- **Sesgo positivo:** La cola larga hacia la derecha indica presencia de propiedades de lujo
- **Concentracion:** La mayoria de opciones estan por debajo de $150/noche
- **Oportunidad:** Hay buena disponibilidad de opciones economicas en Berlin
                    """)

with tab2:
    sentiment_colors = {"positive": "#00A699", "neutral": "#767676", "negative": "#FF5A5F", "unknown": "#484848"}
    col1, col2 = st.columns(2)
    with col1:
        with st.container(border=True):
            st.subheader("Distribucion de Sentimiento")
            filtered_df = sentiment_df[sentiment_df["sentiment"] != "unknown"]
            fig = px.pie(filtered_df, values="count", names="sentiment", color="sentiment", color_discrete_map=sentiment_colors, hole=0.4)
            fig.update_traces(textposition="inside", textinfo="percent+label")
            fig.update_layout(showlegend=False, margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig, use_container_width=True)
            with st.expander("Ver analisis"):
                if not filtered_df.empty:
                    total = filtered_df['count'].sum()
                    positive_pct = filtered_df[filtered_df['sentiment'] == 'positive']['count'].sum() / total * 100 if total > 0 else 0
                    st.markdown(f"""
**Analisis de sentimiento:**

Las reviews reflejan la satisfaccion general de los huespedes en Berlin.

- **{positive_pct:.1f}%** de las reviews son positivas
- Indicador de un mercado con buena calidad de servicio
- Las reviews negativas suelen relacionarse con limpieza, ubicacion o comunicacion

Un alto porcentaje de reviews positivas sugiere que la mayoria de anfitriones 
mantienen buenos estandares de calidad.
                    """)
    with col2:
        with st.container(border=True):
            st.subheader("Evolucion del Sentimiento por Anio")
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=reviews_by_year["year"], y=reviews_by_year["positive"], name="Positivo", fill="tonexty", mode="lines", line=dict(color="#00A699"), stackgroup="one"))
            fig.add_trace(go.Scatter(x=reviews_by_year["year"], y=reviews_by_year["neutral"], name="Neutral", fill="tonexty", mode="lines", line=dict(color="#767676"), stackgroup="one"))
            fig.add_trace(go.Scatter(x=reviews_by_year["year"], y=reviews_by_year["negative"], name="Negativo", fill="tonexty", mode="lines", line=dict(color="#FF5A5F"), stackgroup="one"))
            fig.update_layout(margin=dict(t=20, b=20, l=20, r=20), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
            st.plotly_chart(fig, use_container_width=True)
            with st.expander("Ver analisis"):
                st.markdown("""
**Evolucion temporal del sentimiento:**

Este grafico de area apilada muestra como ha cambiado la proporcion de 
sentimientos a lo largo de los anios.

- **Verde (Positivo):** Experiencias satisfactorias
- **Gris (Neutral):** Reviews sin opinion clara
- **Rojo (Negativo):** Experiencias insatisfactorias

Las variaciones pueden reflejar cambios en la calidad del servicio, 
nuevas regulaciones o eventos externos (como pandemias).
                """)

with tab3:
    with st.container(border=True):
        st.subheader("Tendencia de Reviews por Anio")
        fig = px.line(reviews_by_year, x="year", y="reviews_count", markers=True, color_discrete_sequence=[AIRBNB_COLORS["primary"]])
        fig.update_layout(margin=dict(t=20, b=20, l=20, r=20))
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("Ver analisis"):
            if not reviews_by_year.empty:
                peak_year = reviews_by_year.loc[reviews_by_year['reviews_count'].idxmax()]
                st.markdown(f"""
**Tendencia historica de reviews:**

El volumen de reviews refleja la actividad del mercado de Airbnb en Berlin.

- **Anio pico:** {int(peak_year['year'])} con {peak_year['reviews_count']:,} reviews
- **Periodo filtrado:** {year_range[0]} - {year_range[1]}

Factores que afectan las tendencias:
- Crecimiento del turismo en Berlin
- Regulaciones locales de alquiler a corto plazo
- Eventos globales (pandemias, crisis economicas)
- Competencia de otras plataformas

La caida en 2020-2021 probablemente refleja el impacto de la pandemia COVID-19.
                """)

with tab4:
    with st.container(border=True):
        st.subheader("Top 15 Hosts por Numero de Propiedades")
        df_display = top_hosts_df.copy()
        df_display["is_superhost"] = df_display["is_superhost"].apply(lambda x: "Si" if x == "t" else "No")
        fig = px.bar(df_display.head(10), x="listing_count", y="host_name", orientation="h", color="is_superhost", color_discrete_map={"Si": "#00A699", "No": "#767676"})
        fig.update_layout(margin=dict(t=20, b=20, l=20, r=20), yaxis=dict(categoryorder="total ascending"), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("Ver analisis"):
            if not df_display.empty:
                total_listings_top = df_display['listing_count'].sum()
                superhost_count = (df_display['is_superhost'] == 'Si').sum()
                st.markdown(f"""
**Analisis de los principales anfitriones:**

Los top 15 hosts gestionan **{total_listings_top:,} propiedades** en total.

- **Superhosts en el top:** {superhost_count} de 15
- **Promedio de propiedades:** {total_listings_top/len(df_display):.1f} por host

Estos hosts profesionales representan una parte significativa del mercado.
Algunos pueden ser empresas de gestion de propiedades en lugar de 
anfitriones individuales.

La presencia de superhosts en el ranking indica que la calidad y cantidad 
pueden ir de la mano en el mercado de Airbnb.
                """)
        with st.expander("Ver tabla completa"):
            st.dataframe(df_display, use_container_width=True, hide_index=True)

st.divider()
st.caption("Dashboard creado con Streamlit | Datos: Airbnb Berlin")
