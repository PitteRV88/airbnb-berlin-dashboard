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


@st.cache_resource
def get_snowflake_connection():
    try:
        return st.connection("snowflake")
    except Exception as e:
        st.error(f"Error conectando a Snowflake: {e}")
        st.info("Configura la conexion en .streamlit/secrets.toml")
        st.stop()


@st.cache_data(ttl=3600, show_spinner="Calculando estadisticas...")
def load_aggregated_stats():
    conn = get_snowflake_connection()
    listing_stats = conn.query("""
        SELECT COUNT(*) as total_listings, COUNT(DISTINCT HOST_ID) as unique_hosts,
            AVG(CAST(REPLACE(PRICE, '$', '') AS DECIMAL(10,2))) as avg_price,
            MEDIAN(CAST(REPLACE(PRICE, '$', '') AS DECIMAL(10,2))) as median_price
        FROM AIRBNB.RAW.RAW_LISTINGS WHERE PRICE IS NOT NULL
    """)
    host_stats = conn.query("""
        SELECT COUNT(*) as total_hosts,
            SUM(CASE WHEN IS_SUPERHOST = 't' THEN 1 ELSE 0 END) as superhosts
        FROM AIRBNB.RAW.RAW_HOSTS
    """)
    review_stats = conn.query("SELECT COUNT(*) as total_reviews FROM AIRBNB.RAW.RAW_REVIEWS")
    return {
        "total_listings": int(listing_stats["TOTAL_LISTINGS"].iloc[0]),
        "unique_hosts": int(listing_stats["UNIQUE_HOSTS"].iloc[0]),
        "avg_price": float(listing_stats["AVG_PRICE"].iloc[0]),
        "median_price": float(listing_stats["MEDIAN_PRICE"].iloc[0]),
        "total_hosts": int(host_stats["TOTAL_HOSTS"].iloc[0]),
        "superhosts": int(host_stats["SUPERHOSTS"].iloc[0]),
        "total_reviews": int(review_stats["TOTAL_REVIEWS"].iloc[0]),
    }


@st.cache_data(ttl=3600)
def load_room_type_distribution():
    conn = get_snowflake_connection()
    df = conn.query("""
        SELECT ROOM_TYPE, COUNT(*) as count,
            AVG(CAST(REPLACE(PRICE, '$', '') AS DECIMAL(10,2))) as avg_price
        FROM AIRBNB.RAW.RAW_LISTINGS WHERE PRICE IS NOT NULL
        GROUP BY ROOM_TYPE ORDER BY count DESC
    """)
    df.columns = df.columns.str.lower()
    return df


@st.cache_data(ttl=3600)
def load_sentiment_distribution():
    conn = get_snowflake_connection()
    df = conn.query("""
        SELECT COALESCE(SENTIMENT, 'unknown') as sentiment, COUNT(*) as count
        FROM AIRBNB.RAW.RAW_REVIEWS GROUP BY SENTIMENT ORDER BY count DESC
    """)
    df.columns = df.columns.str.lower()
    return df


@st.cache_data(ttl=3600)
def load_reviews_by_year():
    conn = get_snowflake_connection()
    df = conn.query("""
        SELECT YEAR(DATE) as year, COUNT(*) as reviews_count,
            SUM(CASE WHEN SENTIMENT = 'positive' THEN 1 ELSE 0 END) as positive,
            SUM(CASE WHEN SENTIMENT = 'neutral' THEN 1 ELSE 0 END) as neutral,
            SUM(CASE WHEN SENTIMENT = 'negative' THEN 1 ELSE 0 END) as negative
        FROM AIRBNB.RAW.RAW_REVIEWS WHERE DATE IS NOT NULL
        GROUP BY YEAR(DATE) ORDER BY year
    """)
    df.columns = df.columns.str.lower()
    return df


@st.cache_data(ttl=3600)
def load_top_hosts():
    conn = get_snowflake_connection()
    df = conn.query("""
        SELECT l.HOST_ID, h.NAME as host_name, h.IS_SUPERHOST, COUNT(*) as listing_count,
            AVG(CAST(REPLACE(l.PRICE, '$', '') AS DECIMAL(10,2))) as avg_price
        FROM AIRBNB.RAW.RAW_LISTINGS l
        LEFT JOIN AIRBNB.RAW.RAW_HOSTS h ON l.HOST_ID = h.ID
        WHERE l.PRICE IS NOT NULL
        GROUP BY l.HOST_ID, h.NAME, h.IS_SUPERHOST
        ORDER BY listing_count DESC LIMIT 15
    """)
    df.columns = df.columns.str.lower()
    return df


@st.cache_data(ttl=3600)
def load_price_by_room_type():
    conn = get_snowflake_connection()
    df = conn.query("""
        SELECT ROOM_TYPE, CAST(REPLACE(PRICE, '$', '') AS DECIMAL(10,2)) as price
        FROM AIRBNB.RAW.RAW_LISTINGS
        WHERE PRICE IS NOT NULL AND CAST(REPLACE(PRICE, '$', '') AS DECIMAL(10,2)) <= 500
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


def render_kpis(stats):
    superhost_pct = (stats["superhosts"] / stats["total_hosts"] * 100) if stats["total_hosts"] > 0 else 0
    cols = st.columns(6)
    with cols[0]:
        st.metric(label="Total Listings", value=f"{stats['total_listings']:,}", border=True)
    with cols[1]:
        st.metric(label="Total Hosts", value=f"{stats['total_hosts']:,}", border=True)
    with cols[2]:
        st.metric(label="Total Reviews", value=f"{stats['total_reviews']:,}", border=True)
    with cols[3]:
        st.metric(label="Precio Promedio", value=f"${stats['avg_price']:.2f}", border=True)
    with cols[4]:
        st.metric(label="Precio Mediana", value=f"${stats['median_price']:.2f}", border=True)
    with cols[5]:
        st.metric(label="Superhosts", value=f"{stats['superhosts']:,}", delta=f"{superhost_pct:.1f}%", border=True)


# Cargar datos
stats = load_aggregated_stats()
room_type_df = load_room_type_distribution()
sentiment_df = load_sentiment_distribution()
reviews_by_year = load_reviews_by_year()
top_hosts_df = load_top_hosts()
price_by_room = load_price_by_room_type()

# Header y KPIs
render_header()
st.divider()
render_kpis(stats)
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
    with col2:
        with st.container(border=True):
            st.subheader("Precio Promedio por Tipo")
            fig = px.bar(room_type_df, x="room_type", y="avg_price", color="room_type", color_discrete_sequence=COLOR_SEQUENCE)
            fig.update_layout(showlegend=False, margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig, use_container_width=True)
    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        with st.container(border=True):
            st.subheader("Distribucion de Precios por Tipo")
            fig = px.box(price_by_room, x="room_type", y="price", color="room_type", color_discrete_sequence=COLOR_SEQUENCE)
            fig.update_layout(showlegend=False, margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig, use_container_width=True)
    with col2:
        with st.container(border=True):
            st.subheader("Histograma de Precios")
            fig = px.histogram(price_by_room, x="price", nbins=50, color_discrete_sequence=[AIRBNB_COLORS["primary"]])
            fig.update_layout(margin=dict(t=20, b=20, l=20, r=20), bargap=0.1)
            st.plotly_chart(fig, use_container_width=True)

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
    with col2:
        with st.container(border=True):
            st.subheader("Evolucion del Sentimiento por Anio")
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=reviews_by_year["year"], y=reviews_by_year["positive"], name="Positivo", fill="tonexty", mode="lines", line=dict(color="#00A699"), stackgroup="one"))
            fig.add_trace(go.Scatter(x=reviews_by_year["year"], y=reviews_by_year["neutral"], name="Neutral", fill="tonexty", mode="lines", line=dict(color="#767676"), stackgroup="one"))
            fig.add_trace(go.Scatter(x=reviews_by_year["year"], y=reviews_by_year["negative"], name="Negativo", fill="tonexty", mode="lines", line=dict(color="#FF5A5F"), stackgroup="one"))
            fig.update_layout(margin=dict(t=20, b=20, l=20, r=20), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
            st.plotly_chart(fig, use_container_width=True)

with tab3:
    with st.container(border=True):
        st.subheader("Tendencia de Reviews por Anio")
        fig = px.line(reviews_by_year, x="year", y="reviews_count", markers=True, color_discrete_sequence=[AIRBNB_COLORS["primary"]])
        fig.update_layout(margin=dict(t=20, b=20, l=20, r=20))
        st.plotly_chart(fig, use_container_width=True)

with tab4:
    with st.container(border=True):
        st.subheader("Top 15 Hosts por Numero de Propiedades")
        df_display = top_hosts_df.copy()
        df_display["is_superhost"] = df_display["is_superhost"].apply(lambda x: "Si" if x == "t" else "No")
        fig = px.bar(df_display.head(10), x="listing_count", y="host_name", orientation="h", color="is_superhost", color_discrete_map={"Si": "#00A699", "No": "#767676"})
        fig.update_layout(margin=dict(t=20, b=20, l=20, r=20), yaxis=dict(categoryorder="total ascending"), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("Ver tabla completa"):
            st.dataframe(df_display, use_container_width=True, hide_index=True)

st.divider()
st.caption("Dashboard creado con Streamlit | Datos: Airbnb Berlin")
