import os
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(Path(__file__).parent.parent / ".env")  # solo actúa en local; en Cloud usa st.secrets

st.set_page_config(
    page_title="Retail Intelligence | Blinkit Analytics",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── CSS responsivo para móvil ─────────────────────────────────────────────────
st.markdown("""
<style>
/* Reduce padding general en móvil */
@media screen and (max-width: 768px) {
    .block-container {
        padding: 1rem 0.75rem 2rem 0.75rem !important;
    }
    h1 { font-size: 1.4rem !important; }
    h2 { font-size: 1.1rem !important; }
    h3 { font-size: 1rem !important; }

    /* Apila todas las columnas verticalmente */
    [data-testid="stHorizontalBlock"] {
        flex-wrap: wrap !important;
    }
    [data-testid="stHorizontalBlock"] > [data-testid="column"] {
        width: 100% !important;
        min-width: 100% !important;
        flex: 1 1 100% !important;
    }

    /* Tabs: scroll horizontal si no caben */
    [data-testid="stTabs"] [role="tablist"] {
        overflow-x: auto !important;
        flex-wrap: nowrap !important;
        -webkit-overflow-scrolling: touch;
    }
    [data-testid="stTabs"] [role="tab"] {
        font-size: 0.78rem !important;
        padding: 0.4rem 0.6rem !important;
        white-space: nowrap;
    }

    /* Bloques de código: scroll horizontal */
    pre, code {
        overflow-x: auto !important;
        font-size: 0.7rem !important;
    }

    /* Métricas: 2 por fila en móvil */
    [data-testid="metric-container"] {
        border: 1px solid rgba(128,128,128,0.2);
        border-radius: 8px;
        padding: 0.5rem !important;
    }

    /* Gráficos: asegurar ancho completo */
    .js-plotly-plot, .plotly {
        width: 100% !important;
    }
}

/* En pantallas medianas: 2 métricas por fila */
@media screen and (min-width: 480px) and (max-width: 768px) {
    [data-testid="stHorizontalBlock"] > [data-testid="column"] {
        width: 48% !important;
        min-width: 48% !important;
        flex: 1 1 48% !important;
    }
}
</style>
""", unsafe_allow_html=True)

TIER_COLORS = {
    "Budget":    "#27AE60",
    "Mid-Range": "#F39C12",
    "Premium":   "#C0392B",
}

OUTLET_COLORS = {
    "Grocery Store":     "#3498DB",
    "Supermarket Type1": "#9B59B6",
    "Supermarket Type2": "#E67E22",
    "Supermarket Type3": "#1ABC9C",
}


# ── Conexión ──────────────────────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    # Streamlit Cloud provee secrets via st.secrets; local usa .env
    try:
        url = st.secrets["DATABASE_URL"]
    except Exception:
        url = os.getenv("DATABASE_URL", "")
    url = url.replace("aws-1-us-east-1.pooler.supabase.com", "18.213.155.45")
    url = url.replace("postgresql://", "postgresql+psycopg2://")
    sep = "&" if "?" in url else "?"
    url += f"{sep}sslmode=require"
    return create_engine(url)


# ── Queries ───────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_executive_kpis():
    engine = get_engine()
    q = """
        SELECT
            COUNT(DISTINCT f.item_id)                                               AS total_skus,
            COUNT(DISTINCT f.outlet_id)                                             AS total_outlets,
            ROUND(SUM(f.weighted_revenue_potential)::numeric, 0)                   AS total_potential,
            ROUND(100.0 * COUNT(*) FILTER (WHERE f.price_tier = 'Premium') / COUNT(*), 1) AS pct_premium,
            ROUND(100.0 * COUNT(*) FILTER (WHERE f.price_tier = 'Budget')  / COUNT(*), 1) AS pct_budget,
            COUNT(DISTINCT p.item_category)                                         AS total_categories
        FROM public_marts.fact_sales f
        JOIN public_marts.dim_product p ON p.product_key = f.product_key
    """
    return pd.read_sql(text(q), engine).iloc[0]


@st.cache_data(ttl=300)
def load_price_tier_breakdown():
    engine = get_engine()
    q = """
        SELECT
            price_tier,
            COUNT(DISTINCT item_id)                             AS skus,
            ROUND(SUM(weighted_revenue_potential)::numeric, 0) AS revenue_potential,
            ROUND(AVG(item_mrp)::numeric, 2)                   AS avg_mrp
        FROM public_marts.fact_sales
        GROUP BY price_tier
        ORDER BY CASE price_tier WHEN 'Budget' THEN 1 WHEN 'Mid-Range' THEN 2 ELSE 3 END
    """
    return pd.read_sql(text(q), engine)


@st.cache_data(ttl=300)
def load_shelf_by_category():
    engine = get_engine()
    q = """
        SELECT
            p.item_category,
            ROUND(AVG(f.item_shelf_fraction)::numeric, 4)        AS avg_shelf,
            ROUND(AVG(f.item_mrp)::numeric, 2)                   AS avg_mrp,
            COUNT(DISTINCT f.item_id)                            AS skus,
            ROUND(SUM(f.weighted_revenue_potential)::numeric, 0) AS total_potential
        FROM public_marts.fact_sales f
        JOIN public_marts.dim_product p ON p.product_key = f.product_key
        GROUP BY p.item_category
        ORDER BY avg_shelf DESC
    """
    return pd.read_sql(text(q), engine)


@st.cache_data(ttl=300)
def load_shelf_scatter():
    engine = get_engine()
    q = """
        SELECT
            p.item_category,
            f.item_mrp,
            ROUND(AVG(f.item_shelf_fraction)::numeric, 4) AS avg_shelf,
            f.price_tier
        FROM public_marts.fact_sales f
        JOIN public_marts.dim_product p ON p.product_key = f.product_key
        GROUP BY p.item_category, f.item_mrp, f.price_tier
    """
    return pd.read_sql(text(q), engine)


@st.cache_data(ttl=300)
def load_outlet_profile():
    engine = get_engine()
    q = """
        SELECT
            o.outlet_type,
            o.outlet_tier,
            o.outlet_size,
            ROUND(AVG(f.item_mrp)::numeric, 2)                    AS avg_mrp,
            COUNT(DISTINCT f.item_id)                             AS catalog_breadth,
            ROUND(SUM(f.weighted_revenue_potential)::numeric, 0)  AS total_potential,
            ROUND(AVG(f.item_shelf_fraction)::numeric, 4)         AS avg_shelf
        FROM public_marts.fact_sales f
        JOIN public_marts.dim_outlet o ON o.outlet_key = f.outlet_key
        GROUP BY o.outlet_type, o.outlet_tier, o.outlet_size
        ORDER BY total_potential DESC
    """
    return pd.read_sql(text(q), engine)


@st.cache_data(ttl=300)
def load_revenue_by_category():
    engine = get_engine()
    q = """
        SELECT
            p.item_category,
            f.price_tier,
            ROUND(SUM(f.weighted_revenue_potential)::numeric, 0) AS total_potential
        FROM public_marts.fact_sales f
        JOIN public_marts.dim_product p ON p.product_key = f.product_key
        GROUP BY p.item_category, f.price_tier
        ORDER BY total_potential DESC
    """
    return pd.read_sql(text(q), engine)


@st.cache_data(ttl=300)
def load_tier_by_outlet():
    engine = get_engine()
    q = """
        SELECT
            o.outlet_tier,
            f.price_tier,
            ROUND(SUM(f.weighted_revenue_potential)::numeric, 0) AS total_potential
        FROM public_marts.fact_sales f
        JOIN public_marts.dim_outlet o ON o.outlet_key = f.outlet_key
        GROUP BY o.outlet_tier, f.price_tier
        ORDER BY o.outlet_tier, f.price_tier
    """
    return pd.read_sql(text(q), engine)


# ── Carga datos ───────────────────────────────────────────────────────────────
with st.spinner("Cargando datos desde Supabase..."):
    kpis        = load_executive_kpis()
    tiers       = load_price_tier_breakdown()
    shelf_cat   = load_shelf_by_category()
    shelf_scat  = load_shelf_scatter()
    outlets     = load_outlet_profile()
    rev_cat     = load_revenue_by_category()
    tier_outlet = load_tier_by_outlet()


# ── Sidebar — Contexto y Glosario ─────────────────────────────────────────────
with st.sidebar:
    st.caption("☰ Menú lateral — abre con el ícono arriba a la izquierda")
    st.image(
        "https://upload.wikimedia.org/wikipedia/commons/thumb/2/2e/Blinkit-yellow-app-icon.svg/240px-Blinkit-yellow-app-icon.svg.png",
        width=60,
    )
    st.markdown("### ¿Qué es Blinkit?")
    st.markdown(
        "Blinkit es una cadena de supermercados y tiendas de conveniencia en India, "
        "conocida por sus entregas rápidas en minutos. "
        "Este análisis estudia **cómo están posicionados sus productos en cada tipo de tienda.**"
    )

    st.divider()
    st.markdown("### 📖 Glosario")

    st.markdown("**🟢 Budget** — Productos económicos (precio < $70). "
                "Alto volumen, márgenes bajos.")
    st.markdown("**🟡 Mid-Range** — Productos de rango medio ($70–$140). "
                "Equilibrio entre volumen y margen.")
    st.markdown("**🔴 Premium** — Productos de alto valor (precio > $140). "
                "Bajo volumen, margen alto.")

    st.divider()
    st.markdown("**Tienda / Outlet** — Cada punto de venta físico de Blinkit.")
    st.markdown("**Tier de ciudad** — Nivel socioeconómico de la ciudad donde está la tienda. "
                "Tier 1 = grandes ciudades (Mumbai, Delhi). Tier 3 = ciudades pequeñas.")
    st.markdown("**Espacio en góndola** — Fracción del estante que ocupa un producto. "
                "Un valor de 0.07 significa que ocupa el 7% del espacio visible.")
    st.markdown("**Potencial de ventas** — Estimación de ingresos calculada como: "
                "_precio × espacio en góndola_. No son ventas reales, sino una priorización del catálogo.")
    st.markdown("**SKU** — Siglas de *Stock Keeping Unit*. En términos simples: "
                "un producto único en el catálogo.")

    st.divider()
    st.caption("Datos: Blinkit Retail Dataset · Kaggle")


# ── Header ────────────────────────────────────────────────────────────────────
st.title("🛒 Retail Intelligence Dashboard")
st.markdown(
    "**¿Qué responde este dashboard?** "
    "Imagina que eres gerente de categorías de Blinkit y necesitas saber: "
    "_¿qué productos están ocupando más espacio en nuestras tiendas? "
    "¿Los productos más caros están bien posicionados? "
    "¿Qué tipo de tienda tiene más potencial?_ "
    "Este análisis responde esas preguntas con datos reales de **1,559 productos en 10 tiendas** "
    "(5,681 combinaciones producto × tienda)."
)

st.divider()

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Resumen General",
    "🏪 Tipos de Tienda",
    "🛖 Espacio en Estantes",
    "💰 ¿Dónde está el dinero?",
    "🔧 Cómo se construyó",
])


# ════════════════════════════════════════════════════════════════════════════
# TAB 1 — RESUMEN GENERAL
# ════════════════════════════════════════════════════════════════════════════
with tab1:
    st.markdown(
        "#### ¿Cómo está compuesto el catálogo de Blinkit?\n"
        "Un vistazo rápido a los números más importantes: cuántos productos hay, "
        "en cuántas tiendas, y cómo se reparten entre económicos, intermedios y premium."
    )

    with st.expander("📖 ¿No conoces algún término? Abre el glosario"):
        st.markdown(
            "**🟢 Económico (Budget)** — productos con precio menor a $70.\n\n"
            "**🟡 Intermedio (Mid-Range)** — productos entre $70 y $140.\n\n"
            "**🔴 Premium** — productos con precio mayor a $140.\n\n"
            "**Tienda / Outlet** — cada punto de venta físico de Blinkit.\n\n"
            "**Tier de ciudad** — nivel de la ciudad: Tier 1 = grandes ciudades, Tier 3 = pequeñas.\n\n"
            "**Espacio en estante** — fracción del estante que ocupa un producto (0.07 = 7%).\n\n"
            "**Potencial de ventas** — estimación calculada como precio × espacio en estante. "
            "No son ventas reales, sino una forma de priorizar el catálogo.\n\n"
            "**SKU** — producto único en el catálogo (cada referencia individual)."
        )

    st.divider()

    c1, c2, c3 = st.columns(3)
    c1.metric(
        "Productos únicos",
        f"{int(kpis['total_skus']):,}",
        help="Cantidad de productos diferentes en el catálogo (cada referencia cuenta una sola vez).",
    )
    c2.metric(
        "Tiendas analizadas",
        f"{int(kpis['total_outlets']):,}",
        help="Número de puntos de venta incluidos en este análisis.",
    )
    c3.metric(
        "Categorías",
        f"{int(kpis['total_categories']):,}",
        help="Ej: Lácteos, Snacks, Bebidas, Frutas & Verduras, etc.",
    )

    c4, c5, c6 = st.columns(3)
    c4.metric(
        "Potencial de ventas",
        f"${float(kpis['total_potential']):,.0f}",
        help="Estimación de ingresos: precio × espacio en estante. No son ventas reales.",
    )
    c5.metric(
        "🔴 Productos Premium",
        f"{float(kpis['pct_premium']):.1f}%",
        help="Porcentaje del catálogo con precio superior a $140.",
    )
    c6.metric(
        "🟢 Productos Económicos",
        f"{float(kpis['pct_budget']):.1f}%",
        help="Porcentaje del catálogo con precio inferior a $70.",
    )

    st.divider()

    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("¿Cuántos productos hay de cada tipo?")
        st.caption("Distribución del catálogo entre productos económicos, de rango medio y premium.")
        fig_pie = px.pie(
            tiers, values="skus", names="price_tier",
            color="price_tier", color_discrete_map=TIER_COLORS, hole=0.45,
        )
        fig_pie.update_traces(textposition="inside", textinfo="percent+label")
        fig_pie.update_layout(showlegend=False, margin=dict(t=10, b=10))
        st.plotly_chart(fig_pie, width='stretch')
        st.caption(
            "🟢 **Económico** — precio < $70 &nbsp;|&nbsp; "
            "🟡 **Intermedio** — $70–$140 &nbsp;|&nbsp; "
            "🔴 **Premium** — precio > $140"
        )

    with col_r:
        st.subheader("¿Cuál segmento tiene más potencial de ventas?")
        st.caption(
            "Aunque haya más productos económicos, no siempre generan más potencial de ingresos. "
            "Aquí se ve cuál segmento pesa más en el total estimado."
        )
        fig_tier_bar = px.bar(
            tiers, x="price_tier", y="revenue_potential",
            color="price_tier", color_discrete_map=TIER_COLORS,
            text="revenue_potential",
            labels={"price_tier": "Segmento de precio", "revenue_potential": "Potencial de ventas ($)"},
        )
        fig_tier_bar.update_traces(texttemplate="$%{text:,.0f}", textposition="outside")
        fig_tier_bar.update_layout(showlegend=False, margin=dict(t=30, b=10))
        st.plotly_chart(fig_tier_bar, width='stretch')

    top_tier   = tiers.loc[tiers["revenue_potential"].idxmax()]
    budget_row = tiers[tiers["price_tier"] == "Budget"]
    budget_pct = (budget_row["skus"].values[0] / tiers["skus"].sum() * 100) if len(budget_row) else 0
    st.info(
        f"💡 **Lo que nos dice el dato:** El segmento **{top_tier['price_tier']}** concentra "
        f"el mayor potencial de ventas estimado (${float(top_tier['revenue_potential']):,.0f}), "
        f"aunque los productos económicos representan el **{budget_pct:.0f}%** del catálogo. "
        "Esto significa que el catálogo está diseñado para volumen, no para margen. "
        "Una oportunidad: ¿se podría aumentar el espacio de los productos premium sin reducir variedad?"
    )


# ════════════════════════════════════════════════════════════════════════════
# TAB 2 — TIPOS DE TIENDA
# ════════════════════════════════════════════════════════════════════════════
with tab2:
    st.markdown(
        "#### ¿Qué tipo de tienda vende más y tiene mayor variedad?\n"
        "Blinkit opera distintos formatos: desde pequeñas tiendas de barrio (*Grocery Store*) "
        "hasta supermercados grandes (*Supermarket Type 3*). "
        "Cada formato tiene un perfil diferente de productos y potencial."
    )
    st.divider()

    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Variedad de productos vs. precio promedio por tipo de tienda")
        st.caption(
            "Cada punto es un tipo de tienda. "
            "**Eje X:** cuántos productos diferentes tiene. "
            "**Eje Y:** cuánto cuestan en promedio esos productos. "
            "El tamaño del punto indica el potencial de ventas total."
        )
        fig_scatter_outlet = px.scatter(
            outlets, x="catalog_breadth", y="avg_mrp",
            color="outlet_type", size="total_potential", symbol="outlet_tier",
            color_discrete_map=OUTLET_COLORS,
            hover_data={"outlet_size": True, "total_potential": True,
                        "catalog_breadth": True, "avg_mrp": True},
            labels={
                "catalog_breadth": "Variedad de productos",
                "avg_mrp": "Precio promedio ($)",
                "outlet_type": "Tipo de tienda",
                "outlet_tier": "Nivel de ciudad",
                "total_potential": "Potencial ($)",
                "outlet_size": "Tamaño",
            },
        )
        fig_scatter_outlet.update_layout(margin=dict(t=10, b=10))
        st.plotly_chart(fig_scatter_outlet, width='stretch')

    with col_b:
        st.subheader("¿Qué tipo de tienda tiene más potencial de ventas?")
        st.caption("Las barras más largas = mayor potencial de ventas estimado en ese tipo de tienda.")
        outlet_agg = (
            outlets.groupby("outlet_type")["total_potential"]
            .sum().reset_index()
            .sort_values("total_potential", ascending=True)
        )
        fig_outlet_h = px.bar(
            outlet_agg, x="total_potential", y="outlet_type", orientation="h",
            color="outlet_type", color_discrete_map=OUTLET_COLORS,
            text="total_potential",
            labels={"total_potential": "Potencial de ventas ($)", "outlet_type": ""},
        )
        fig_outlet_h.update_traces(texttemplate="$%{text:,.0f}", textposition="outside")
        fig_outlet_h.update_layout(showlegend=False, margin=dict(t=10, b=10))
        st.plotly_chart(fig_outlet_h, width='stretch')

    st.subheader("¿Cómo se mezclan los segmentos de precio en cada nivel de ciudad?")
    st.caption(
        "**Tier 1** = ciudades grandes (ej. Mumbai, Delhi). "
        "**Tier 2** = ciudades medianas. **Tier 3** = ciudades pequeñas. "
        "Las barras apiladas muestran cuánto pesa cada segmento de precio en cada nivel."
    )
    pivot = (
        tier_outlet
        .pivot_table(index="outlet_tier", columns="price_tier", values="total_potential", aggfunc="sum")
        .fillna(0).reset_index()
    )
    fig_stacked = go.Figure()
    for tier_name, color in TIER_COLORS.items():
        if tier_name in pivot.columns:
            fig_stacked.add_trace(go.Bar(
                name=tier_name,
                x=pivot["outlet_tier"],
                y=pivot[tier_name],
                marker_color=color,
            ))
    fig_stacked.update_layout(
        barmode="stack",
        xaxis_title="Nivel de ciudad (Tier)",
        yaxis_title="Potencial de ventas ($)",
        legend_title="Segmento de precio",
        margin=dict(t=10, b=10),
    )
    st.plotly_chart(fig_stacked, width='stretch')

    best = outlets.loc[outlets["total_potential"].idxmax()]
    st.success(
        f"💡 **Lo que nos dice el dato:** Las tiendas tipo **{best['outlet_type']}** "
        f"({best['outlet_tier']}) tienen el mayor potencial individual "
        f"(${float(best['total_potential']):,.0f}) con **{int(best['catalog_breadth'])} productos** diferentes. "
        "Los supermercados más grandes no solo tienen más variedad — también tienen productos más caros, "
        "lo que los convierte en el canal ideal para lanzar productos premium."
    )


# ════════════════════════════════════════════════════════════════════════════
# TAB 3 — ESPACIO EN ESTANTES
# ════════════════════════════════════════════════════════════════════════════
with tab3:
    st.markdown(
        "#### ¿Qué categorías dominan el espacio en los estantes?\n"
        "En retail, **el espacio en el estante vale oro**. Un producto que ocupa más espacio "
        "tiene más visibilidad y, en teoría, más posibilidades de ser comprado. "
        "Pero, ¿coincide eso con los productos más rentables?"
    )
    st.divider()

    col_c, col_d = st.columns(2)

    with col_c:
        st.subheader("¿Qué categoría ocupa más espacio en el estante?")
        st.caption(
            "El número representa la fracción promedio del estante que ocupa cada categoría. "
            "0.07 = ocupa el 7% del espacio visible. "
            "🟢 más espacio → 🔴 menos espacio."
        )
        shelf_sorted = shelf_cat.sort_values("avg_shelf", ascending=True)
        fig_shelf = px.bar(
            shelf_sorted, x="avg_shelf", y="item_category", orientation="h",
            color="avg_shelf", color_continuous_scale="RdYlGn",
            text="avg_shelf",
            labels={"avg_shelf": "Espacio promedio en estante", "item_category": "Categoría"},
        )
        fig_shelf.update_traces(texttemplate="%{text:.3f}", textposition="outside")
        fig_shelf.update_layout(coloraxis_showscale=False, margin=dict(t=10, b=10))
        st.plotly_chart(fig_shelf, width='stretch')

    with col_d:
        st.subheader("¿Los productos más caros están mejor posicionados?")
        st.caption(
            "Si el sistema fuera perfecto, los puntos rojos (Premium) deberían estar "
            "arriba a la derecha: más caros Y más visibles. "
            "¿Eso ocurre realmente?"
        )
        fig_scat2 = px.scatter(
            shelf_scat, x="item_mrp", y="avg_shelf",
            color="price_tier", color_discrete_map=TIER_COLORS,
            hover_data=["item_category"],
            labels={
                "item_mrp": "Precio del producto ($)",
                "avg_shelf": "Espacio en estante",
                "price_tier": "Segmento",
                "item_category": "Categoría",
            },
        )
        fig_scat2.update_layout(margin=dict(t=10, b=10))
        st.plotly_chart(fig_scat2, width='stretch')

    top_shelf = shelf_cat.loc[shelf_cat["avg_shelf"].idxmax(), "item_category"]
    low_shelf = shelf_cat.loc[shelf_cat["avg_shelf"].idxmin(), "item_category"]
    st.warning(
        f"💡 **Lo que nos dice el dato:** **{top_shelf}** domina el espacio de estante, "
        f"mientras que **{low_shelf}** tiene la menor visibilidad. "
        "El precio del producto y el espacio que ocupa NO están directamente relacionados — "
        "hay productos económicos que ocupan más estante que productos premium. "
        "Esto es una señal de que el planograma (la distribución en góndola) "
        "podría estar desalineado con la estrategia de rentabilidad."
    )


# ════════════════════════════════════════════════════════════════════════════
# TAB 4 — ¿DÓNDE ESTÁ EL DINERO?
# ════════════════════════════════════════════════════════════════════════════
with tab4:
    st.markdown(
        "#### ¿Qué categorías tienen más potencial de generar ingresos?\n"
        "Sin datos de ventas reales, usamos una métrica derivada: "
        "**potencial = precio × espacio en estante**. "
        "Esto nos dice qué categorías están mejor 'apostadas' para generar ingresos, "
        "considerando tanto su precio como su visibilidad."
    )
    st.divider()

    col_e, col_f = st.columns(2)

    with col_e:
        st.subheader("Top 10 categorías con mayor potencial")
        st.caption("Las categorías con barras más largas combinan buen precio Y buen espacio en estante.")
        cat_total = (
            rev_cat.groupby("item_category")["total_potential"]
            .sum().reset_index()
            .sort_values("total_potential", ascending=True)
            .tail(10)
        )
        fig_top_cat = px.bar(
            cat_total, x="total_potential", y="item_category", orientation="h",
            text="total_potential", color="total_potential", color_continuous_scale="Blues",
            labels={"total_potential": "Potencial de ventas ($)", "item_category": ""},
        )
        fig_top_cat.update_traces(texttemplate="$%{text:,.0f}", textposition="outside")
        fig_top_cat.update_layout(coloraxis_showscale=False, margin=dict(t=10, b=10))
        st.plotly_chart(fig_top_cat, width='stretch')

    with col_f:
        st.subheader("¿De qué segmento viene ese potencial?")
        st.caption(
            "Dentro de cada categoría, ¿cuánto del potencial viene de productos económicos "
            "vs. intermedios vs. premium?"
        )
        top_cats = (
            rev_cat.groupby("item_category")["total_potential"]
            .sum().nlargest(10).index.tolist()
        )
        fig_comp = px.bar(
            rev_cat[rev_cat["item_category"].isin(top_cats)],
            x="item_category", y="total_potential",
            color="price_tier", color_discrete_map=TIER_COLORS,
            labels={
                "total_potential": "Potencial ($)",
                "item_category": "Categoría",
                "price_tier": "Segmento",
            },
        )
        fig_comp.update_layout(xaxis_tickangle=-35, margin=dict(t=10, b=90))
        st.plotly_chart(fig_comp, width='stretch')

    st.subheader("Eficiencia: ¿cuánto potencial genera cada producto dentro de su categoría?")
    st.caption(
        "**Eje X:** cuántos productos tiene la categoría. "
        "**Eje Y:** potencial total de la categoría. "
        "**Tamaño del círculo:** espacio promedio en estante. "
        "Las categorías arriba a la izquierda son las más eficientes: "
        "pocos productos, mucho potencial."
    )
    cat_full = (
        rev_cat.groupby("item_category")["total_potential"]
        .sum().reset_index()
        .merge(shelf_cat[["item_category", "skus", "avg_shelf"]], on="item_category")
        .assign(efficiency=lambda d: d["total_potential"] / d["skus"])
    )
    fig_bubble = px.scatter(
        cat_full, x="skus", y="total_potential",
        size="avg_shelf", color="efficiency",
        color_continuous_scale="Oranges",
        text="item_category",
        labels={
            "skus": "Número de productos en la categoría",
            "total_potential": "Potencial total de ventas ($)",
            "avg_shelf": "Espacio en estante",
            "efficiency": "Potencial por producto",
        },
    )
    fig_bubble.update_traces(textposition="top center")
    fig_bubble.update_layout(coloraxis_showscale=True, margin=dict(t=10, b=10))
    st.plotly_chart(fig_bubble, width='stretch')

    top_cat = cat_full.loc[cat_full["total_potential"].idxmax(), "item_category"]
    top_eff = cat_full.loc[cat_full["efficiency"].idxmax(), "item_category"]
    top_pot = float(cat_full.loc[cat_full["total_potential"].idxmax(), "total_potential"])
    st.info(
        f"💡 **Lo que nos dice el dato:** **{top_cat}** lidera el potencial total "
        f"(${top_pot:,.0f}), pero **{top_eff}** es la categoría más eficiente: "
        "genera mucho potencial con pocos productos. "
        "Para una estrategia de expansión de catálogo con espacio limitado en tienda, "
        f"**{top_eff}** debería ser la primera opción."
    )


# ════════════════════════════════════════════════════════════════════════════
# TAB 5 — CÓMO SE CONSTRUYÓ
# ════════════════════════════════════════════════════════════════════════════
with tab5:
    st.markdown(
        "#### ¿Cómo se construyó este dashboard?\n"
        "Este proyecto es un pipeline de datos completo, desde la descarga del archivo CSV "
        "hasta el dashboard que estás viendo. Cada paso está automatizado y documentado."
    )
    st.divider()

    st.subheader("El recorrido del dato: de CSV a dashboard")
    st.code("""
  DATOS CRUDOS         INGEST              LIMPIEZA           MODELADO              DASHBOARD
  ────────────────────────────────────────────────────────────────────────────────────────────
  Blinkit Dataset  →  Python           →  Validación     →  dbt Core           →  Streamlit
  (CSV de Kaggle)      psycopg2             y corrección      Staging Layer          + Plotly
  5,681 registros      01_ingest.py         de datos          stg_sales (vista)
                       ↓                    02_clean.py       ↓
                  raw_sales table                         Capa de análisis
                  (Supabase)              clean_sales      dim_product  (tabla)
                                          (Supabase)       dim_outlet   (tabla)
                                                           fact_sales   (tabla)
                                                           ↓
                                                      52 / 52 pruebas ✅
    """, language="text")

    st.divider()

    with st.expander("📥 Paso 1 — Descarga y carga de datos", expanded=True):
        st.markdown("""
**Dataset original:** Blinkit Grocery Dataset (disponible en Kaggle).
Contiene información de 1,559 productos distribuidos en 10 puntos de venta.

**Decisión importante:** El archivo descargado no incluía datos de ventas reales —
solo el catálogo de productos. En lugar de buscar otro dataset, se cambió el enfoque analítico:
de *"¿cuánto vendemos?"* a **"¿cómo está posicionado nuestro catálogo?"**
Esta es una pregunta igual de válida y más honesta con los datos disponibles.

**Carga técnica:** Se usó `execute_values` (carga masiva en PostgreSQL)
para insertar las 5,681 filas de forma eficiente en la tabla `raw_sales` de Supabase.
        """)

    with st.expander("🧹 Paso 2 — Limpieza y corrección de datos"):
        st.markdown("""
Los datos crudos tenían varios problemas típicos del mundo real:

| Problema encontrado | Cómo se resolvió |
|---|---|
| Pesos de productos en blanco | Se rellenó con el peso promedio de esa categoría |
| "Low Fat" escrito de 5 formas distintas | Se normalizó a un solo texto uniforme |
| Tamaño de tienda en blanco | Se asignó "Pequeña" como valor por defecto |
| Tipos de datos incorrectos | Se convirtieron a número donde correspondía |

Al final de la limpieza se corren validaciones automáticas para asegurar
que no queden datos vacíos en columnas críticas.
        """)

    with st.expander("⚙️ Paso 3 — Transformación con dbt (modelado de datos)"):
        st.markdown("""
**dbt** es una herramienta que permite transformar y organizar los datos en capas,
como si fuera un proceso de manufactura para datos.

Se crearon 3 tablas finales conectadas entre sí:

| Tabla | ¿Qué contiene? |
|---|---|
| `dim_product` | Un registro por producto único — nombre, categoría, precio, segmento |
| `dim_outlet` | Un registro por tienda — tipo, tamaño, nivel de ciudad, antigüedad |
| `fact_sales` | Cada combinación producto × tienda con el potencial de ventas calculado |

**Métrica clave calculada:**
> `Potencial de ventas = precio del producto × espacio que ocupa en el estante`

Esta fórmula permite priorizar el catálogo aunque no tengamos datos de ventas reales.

Al final se corren **52 pruebas automáticas** para garantizar la integridad de los datos.
Resultado: **52/52 pruebas correctas ✅**
        """)

    with st.expander("📊 Paso 4 — Dashboard interactivo con Streamlit"):
        st.markdown("""
El dashboard está construido con **Streamlit** (para la interfaz web)
y **Plotly** (para los gráficos interactivos).

Algunas decisiones de diseño:
- Los datos se guardan en memoria por 5 minutos para que el dashboard sea rápido,
  sin necesidad de consultar la base de datos en cada clic.
- Los colores son consistentes en todos los gráficos:
  🟢 Económico · 🟡 Intermedio · 🔴 Premium
- Cada sección incluye un insight automático calculado desde los datos reales,
  no escrito a mano.
        """)

    with st.expander("🛠️ Tecnologías utilizadas"):
        col1, col2, col3, col4 = st.columns(4)
        col1.markdown("**Carga de datos**\n- Python 3.12\n- Pandas\n- psycopg2")
        col2.markdown("**Base de datos**\n- Supabase\n- PostgreSQL 17\n- Conexión pooler")
        col3.markdown("**Transformación**\n- dbt Core\n- Star schema\n- 52 tests automáticos")
        col4.markdown("**Dashboard**\n- Streamlit 1.56\n- Plotly Express\n- Plotly Graph Objects")


# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.markdown(
    "<div style='text-align:center; color:#888; font-size:0.85rem; padding-bottom:1rem;'>"
    "Desarrollado por <strong>Luis Miguel Rodríguez</strong> &nbsp;·&nbsp; "
    "<a href='https://www.luismiguelro.com' target='_blank' style='color:#888;'>"
    "luismiguelro.com</a> &nbsp;·&nbsp; "
    "Pipeline: Python → Supabase → dbt Core → Streamlit"
    "</div>",
    unsafe_allow_html=True,
)
