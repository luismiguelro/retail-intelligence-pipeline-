import pandas as pd
from psycopg2.extras import execute_values
from rich.console import Console
from db_connection import get_connection

console = Console(force_terminal=True)

FAT_CONTENT_MAP = {
    "LF":       "Low Fat",
    "low fat":  "Low Fat",
    "reg":      "Regular",
}


def load_raw(conn) -> pd.DataFrame:
    cur = conn.cursor()
    cur.execute("SELECT * FROM raw_sales")
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    cur.close()
    df = pd.DataFrame(rows, columns=cols)
    console.print(f"[green]✓ {len(df)} filas leídas desde raw_sales[/green]")
    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 1. Normalizar Item_Fat_Content: 5 valores → 2
    df["item_fat_content"] = df["item_fat_content"].replace(FAT_CONTENT_MAP)

    # 2. Imputar Item_Weight con mediana por Item_Type (~17% nulos)
    medians = df.groupby("item_type")["item_weight"].transform("median")
    df["item_weight"] = df["item_weight"].fillna(medians)

    # 3. Imputar Outlet_Size con moda por Outlet_Type (~28% nulos), resto → "N/A"
    mode_by_outlet = (
        df.groupby("outlet_type")["outlet_size"]
        .transform(lambda x: x.fillna(x.mode()[0] if not x.mode().empty else "N/A"))
    )
    df["outlet_size"] = df["outlet_size"].fillna(mode_by_outlet).fillna("N/A")

    # 4. Imputar Item_Visibility = 0 con media por Item_Type (error de datos)
    mean_vis = df.groupby("item_type")["item_visibility"].transform(
        lambda x: x.replace(0, x[x > 0].mean())
    )
    df["item_visibility"] = df["item_visibility"].where(df["item_visibility"] > 0, mean_vis)

    return df


def run_asserts(df_raw: pd.DataFrame, df_clean: pd.DataFrame) -> None:
    console.print("\n[bold cyan]► Ejecutando asserts de calidad...[/bold cyan]")
    errors = []

    # Fat content solo debe tener 2 valores
    fat_vals = set(df_clean["item_fat_content"].unique())
    if fat_vals != {"Low Fat", "Regular"}:
        errors.append(f"item_fat_content tiene valores inesperados: {fat_vals}")

    # No deben quedar nulos en item_weight
    nulls_weight = df_clean["item_weight"].isnull().sum()
    if nulls_weight > 0:
        errors.append(f"item_weight tiene {nulls_weight} nulos tras imputación")

    # No deben quedar nulos en outlet_size (NaN explícito, "N/A" es válido)
    nulls_size = df_clean["outlet_size"].isnull().sum()
    if nulls_size > 0:
        errors.append(f"outlet_size tiene {nulls_size} nulos tras imputación")

    # No deben quedar visibility = 0
    zeros_vis = (df_clean["item_visibility"] == 0).sum()
    if zeros_vis > 0:
        errors.append(f"item_visibility tiene {zeros_vis} registros en 0")

    # El número de filas no debe cambiar
    if len(df_raw) != len(df_clean):
        errors.append(f"Filas cambiaron: raw={len(df_raw)} clean={len(df_clean)}")

    if errors:
        for e in errors:
            console.print(f"[bold red]  ✗ {e}[/bold red]")
        raise AssertionError("Falló la validación de calidad — ver errores arriba")

    console.print("[bold green]  ✓ Todos los asserts pasaron[/bold green]")


def create_clean_table(cur) -> None:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS clean_sales (
            item_identifier           TEXT,
            item_weight               FLOAT,
            item_fat_content          TEXT,
            item_visibility           FLOAT,
            item_type                 TEXT,
            item_mrp                  FLOAT,
            outlet_identifier         TEXT,
            outlet_establishment_year INTEGER,
            outlet_size               TEXT,
            outlet_location_type      TEXT,
            outlet_type               TEXT
        );
    """)


def load_clean(df: pd.DataFrame, conn) -> int:
    cur = conn.cursor()
    create_clean_table(cur)
    cur.execute("TRUNCATE TABLE clean_sales;")
    conn.commit()

    rows = [tuple(row) for row in df.itertuples(index=False)]
    execute_values(
        cur,
        "INSERT INTO clean_sales VALUES %s",
        rows,
        page_size=2000,
    )
    conn.commit()
    cur.close()
    return len(rows)


if __name__ == "__main__":
    console.rule("[bold blue]Retail Intelligence Pipeline — Clean[/bold blue]")

    conn = get_connection()

    df_raw = load_raw(conn)
    console.print("\n[cyan]► Aplicando limpieza...[/cyan]")
    df_clean = clean(df_raw)

    run_asserts(df_raw, df_clean)

    console.print("\n[cyan]► Cargando clean_sales en Supabase...[/cyan]")
    total = load_clean(df_clean, conn)
    conn.close()

    console.print(f"[bold green]✓ Carga completada: {total} filas en clean_sales[/bold green]")


# =============================================================================
# APRENDIZAJE — Estrategias de imputación y por qué importan
# =============================================================================
#
# ¿POR QUÉ NO ELIMINAR LOS NULOS?
#   Eliminar filas con nulos reduce el dataset y puede introducir sesgo.
#   Item_Weight tiene 17% de nulos → eliminarlos = perder 968 filas de 5681.
#   Los modelos y dashboards posteriores necesitan datos completos.
#
# ESTRATEGIAS USADAS:
#
#   item_weight → mediana por item_type
#     La mediana es más robusta que la media ante outliers de peso.
#     Productos de la misma categoría tienen pesos similares
#     (ej: todos los "Dairy" pesan entre 1 y 5 kg).
#
#   outlet_size → moda por outlet_type
#     El tamaño del outlet es determinístico según su tipo:
#     Grocery Store siempre es Small, Supermarket Type3 siempre es Medium.
#     La moda captura esa relación sin necesidad de tabla de referencia.
#
#   item_visibility = 0 → media por item_type
#     Visibility=0 es un error de captura (un producto en góndola siempre
#     ocupa espacio visual). Se reemplaza con el promedio de productos
#     similares para mantener consistencia dentro de la categoría.
#
# ASSERTS COMO CONTRATO DE CALIDAD:
#   Los asserts funcionan como un "contrato": si el pipeline corre y los
#   asserts pasan, la tabla clean_sales cumple las reglas de negocio.
#   Si algo cambia en raw_sales (nuevos valores en fat_content, etc.),
#   el assert falla y te avisa antes de que datos sucios lleguen al dashboard.
# =============================================================================
