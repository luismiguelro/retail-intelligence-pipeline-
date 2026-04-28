import os
import pandas as pd
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, MofNCompleteColumn
from db_connection import get_connection

load_dotenv()

CSV_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "blinkit_grocery_data.csv")
CHUNK_SIZE = 2000
console = Console(force_terminal=True)


def load_csv() -> pd.DataFrame:
    df = pd.read_csv(CSV_PATH)
    df.columns = [c.lower() for c in df.columns]
    return df


def create_table(cur) -> None:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_sales (
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


def ingest(df: pd.DataFrame, conn) -> int:
    cur = conn.cursor()

    console.print("[bold cyan]► Creando tabla raw_sales (si no existe)...[/bold cyan]")
    create_table(cur)

    console.print("[bold cyan]► Limpiando datos previos (TRUNCATE)...[/bold cyan]")
    cur.execute("TRUNCATE TABLE raw_sales;")
    conn.commit()

    rows = [tuple(row) for row in df.itertuples(index=False)]
    chunks = [rows[i : i + CHUNK_SIZE] for i in range(0, len(rows), CHUNK_SIZE)]
    inserted = 0

    with Progress(
        TextColumn("[bold green]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("filas"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Subiendo a Supabase", total=len(rows))

        for i, chunk in enumerate(chunks):
            try:
                execute_values(
                    cur,
                    "INSERT INTO raw_sales VALUES %s",
                    chunk,
                    page_size=CHUNK_SIZE,
                )
                conn.commit()
                inserted += len(chunk)
                progress.advance(task, len(chunk))
                pct = int(inserted / len(rows) * 100)
                print(f"  Chunk {i+1}/{len(chunks)} — {inserted}/{len(rows)} filas ({pct}%)", flush=True)
            except Exception as e:
                conn.rollback()
                console.print(f"[bold red]✗ Error en chunk (fila ~{inserted}): {e}[/bold red]")
                cur.close()
                return inserted

    cur.close()
    return inserted


if __name__ == "__main__":
    console.rule("[bold blue]Retail Intelligence Pipeline — Ingest[/bold blue]")

    console.print(f"[cyan]► Leyendo CSV:[/cyan] {CSV_PATH}")
    df = load_csv()
    console.print(f"[green]✓ {len(df)} filas "
                  f"cargadas desde el CSV[/green]\n")

    console.print("[cyan]► Conectando a Supabase...[/cyan]")
    conn = get_connection()
    console.print("[green]✓ Conexión establecida[/green]\n")

    total = ingest(df, conn)
    conn.close()

    if total == len(df):
        console.print(f"\n[bold green]✓ Carga completada: {total}/{len(df)} filas en raw_sales[/bold green]")
    else:
        console.print(f"\n[bold yellow]⚠ Carga parcial: {total}/{len(df)} filas insertadas[/bold yellow]")


# =============================================================================
# APRENDIZAJE — ¿Por qué dividir en chunks y por qué importa el tamaño?
# =============================================================================
#
# PROBLEMA SIN CHUNKS:
#   - Enviar todo de una sola vez (8523 filas) puede superar el límite de memoria
#     del servidor o el timeout de la conexión de red.
#   - Si falla en la fila 8000, pierdes todo y hay que reiniciar desde cero.
#
# PROBLEMA CON CHUNKS MUY PEQUEÑOS (ej. 1 fila):
#   - Cada chunk es un round-trip a la base de datos:
#     máquina → Supabase (EAST US (NORTH VIRGINIA)) → tu máquina.
#   - Con 8523 filas = 8523 viajes. A ~30ms por viaje = ~4 minutos solo en latencia.
#
# SOLUCIÓN — CHUNK_SIZE = 2000:
#   - Balance entre memoria usada por INSERT y cantidad de round-trips.
#   - 8523 filas / 2000 = 4 viajes en lugar de 8523.
#   - Si falla un chunk, solo se pierde ese bloque; los anteriores ya están
#     confirmados (conn.commit() por chunk).
#
# REGLA PRÁCTICA PARA ELEGIR EL TAMAÑO:
#   - Tablas pequeñas  (<  50k filas): 1000 – 2000  → pocos chunks, rápido
#   - Tablas medianas  (< 500k filas): 5000 – 10000 → balance memoria/velocidad
#   - Tablas grandes   (>  1M  filas): usar COPY en lugar de INSERT (aún más rápido)
#
# execute_values vs executemany:
#   - executemany:    genera N sentencias INSERT separadas  → N round-trips
#   - execute_values: genera 1 INSERT con N filas           → 1 round-trip por chunk
#   Ejemplo real con este dataset:
#     executemany    → ~8523 round-trips → ~4 min
#     execute_values →     4 round-trips → ~5 seg
# =============================================================================

