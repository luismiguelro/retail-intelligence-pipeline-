import os
from dotenv import load_dotenv
import psycopg2

# Carga las variables de entorno desde el archivo .env
load_dotenv()

def get_connection():
    """Establece la conexión utilizando la URL de la base de datos."""
    return psycopg2.connect(os.getenv("DATABASE_URL"))

def test_connection():
    """Prueba la conexión y muestra la versión de PostgreSQL."""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()
        print(f"Conexión exitosa: {version[0]}")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")

if __name__ == "__main__":
    test_connection()