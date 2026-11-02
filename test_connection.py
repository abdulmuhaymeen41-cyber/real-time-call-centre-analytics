import os
import pyodbc
from dotenv import load_dotenv

load_dotenv(".env")

SERVER = os.getenv("SQL_SERVER")
DATABASE = os.getenv("SQL_DATABASE")
USERNAME = os.getenv("SQL_USERNAME")
PASSWORD = os.getenv("SQL_PASSWORD")

connection_string = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
)

try:
    print("Connecting to SQL Server...")
    connection = pyodbc.connect(connection_string)
    print("SUCCESS: Connected to SQL Server!")

    cursor = connection.cursor()
    cursor.execute("SELECT DB_NAME()")
    print("Database:", cursor.fetchone()[0])

    connection.close()

except Exception as e:
    print("Connection failed:", e)
