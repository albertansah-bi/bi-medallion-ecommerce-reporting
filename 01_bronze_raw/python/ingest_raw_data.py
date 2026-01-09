import os
import pandas as pd
from sqlalchemy import create_engine
import urllib
import os

# -------------------------------
# Get database credentials
# -------------------------------
username = os.environ.get("DB_USER")
password = os.environ.get("DB_PASS")

# Prompt for credentials if environment variables are not set
if not username:
    username = input("Enter DB username: ")
if not password:
    password = input("Enter DB password: ")

# -------------------------------
# Create SQLAlchemy engine
# -------------------------------
server = "."  # local SQL Server
database = "BI_Demo"

# Create connection string for SQL Server
params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password}"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# -------------------------------
#  Read CSV safely
# -------------------------------
base_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(base_dir, "../data/raw_csv_files/orders.csv")

if not os.path.exists(csv_path):
    raise FileNotFoundError(f"CSV file not found at: {csv_path}")

df = pd.read_csv(csv_path)

# -------------------------------
# Upload DataFrame to SQL Server
# -------------------------------
df.to_sql("bronze_orders", engine, if_exists="append", index=False)
print("Data uploaded successfully!")
