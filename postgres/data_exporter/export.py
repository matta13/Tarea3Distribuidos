import os
import pandas as pd
import psycopg2
from hdfs import InsecureClient
import logging
from dotenv import load_dotenv
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Data-Exporter")

load_dotenv()

def get_db_connection():
    DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
    DB_USER = os.getenv("POSTGRES_USER", "admin")
    DB_PASS = os.getenv("POSTGRES_PASSWORD", "admin123")
    DB_NAME = os.getenv("POSTGRES_DB", "mydatabase")
    conn_str = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
    return psycopg2.connect(conn_str)

def get_hdfs_client():
    HDFS_URL = os.getenv("HDFS_URL", "http://hadoop-namenode:9870")
    return InsecureClient(HDFS_URL, user='root')

def clean_text(text):
    if text is None:
        return ""
    # Replace newlines and carriage returns with a space
    return str(text).replace('\n', ' ').replace('\r', ' ')

def export_table_to_hdfs(table_name, hdfs_dest_folder):
    try:
        conn = get_db_connection()
        client = get_hdfs_client()
        logger.info(f"Reading table '{table_name}'...")
        
        # Read without ID
        query = f"SELECT score, title, body, answer FROM public.{table_name}"
        df = pd.read_sql_query(query, conn)
        
        logger.info(f"Cleaning data for '{table_name}'...")
        # Apply aggressive cleaning to text columns
        text_cols = ['title', 'body', 'answer']
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].apply(clean_text)

        # Ensure score is numeric (handle potential errors)
        df['score'] = pd.to_numeric(df['score'], errors='coerce').fillna(0).astype(int)

        local_file = f"/app/data_export/{table_name}.csv"
        
        # Save as CSV. 
        # quoting=1 (QUOTE_ALL) puts quotes around everything.
        # escapechar='\\' allows escaping quotes inside strings.
        df.to_csv(local_file, index=False, encoding='utf-8', quoting=1, escapechar='\\')
        
        # Ensure HDFS folder exists
        try:
            client.makedirs(hdfs_dest_folder)
        except:
            pass

        hdfs_path = f"{hdfs_dest_folder}/{table_name}.csv"
        client.upload(hdfs_path, local_file, overwrite=True)
        
        logger.info(f"Success: {table_name} -> HDFS:{hdfs_path} ({len(df)} rows)")
        conn.close()
        
    except Exception as e:
        logger.error(f"Error exporting {table_name}: {e}")

def main():
    export_table_to_hdfs("yahoo_dataset", "/data/yahoo")
    export_table_to_hdfs("llm_dataset", "/data/llm")

if __name__ == "__main__":
    main()