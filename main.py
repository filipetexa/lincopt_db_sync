# main.py
import pandas as pd
import asyncio
import psycopg2
from config import config
from psycopg2 import sql
from database.connection import get_db_connection
from database.queries import *

async def main_loop():
    while True:
        # Conectar aos bancos de dados
        source_conn = get_db_connection(config['rpa_database'])
        target_conn = get_db_connection(config['lincopt_database'])

        # Query SQL para buscar os dados do banco de origem
        query = "SELECT * FROM executions"

        try:
            # 1. Executar consulta no banco de origem e obter dados
            df = fetch_data(source_conn, query)

            df_ids_exists, df_ids_not_exists = check_data_exists(target_conn, df, "execution_history")
            
            # Para o exists vou fazer um update
            update_existing_data(target_conn, df_ids_exists, "execution_history")
            
            # Para o not exists vou fazer um insert.
            insert_new_data(target_conn, df_ids_not_exists, "execution_history")
            
        finally:
            # Fechar conexões
            source_conn.close()
            target_conn.close()

        await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(main_loop())
