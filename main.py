# main.py
import pandas as pd
import asyncio
import psycopg2
import config
from psycopg2 import sql
from database.connection import get_db_connection
from database.queries import fetch_data, insert_data, check_table_exists, check_data_exists

async def main_loop():
    while True:
        # Conectar aos bancos de dados
        source_conn = get_db_connection(config['rpa_database'])
        target_conn = get_db_connection(config['lincopt_database'])

        # Query SQL para buscar os dados do banco de origem
        query = "SELECT * FROM nome_da_tabela"

        try:
            # 1. Executar consulta no banco de origem e obter dados
            df = fetch_data(source_conn, query)

            # Realizar transformações necessárias no DataFrame (se necessário)
            df = df.drop(columns=['coluna_irrelevante'], errors='ignore')  # Exemplo de transformação

            # 2. Verifica se os dados já estão no banco do destino
            if not check_data_exists(target_conn, df, "nome_da_tabela"):
                # Inserir os dados no banco de destino
                insert_data(df, target_conn, "nome_da_tabela")

                print("Dados transferidos com sucesso!")
            else:
                print("Dados já existem no destino, ignorando inserção.")
        finally:
            # Fechar conexões
            source_conn.close()
            target_conn.close()

        await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(main_loop())
