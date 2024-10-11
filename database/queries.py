# database/queries.py
import pandas as pd
import psycopg2

def fetch_data(connection, query):
    """
    Faz um select especifico no banco de dados e retorna um df
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        records = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
    
    df = pd.DataFrame(records, columns=colnames)
    return df

def insert_data(df, connection, table_name):
    """
    Insere os dados do DataFrame na tabela especificada no banco de destino.
    """
    with connection.cursor() as cursor:
        for _, row in df.iterrows():
            placeholders = ', '.join(['%s'] * len(row))
            query = f"INSERT INTO {table_name} VALUES ({placeholders})"
            cursor.execute(query, tuple(row))
        connection.commit()

def check_table_exists(connection, table_name):
    """
    Verifica se a tabela especificada existe no banco de origem.
    """
    query = """
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_name = %s
    )
    """
    with connection.cursor() as cursor:
        cursor.execute(query, (table_name,))
        return cursor.fetchone()[0]

def check_data_exists(connection, df, table_name):
    """
    Verifica se os dados do DataFrame já existem na tabela especificada no banco de destino.
    """
    # Neste exemplo, vamos assumir que a tabela possui uma coluna 'id' como chave única para verificação
    if 'id' not in df.columns:
        raise ValueError("A coluna 'id' não está presente no DataFrame para verificação de duplicidade.")
    
    ids = df['id'].tolist()
    query = sql.SQL("SELECT id FROM {} WHERE id = ANY(%s)").format(sql.Identifier(table_name))
    with connection.cursor() as cursor:
        cursor.execute(query, (ids,))
        result = cursor.fetchall()
    return len(result) > 0