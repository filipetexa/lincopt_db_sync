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
    Retorna dois DataFrames: um com os dados que já existem e outro com os dados que não existem.
    """
    # Neste exemplo, vamos assumir que a tabela possui uma coluna 'id' como chave única para verificação
    if 'execution_id' not in df.columns:
        raise ValueError("A coluna 'execution_id' não está presente no DataFrame para verificação de duplicidade.")
    
    ids = df['execution_id'].tolist()
    query = f"SELECT ref_id FROM {table_name} WHERE ref_id = ANY(%s)"
    with connection.cursor() as cursor:
        cursor.execute(query, (ids,))
        result = cursor.fetchall()
        existing_ids = [row[0] for row in result]
    
    # Criar DataFrame com os dados que já existem no banco
    df_exists = df[df['execution_id'].isin(existing_ids)]
    
    # Criar DataFrame com os dados que não existem no banco
    df_not_exists = df[~df['execution_id'].isin(existing_ids)]
    
    return df_exists, df_not_exists

def update_existing_data(connection, df_exists, table_name):
    """
    Atualiza os dados do DataFrame que já existem na tabela especificada no banco de destino.
    """

    if df_exists.empty:
        return
    
    with connection.cursor() as cursor:
        for _, row in df_exists.iterrows():
            query = f"""
            UPDATE {table_name}
            SET ref_id = %s, robot_name = %s, machine_id = %s, start_time = %s, end_time = %s, status = %s
            WHERE ref_id = %s
            """
            cursor.execute(query, (row['execution_id'], row['robot_name'], row['machine_id'], row['start_time'], row['end_time'], row['status'], row['execution_id']))
        connection.commit()

def insert_new_data(connection, df_not_exists, table_name):
    """
    Insere os dados do DataFrame que não existem na tabela especificada no banco de destino.
    """

    if df_not_exists.empty:
        return
    
    with connection.cursor() as cursor:
        for _, row in df_not_exists.iterrows():
            query = f"""
            INSERT INTO {table_name} (ref_id, robot_name, machine_id, start_time, end_time, status)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (row['execution_id'], row['robot_name'], row['machine_id'], row['start_time'], row['end_time'], row['status']))
        connection.commit()