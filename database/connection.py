# Conecta e interage com banco de dados postgresql
import psycopg2

# Cria uma conexão psycopg2
def get_db_connection(config):
    connection = psycopg2.connect(
        host=config['host'],
        port=config.getint('port'),
        user=config['user'],
        password=config['password'],
        database=config['database']
    )
    return connection