import configparser
import os

config = configparser.ConfigParser()

# obter caminho absoluto do arquivo  config
config_file = os.path.abspath(
    os.path.join(
    os.path.dirname(__file__),
    '..',
    'config.ini'))

# Verificar se o arquivo de configuração existe
if not os.path.isfile(config_file):
    raise FileExistsError(f"Arquivo de configuração não encontrado: {config_file}")
    

config.read(config_file)