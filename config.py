import configparser
import os


class Config:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        config = configparser.ConfigParser()
        if not os.path.exists('config.ini'):
            config['DEFAULT'] = {
                "user": 'postgres', "password": 'rp1064',
                "database": 'wrpdv_df', "host": '127.0.0.1',
                "port": 5432
            }
            with open('config.ini', "w") as configFile:
                config.write(configFile)
        else:
            config.read("config.ini")
            if len(config['DEFAULT'].items()) == 0:
                print("WARN: O arquivo de configuração parece inválido.")
                print("WARN: Revise as parametrizações em config.ini")
                print(
                    "WARN: Considere deletar o arquivo config.ini para ele ser gerado no formato padrão do programa.")
        self.data = config['DEFAULT']
