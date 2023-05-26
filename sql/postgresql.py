from psycopg2 import connect
from sql.settings_conect import credentials_72

class pgconect():
    #inicio la clase
    def __init__(self, server):
        self.server = server

    #metodos
    def getConect(self):
        if(self.server == 1):
            database = connect(host=credentials_72['pghost'], database=credentials_72['pgdb'], 
                                    user=credentials_72['pguser'], password=credentials_72['pgpasswd'])
        
        return database