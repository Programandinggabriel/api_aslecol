from sql import postgresql_con
from sql import settings_conect
from psycopg2 import extras

import pandas as pd

oDataBaseConn = postgresql_con.getDBConnection(settings_conect.settConnInfo['pguser'], settings_conect.settConnInfo['pgpasswd'],
                                              settings_conect.settConnInfo['pghost'], settings_conect.settConnInfo['pgport'], 
                                              settings_conect.settConnInfo['pgdb'])

def getData():
    #funcion extrae primer nombre
    def get_firts_name(sFullName:str):
        sFullName = sFullName.lower()
        aNameSplit = sFullName.split(' ')
        sValidName = ''

        for sElemName in aNameSplit:
            bIsName = True

            #remover algunos caracteres especiales
            sElemName = sElemName.replace('*', '').replace('-', '').replace('.', '')
            sElemName = sElemName.strip()

            if(sElemName.startswith('sr')):
                bIsName = False
            if(sElemName.__len__() <= 2):
                bIsName = False
            elif(not(sElemName.isalpha())):
                bIsName = False

            if(bIsName):
                sValidName = sElemName
                break; 
        
        #ningun nombre valido
        if(bIsName == False):
            sValidName = 'Sr(a)'
        else:
            sValidName = sValidName.title()

        return sValidName     



    #sIn = "('1.12400388')"
    sQuery = "select accountcode, nombredelcliente, telefono1"
    sQuery += " from consoldescar"
    sQuery +=" where telefono1 like '%*%'"
    #sQuery += " where lower(nombredelcliente) like 'sr%' or lower(nombredelcliente) like 'sr/%' order by nombredelcliente asc"
    #sQuery +=F" where accountcode in {sIn}"

    df = pd.read_sql_query(sQuery, oDataBaseConn)
    #limpiar nombre de la persona
    df.insert(1, 'nombre_1', df['nombredelcliente'].apply(get_firts_name))

    #

    #df = df.where(df.nombre_1 == 'Sr(a)')

    print(df.to_string())
    
    return str(True)

