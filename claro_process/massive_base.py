from sql import psycopg2_con
from sql import settings_conect
from psycopg2 import extras

import pandas as pd

oDataBaseConn = psycopg2_con.getDBConnection(settings_conect.credentials_72['pguser'], settings_conect.credentials_72['pgpasswd'],
                                              settings_conect.credentials_72['pghost'], settings_conect.credentials_72['pgport'], 
                                              settings_conect.credentials_72['pgdb'])

def getData():
    #funcion extrae nombre plan
    #plan --Nombre plan(equipo, nombre plan)
    def f_get_plan_name(crmorigen:str, plan:str):
        if(plan != ''):
            if(crmorigen == 'ASCARD'):
                plan = plan.split(' ')
                return plan[0].capitalize()
            else:
                return plan
    
    #funcion extrae primer nombre
    #fullName --Nombre completo
    def f_get_firts_name(fullName:str):
        fullName = fullName.lower()
        aNameSplit = fullName.split(' ')
        sValidName = ''

        #recorre cada elem del full nme
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
    
    #funcion extrae numeros de telefono reales retorna JSON-dict con la info
    #accountcode -- número de cuenta (identificador deuda cliente) 
    #phonesNumber -- lista con numeros celulares
    def f_get_phones_number(accountcode:str, phonesNumber:list[str]):
        #valida solo números cel (colombia)
        def isCellPhone(sPhoneNumber:str):
            if(sPhoneNumber.isnumeric()):
                if(sPhoneNumber.__len__() == 10 and sPhoneNumber[:1] == '3'):
                    if(sPhoneNumber.count('0') >= 4):
                        return False
                    
                    return True
                else:
                    return False
            else:
                return False


        
        aValidphones = {}
        aPhonesClient= []
        id_column = 1
        aValidphones['accountcode'] = accountcode
        #recorre numeros de celular 
        for sPhoneNumber in phonesNumber:
            #depuracion de telefono(s)
            if(isCellPhone(sPhoneNumber)):
                aPhonesClient.append(sPhoneNumber)
            else:
                #caracteres a separar
                aCharacters = [';', ',', ' ', '*']
                
                #limpiar número de cel (int)
                #intento buscar número
                for sChr in aCharacters:
                    if(sPhoneNumber.find(sChr) != -1):
                        aPhonesNumber = sPhoneNumber.split(sep=sChr)

                        for sPhoneNumber in aPhonesNumber:
                            if(isCellPhone(sPhoneNumber)):
                                aPhonesClient.append(sPhoneNumber)                           
                            else:
                                #posible caracter de - en el número
                                if(sPhoneNumber.find('-') != -1):
                                    #posibles numeros con - en medio
                                    sPhoneNumber = sPhoneNumber.replace('-', '')

                                    if(isCellPhone(sPhoneNumber)):
                                        aPhonesClient.append(sPhoneNumber) 
        
        aValidphones['validPhones'] = aPhonesClient
        #remueve duplicados de tel en la lista resultante
        #de numeros validos

        #remuevo duplicados
        aValidphones['validPhones'] = list(dict.fromkeys(aValidphones['validPhones']))

        return aValidphones


    #campos que se usan de la info
    sQuery = "SELECT numerodecliente, accountcode, crmorigen, numeroreferenciadepago, super, exclusion, modinitcta,"
    sQuery += "cartera, ascard, verificacion_pyme, rango, region, fechavencimiento, telefono1, telefono2, telefono3, activeslines,"
    sQuery += "ciudad,nombredelcliente, min, plan"
    sQuery+="  FROM infoflex_view LIMIT 100 "#WHERE accountcode in ('9876540102995740', '1.20355603')"
    #sQuery+="  WHERE accountcode in ('1.20355603', '1.20768757')" #WHERE accountcode in ('1.20355603', '1.20768757')

    dfDataInfo = pd.read_sql_query(sQuery, oDataBaseConn)
    #oDataBaseConn.close()

    #----inicio base de masivos----
    #cruzo pagos que realizan a la cartera
    sQuerySelect = "SELECT * FROM claropays_002"
    dfPays = pd.read_sql_query(sQuerySelect, oDataBaseConn)
    #cuentas que pagaron
    dfAccPay = pd.merge(left=dfDataInfo, right=dfPays, on=['accountcode'], how='inner')
    #elimino de la data info
    dfDataInfo.drop(dfDataInfo[dfDataInfo['accountcode'].isin(dfAccPay['accountcode'])].index, inplace=True)

    #cuentas que no se aplicaron pagos
    sQuerySelect = "SELECT * FROM psa_002"
    dfPsa = pd.read_sql_query(sQuerySelect, oDataBaseConn)
    #cruzo cuentas en psa
    dfAccPsa = pd.merge(left=dfDataInfo, right=dfPsa, on='accountcode' ,how='inner')

    dfDataInfo.drop(dfDataInfo[dfDataInfo['accountcode'].isin(dfAccPsa['accountcode'])].index, inplace=True)

    #cuentas que hicieron acuerdo de pago
    sQuerySelect = "SELECT *"
    sQuerySelect += " FROM promises_002"
    sQuerySelect +=" WHERE promise_status IN ('ACUERDO DE PAGO', 'TERCEROS REALIZAN ACUERDO')"

    dfPromisess = pd.read_sql_query(sql=sQuerySelect, con=oDataBaseConn)
    #cruzo promesas de pago
    dfAccPromises = pd.merge(left=dfDataInfo, right=dfPromisess, on='accountcode')
    #cruzo promesas

    dfDataInfo.drop(dfDataInfo[dfDataInfo['accountcode'].isin(dfAccPromises['accountcode'])].index, inplace=True)

    #cuentas que se encuentran en no gestionar
    sQuerySelect = "SELECT *"
    sQuerySelect += " FROM blacklist1_002"
    dfNogest = pd.read_sql_query(sql=sQuerySelect, con=oDataBaseConn)

    #cruzo no gestion
    dfAccNogest = pd.merge(left=dfDataInfo, right=dfNogest, on='accountcode')
    dfDataInfo.drop(dfDataInfo[dfDataInfo['accountcode'].isin(dfAccNogest['accountcode'])].index, inplace=True)

    #cuentas que hicieron reclamaciones (CC)
    sQuerySelect = "SELECT *"
    sQuerySelect += " FROM blacklist2_002"
    dfPqrs = pd.read_sql_query(sql=sQuerySelect, con=oDataBaseConn)

    #cruzo reclamaciones
    dfAccPqrs = pd.merge(left=dfDataInfo, right=dfPqrs, on='numerodecliente')
    dfDataInfo.drop(dfDataInfo[dfDataInfo['accountcode'].isin(dfAccPqrs['accountcode'])].index, inplace=True)

    #Datos a eliminar 
    sQuerySelect = "SELECT *"
    sQuerySelect += " FROM blacklist3_002"
    dfDataDelete = pd.read_sql_query(sql=sQuerySelect, con=oDataBaseConn)

    #cruzo datos a eliminar
    dfAccDelete = pd.merge(left=dfDataInfo, right=dfDataDelete, on='accountcode')
    dfDataInfo.drop(dfDataInfo[dfDataInfo['accountcode'].isin(dfAccDelete['accountcode'])].index, inplace=True)

    #cruzo lista negra
    sQuerySelect = "SELECT * "
    sQuerySelect += " FROM blacklist4_002"

    dfBlackList = pd.read_sql_query(sQuerySelect, oDataBaseConn)
    
    #extraigo lineas de celulares validas del la info
    dfValidPhones = dfDataInfo.apply(lambda row: f_get_phones_number(row['accountcode'], row[['telefono1', 'telefono2', 'telefono3', 'activeslines']].values), axis=1)
    
    #armo dataframe de celulares VALIDOS con los dict de retorno
    mTrueCellPhones = pd.DataFrame(columns=['accountcode', 'clientcell'])
    for vValidPhone in dfValidPhones:
        for sPhone in vValidPhone['validPhones']:
           mTrueCellPhones.loc[len(mTrueCellPhones)] = {'accountcode':vValidPhone['accountcode'], 'clientcell':sPhone}

    #cruzo lista negra contra las lineas validas
    dfPhonesBlist = pd.merge(left=mTrueCellPhones, right=dfBlackList, on='clientcell', how='inner')
    #print(mTrueCellPhones)
    print(dfPhonesBlist['accountcode'])

    #print(dfDataInfo)
    oDataBaseConn.close()
    #dfmassBase = pd.DataFrame(columns=[''])
    
    
    #print(dfDataInfo['accountcode'], dfDataInfo[['telefono1', 'telefono2', 'telefono3']])

    #dfInfox.insert(4, 'telefono2.1', dfInfox['telefono2'].apply(lambda sCellPh: f_get_phones_number(sCellPh)))
    #dfInfox.insert(4, 'telefono3.1', dfInfox['telefono3'].apply(lambda sCellPh: f_get_phones_number(sCellPh)))
    
    #aTelefonos = dfInfox[['telefono1.1', 'telefono2.1', 'telefono3.1']]
    #print(dfInfox[['telefono1', 'telefono1.1', 'telefono2', 'telefono2.1', 'telefono3', 'telefono3.1']])
    #print(dfInfox[['telefono1', 'telefono2', 'telefono3']].values)


    #to_csv(path_or_buf="C:\\Users\\Gabriel\\Downloads\\tels.csv", sep=';')

    #modifico columna plan
    #dfInfox['plan'] = dfInfox.apply(lambda row: f_get_plan_name(row['crmorigen'], row['plan']), axis=1)

    #limpiar nombre de la persona
    #dfInfox.insert(1, 'Nombre', dfInfox['nombredelcliente'].apply(f_get_firts_name))

    #df = df.where(df.nombre_1 == 'Sr(a)')

    #print(aTelefonos.to_string())
    
    return str(True)