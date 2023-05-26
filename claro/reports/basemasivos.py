from sql.postgresql import pgconect
import pandas as pd

pg = pgconect(1)
oDataBaseConn = pg.getConect()

def main():
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
    
    #Funcion depura los numeros celulares de la info CLARO
    #accountcode -- número de cuenta (identificador deuda cliente) 
    #phonesNumber -- lista con numeros celulares de la info
    def f_get_phones_number(accountcode:str, phonesNumber:list[str]):
        #Funcion valida que el numero de celular cumpla con
        #ciertos caracteres
        def isCellPhone(sPhoneNumber:str):
            if(sPhoneNumber.isnumeric()):
                if(sPhoneNumber.__len__() == 10 and sPhoneNumber[:1] == '3'):
                    #Primeros 4 caractares
                    if(sPhoneNumber[:4] == '3000'):
                        return False
                    
                    return True
                else:
                    return False
            else:
                return False



        #Busqueda y validación de número de celular
        aPhonesClient= []

        #recorre numeros de celular 
        for sPhoneNumber in phonesNumber:
            #depuracion de telefono(s)
            if(isCellPhone(sPhoneNumber)):
                aPhonesClient.append(sPhoneNumber)
            else:
                #posible caracter de - en el número
                sPhoneNumber = sPhoneNumber.replace('-', '')
                if(isCellPhone(sPhoneNumber)):
                    aPhonesClient.append(sPhoneNumber)
                else:
                    #separar numero por los caracteres
                    aCharacters = [';', ',', ' ', '*']
                    
                    #intento buscar número
                    for sChr in aCharacters:
                        if(sPhoneNumber.find(sChr) != -1):
                            aPhonesNumber = sPhoneNumber.split(sep=sChr)

                            for sPhoneNumber in aPhonesNumber:
                                if(isCellPhone(sPhoneNumber)):
                                    aPhonesClient.append(sPhoneNumber)                          
                                else:
                                    #posible caracter de - en el número
                                    sPhoneNumber = sPhoneNumber.replace('-', '')
                                    if(isCellPhone(sPhoneNumber)):
                                        aPhonesClient.append(sPhoneNumber)
        
        #remuevo duplicados
        aPhonesClient = list(dict.fromkeys(aPhonesClient))

        return {'accountcode': accountcode, 'clientcell':aPhonesClient}



    #campos que se usan de la info
    sQuery = "SELECT numerodecliente, accountcode, crmorigen, numeroreferenciadepago, super, exclusion, modinitcta,"
    sQuery +=        "cartera, ascard, verificacion_pyme, rango, region, fechavencimiento, telefono1, telefono2, telefono3, activeslines,"
    sQuery +=        "ciudad,nombredelcliente, min, plan"
    sQuery+=" FROM infofechaxx where accountcode = '9876540014178880'"#WHERE accountcode in ('9876520007830810', '00010860001014', '9876540081167493', '9876540081159169', '00005819006005', '1.16509129', '9876540037642482')"

    dfDataInfo = pd.read_sql_query(sQuery, oDataBaseConn)
    
    #----inicio base de masivos----
    #ELimino (cuentas pagadas CLARO)
    sQuerySelect = "SELECT accountcode FROM claropays_002"
    dfPays = pd.read_sql_query(sQuerySelect, oDataBaseConn)

    dfAccPay = pd.merge(left=dfDataInfo, right=dfPays, on=['accountcode'], how='inner')

    dfDataInfo.drop(dfDataInfo[dfDataInfo['accountcode'].isin(dfAccPay['accountcode'])].index, inplace=True)

    #ELimino (cuentas con pagos sin aplicar CLARO)
    sQuerySelect = "SELECT numeroreferenciadepago FROM psa_002"
    dfPsa = pd.read_sql_query(sQuerySelect, oDataBaseConn)
    
    dfAccPsa = pd.merge(left=dfDataInfo, right=dfPsa, on='numeroreferenciadepago' ,how='inner')

    dfDataInfo.drop(dfDataInfo[dfDataInfo['accountcode'].isin(dfAccPsa['accountcode'])].index, inplace=True)

    #ELimino (cuentas con acuerdos de pago CLARO)
    sQuerySelect = "SELECT accountcode"
    sQuerySelect += " FROM promises_002"
    sQuerySelect +=" WHERE status IN ('ACUERDO DE PAGO', 'TERCEROS REALIZAN ACUERDO')"
    dfPromisess = pd.read_sql_query(sql=sQuerySelect, con=oDataBaseConn)
    
    dfAccPromises = pd.merge(left=dfDataInfo, right=dfPromisess, on='accountcode')
    
    dfDataInfo.drop(dfDataInfo[dfDataInfo['accountcode'].isin(dfAccPromises['accountcode'])].index, inplace=True)

    #ELimino (cuentas NO GESTIONAR CLARO)
    sQuerySelect = "SELECT accountcode"
    sQuerySelect += " FROM blacklist1_002"
    dfNogest = pd.read_sql_query(sql=sQuerySelect, con=oDataBaseConn)

    dfAccNogest = pd.merge(left=dfDataInfo, right=dfNogest, on='accountcode')
    
    dfDataInfo.drop(dfDataInfo[dfDataInfo['accountcode'].isin(dfAccNogest['accountcode'])].index, inplace=True)

    #ELimino (cuentas con reclamaciones, pqrs (CC) de CLARO)
    sQuerySelect = "SELECT numerodecliente"
    sQuerySelect += " FROM blacklist2_002"
    dfPqrs = pd.read_sql_query(sql=sQuerySelect, con=oDataBaseConn)

    dfAccPqrs = pd.merge(left=dfDataInfo, right=dfPqrs, on='numerodecliente')
    
    dfDataInfo.drop(dfDataInfo[dfDataInfo['accountcode'].isin(dfAccPqrs['accountcode'])].index, inplace=True)

    #ELimino (cuentas por solicitud de CLARO) (DATOS A ELIMINAR)
    sQuerySelect = "SELECT accountcode"
    sQuerySelect += " FROM blacklist3_002"
    dfDataDelete = pd.read_sql_query(sql=sQuerySelect, con=oDataBaseConn)

    dfAccDelete = pd.merge(left=dfDataInfo, right=dfDataDelete, on='accountcode')
    
    dfDataInfo.drop(dfDataInfo[dfDataInfo['accountcode'].isin(dfAccDelete['accountcode'])].index, inplace=True)
    
    #Reduzco data de la info para poder extraer los números
    #Extraigo celulares validos
    dictPhones = dfDataInfo.apply(lambda row: f_get_phones_number(row['accountcode'], row[['telefono1', 'telefono2', 'telefono3', 'activeslines']].values), axis=1)
    
    #Data frame que contendra las lineas validas por numero de cuenta
    mValidPhones = pd.DataFrame(columns=['accountcode', 'clientcell'])
    
    #Recorro resultados de la funcion
    if(not dictPhones.empty):
        for aLstPhones in dictPhones:
            #recorro celulares en la lista del dict
            for sCellPhone in aLstPhones['clientcell']:
                mValidPhones.loc[len(mValidPhones)] = {'accountcode':aLstPhones['accountcode'], 'clientcell':sCellPhone}

        #Elimino (celulares en lista negra)
        sQuerySelect = "SELECT clientcell"
        sQuerySelect += " FROM blacklist4_002"
        dfBlackLst = pd.read_sql_query(sQuerySelect, oDataBaseConn)

        dfBlListPhones = pd.merge(left=mValidPhones, right=dfBlackLst, how='inner', on='clientcell') 
        
        mValidPhones.drop(mValidPhones[mValidPhones['clientcell'].isin(dfBlListPhones['clientcell'])].index, inplace=True)
        
        #Elimino (celulares que crucen con el min, si son edades altas de cartera)
        #Cuentas con edades altas
        dfEdAltas =  dfDataInfo[dfDataInfo['cartera'].isin(['120', '150', '180', '210', 'CASTIGO'])]

        #Celulares de las edades altas
        mPhnsEdAltas = mValidPhones.where(mValidPhones['accountcode'].isin(dfEdAltas['accountcode']))
        #Cruzo celulares contra el min
        mPhnsDelete = pd.merge(left=mPhnsEdAltas, right=dfEdAltas, how='inner', left_on='clientcell', right_on='min')

        print(mPhnsDelete)
        
        #CUENTAS QUE NO FUERON ELIMINADAS DESPUES DE EL PROCESO
        #print(pd.merge(left=mValidPhones, right=dfDataInfo, how='inner', on='accountcode'))

        #mValidPhones.to_csv(path_or_buf="C:\\Users\\Gabriel\\Downloads\\tels.csv", sep=';')
        









    #oDataBaseConn.close()

   #print(dfDataInfo)
   #print(mValidPhones)
    
    return 'Fin'
    

    #print(dfDataInfo)
    #dfmassBase = pd.DataFrame(columns=[''])


    #modifico columna plan
    #dfInfox['plan'] = dfInfox.apply(lambda row: f_get_plan_name(row['crmorigen'], row['plan']), axis=1)

    #limpiar nombre de la persona
    #dfInfox.insert(1, 'Nombre', dfInfox['nombredelcliente'].apply(f_get_firts_name))

    #df = df.where(df.nombre_1 == 'Sr(a)')

    #print(aTelefonos.to_string())
    
    return str(True)