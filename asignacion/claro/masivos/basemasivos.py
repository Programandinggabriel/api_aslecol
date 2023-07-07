from flask import jsonify
from sql.postgresql import pgconect
import pandas as pd

import os
from zipfile import ZipFile
from datetime import datetime
from base64 import b64encode

pg = pgconect(1)
oDataBaseConn = pg.getConect()

#funcion extrae nombre plan
#plan --Nombre plan(equipo, nombre plan)
def f_get_plan_name(crmorigen:str, plan:str):
    if(plan != ''):
        if(crmorigen == 'ASCARD'):
            plan = plan.split(' ')
            return plan[0].capitalize()
        else:
            return plan
    else:
        return ''
    
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

#Data frame que contendra las lineas validas por numero de cuenta
mValidPhones = pd.DataFrame(columns=['numerodecliente', 'clientcell'])

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
    
    #añade a la lista de celulares
    def addToDf(accountcode:str, phoneNumber:str):
        #Valida que no exista en la lista
        bValida = mValidPhones.where((mValidPhones['accountcode'] == accountcode)
                                     & (mValidPhones['clientcell'] == phoneNumber)).dropna()
        
        if(bValida.empty):
            mValidPhones.loc[len(mValidPhones)] = {'accountcode':accountcode, 'clientcell':phoneNumber}

    
    #Busqueda y validacion de numero de celular
    #recorre numeros de celular 
    for sPhoneNumber in phonesNumber:
        #depuracion de telefono(s)
        if(isCellPhone(sPhoneNumber)):
            addToDf(accountcode, sPhoneNumber)
        else:
            #posible caracter de - en el número
            sPhoneNumber = sPhoneNumber.replace('-', '')
            if(isCellPhone(sPhoneNumber)):
                addToDf(accountcode, sPhoneNumber)
            else:
                #separar numero por los caracteres
                aCharacters = [';', ',', ' ', '*']
                
                #intento buscar número
                for sChr in aCharacters:
                    if(sPhoneNumber.find(sChr) != -1):
                        aPhonesNumber = sPhoneNumber.split(sep=sChr)

                        for sPhoneNumber in aPhonesNumber:
                            if(isCellPhone(sPhoneNumber)):
                               addToDf(accountcode, sPhoneNumber)                         
                            else:
                                #posible caracter de - en el número
                                sPhoneNumber = sPhoneNumber.replace('-', '')
                                if(isCellPhone(sPhoneNumber)):
                                    addToDf(accountcode, sPhoneNumber)

def f_get_phones_number1(numerodecliente:str, phonesNumber:list[str]):
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
    
    #añade a la lista de celulares
    def addToDf(numerodecliente:str, phoneNumber:str):
        #Valida que no exista en la lista
        bValida = mValidPhones.where((mValidPhones['numerodecliente'] == numerodecliente)
                                     & (mValidPhones['clientcell'] == phoneNumber)).dropna()
        
        if(bValida.empty):
            mValidPhones.loc[len(mValidPhones)] = {'numerodecliente':numerodecliente, 'clientcell':phoneNumber}

    
    #Busqueda y validacion de numero de celular
    #recorre numeros de celular 
    for sPhoneNumber in phonesNumber:
        #depuracion de telefono(s)
        if(isCellPhone(sPhoneNumber)):
            addToDf(numerodecliente, sPhoneNumber)
        else:
            #posible caracter de - en el número
            sPhoneNumber = sPhoneNumber.replace('-', '')
            if(isCellPhone(sPhoneNumber)):
                addToDf(numerodecliente, sPhoneNumber)
            else:
                #separar numero por los caracteres
                aCharacters = [';', ',', ' ', '*']
                
                #intento buscar número
                for sChr in aCharacters:
                    if(sPhoneNumber.find(sChr) != -1):
                        aPhonesNumber = sPhoneNumber.split(sep=sChr)

                        for sPhoneNumber in aPhonesNumber:
                            if(isCellPhone(sPhoneNumber)):
                               addToDf(numerodecliente, sPhoneNumber)                         
                            else:
                                #posible caracter de - en el número
                                sPhoneNumber = sPhoneNumber.replace('-', '')
                                if(isCellPhone(sPhoneNumber)):
                                    addToDf(numerodecliente, sPhoneNumber)    

def f_create_csv(mData:pd.DataFrame):
    if(not mData.empty):
        sDate = datetime.now().strftime('%Y%m%d_%H%M%S')
        sNameCsv = f"Masivos_{sDate}.csv"
        sOutCsv =  sPathOut + '\\csv\\' + sNameCsv
        
        iRowsExcel = 1048574

        mDirOut = {}

        if(len(mData) >= iRowsExcel):
            mData.iloc[:1048574,:].to_csv(path_or_buf=sOutCsv, sep=';', index=False)
            mDirOut['sOutCsv'] = sOutCsv
            
            mData.iloc[1048574:,:].to_csv(path_or_buf=sOutCsv + '(2)', sep=';', index=False)
            mDirOut['sOutCsv1'] = sOutCsv
        else:
            mData.to_csv(path_or_buf=sOutCsv, sep=";", index=False)
            mDirOut['sOutCsv'] = sOutCsv
                
        return {'info':mDirOut, 'status':bool(True)}

    else:
         return {'info':'dataEmpty', 'status':bool(False)}
    
def f_create_zip(mPathData:dict[str:str]):
    sDate = datetime.now().strftime('%Y%m%d_%H%M%S')
    sNameZip = f"Masivos_{sDate}.zip"
    sOutZip = sPathOut + '\\zip\\' + sNameZip

    oZip = ZipFile(file=sOutZip, mode='w')
    
    #Recorro archivos (csv)
    for key in mPathData:
        oZip.write(filename=mPathData[key], arcname=os.path.basename(mPathData[key]))

    oZip.close()
    
    return ''

#GLOBAL
sPathOut = os.path.dirname(os.path.realpath(__file__)) + '\\output\\'
def main():
    #Información de la info
    sQuery= "SELECT numerodecliente, accountcode, crmorigen, numeroreferenciadepago, super, modinitcta,"
    sQuery +=      "saldo_aslesoft, debtageinicial ,cartera, modinitcta, verificacion_pyme, ascard, region,"
    sQuery +=      "rango, activeslines, ciudad, nombredelcliente, min, plan, numeroreferenciadepago,"
    sQuery +=      "telefono1, telefono2, telefono3, telefono4, activeslines, lineasadd" 
    sQuery+=" FROM infoflex_view limit 100"
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
    
    dfAccPsa = pd.merge(left=dfDataInfo, right=dfPsa, on='numeroreferenciadepago', how='inner')

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
    
    #obtengo 1 registro por persona (para obtener numeros de celular de cada 1)
    dfDataPhones = dfDataInfo.drop_duplicates(subset='numerodecliente')

    #limpio numeros celulares
    dfDataPhones.apply(lambda row: f_get_phones_number1(row['numerodecliente'], row[['telefono1', 'telefono2', 'telefono3', 'telefono4', 'activeslines', 'lineasadd']].values), axis = 1)

    if(not mValidPhones.empty):
        #Relaciono celulares del cliente a la data resultante (asignación)
        dfDataInfo = pd.merge(left=dfDataInfo, right=mValidPhones, on='numerodecliente', how='inner')
        
        #Tomo campos necesarios
        dfDataInfo = dfDataInfo[['numerodecliente', 'accountcode', 'crmorigen', 'numeroreferenciadepago', 'super', 'modinitcta',
                                 'saldo_aslesoft', 'debtageinicial' ,'cartera', 'modinitcta', 'verificacion_pyme', 'ascard', 'region',
                                 'rango', 'ciudad', 'nombredelcliente', 'min', 'plan', 'numeroreferenciadepago', 'clientcell']]

        #Elimino (celulares en lista negra)        
        sQuerySelect = "SELECT clientcell"
        sQuerySelect += " FROM blacklist4_002"
        dfBlackLst = pd.read_sql_query(sQuerySelect, oDataBaseConn)

        dfBlListPhones = pd.merge(left=dfDataInfo, right=dfBlackLst, how='inner', on='clientcell') 
        dfDataInfo.drop(dfDataInfo[dfDataInfo['clientcell'].isin(dfBlListPhones['clientcell'])].index, inplace=True)

        #Elimino (celulares que crucen con el min, si son edades altas de cartera)
        dfDataInfo.drop(dfDataInfo.where((dfDataInfo['cartera'].isin(['120', '150', '180', '210', 'CASTIGO'])) 
                                         & (dfDataInfo['min'] == dfDataInfo['clientcell'])).index)

        #ARREGLO DATA PARA EL CSV      
        #Borro min de las cuentas donde no se necesita
        dfDataInfo.loc[dfDataInfo['crmorigen'].isin(['ASCARD', 'BSCS', 'SGA']), 'min'] = ''
        
        #modifico columna de plan
        dfDataInfo['plan'] = dfDataInfo.apply(lambda row: f_get_plan_name(crmorigen=row['crmorigen'], plan=row['plan']), axis=1)
        #inserto columna nombre (Alterada)
        dfDataInfo.insert(17, 'Nombre', dfDataInfo['nombredelcliente'].apply(f_get_firts_name))
        
        #renombro columna
        dfDataInfo =  dfDataInfo.rename(columns={'clientcell': 'Telefono1'})      
        
        #creo zip con csv
        oCSv = f_create_csv(dfDataInfo)
        
        if(oCSv['status'] == True):
           oZip = f_create_zip(mPathData=oCSv['info'])
    
        
    return {'status':'success'}