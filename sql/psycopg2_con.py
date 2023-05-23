from psycopg2 import connect

def getDBConnection(sUser, sPassword, sHost, sPort, sDbname):
    #sUrl = F"postgresql+psycopg2://{sUser}:{sPassword}@{sHost}:{sPort}/{sDbname}"
    #strCon = host="10.10.0.72", database="dataprocclaro", user="postgresql", password="postgresql"    
    #oDBConn = create_engine(sUrl)
    #oDBConn = oDBConn.raw_connection()

    oDBConn = connect(host= sHost, database = sDbname, user = sUser, password = sPassword)
    
    return oDBConn