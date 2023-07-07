from sqlalchemy import create_engine


def getDBconection(sUser, sPassword, sHost, sPort, sDbname):
    oEngine = create_engine(f'postgresql://{sUser}:{sPassword}@{sHost}:{sPort}/{sDbname}')
    #oEngine = create_engine('postgresql://postgres:postgres@10.10.0.72:5432/rpts_claro')
    oConection = oEngine.connect()
    return oConection
