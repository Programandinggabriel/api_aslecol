from flask import Flask, jsonify, request
from flask_cors import CORS, cross_origin #lib que nos permite intercambio de recursos
from werkzeug.exceptions import InternalServerError

#MASIVOS CLARO
import asignacion.claro.masivos.basemasivos as basemasivos
#from download.masivos.downloadMasivos import 


#Version del api
appVerion = 'v1'

app = Flask(__name__)
CORS(app=app)

@cross_origin
@app.route(f'/api/{appVerion}/asignacion/claro/masivos', methods=['GET'])
def create_basemasivos():
    oResponse = basemasivos.main()
    return jsonify(oResponse)

@app.route(f'/api/{appVerion}/claro/download/masivos', methods=['GET'])
def download_basemasivos():
    vKey = request.args.get('apKey')
    sPath = request.args.get('fullPath')
    
    #valido keys

    print(vKey, sPath)
    return 'descargado'
    #descargo archivo
    #oFile = download(FilePath=sPath)
    
    


@app.errorhandler(500)
def handre_error(oExcept):
    return "Failed", 500


if __name__ == '__main__':
    app.run(debug=True, port=4000)