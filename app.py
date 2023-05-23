from flask import Flask
#proceso base de masivos claro
from claro_process.massive_base import getData

app = Flask(__name__)

@app.route('/claro/process/<int:iprocId>')
def prueba(iprocId):
    if(iprocId == 1):
        return getData()

if __name__ == '__main__':
    app.run(debug=True, port=4000)

#def f_execute_job(id_job:int):
#   return id_job
