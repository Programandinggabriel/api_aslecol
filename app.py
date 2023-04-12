from flask import Flask
from process_massivebase_claro import getData

app = Flask(__name__)

@app.route('/process/<int:iprocId>')
def prueba(iprocId):
    if(iprocId == 1):
        return getData()

if __name__ == '__main__':
    app.run(debug=True, port=4000)

#def f_execute_job(id_job:int):
#   return id_job
