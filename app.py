from flask import Flask

#proceso base de masivos claro
from  claro.reports.basemasivos import main

app = Flask(__name__)

@app.route('/claro/reports/<int:idRpt>')
def prueba(idRpt):
    if(idRpt == 1):
        return main()

if __name__ == '__main__':
    app.run(debug=True, port=4000)
