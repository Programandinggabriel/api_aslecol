from flask import Flask

app = Flask(__name__)

@app.route('/jobs/<int:iIdJob>', methods=['GET'])
def prueba():
    return 'd'

if __name__ == '__main__':
    app.run(debug=True, port=8000)

#def f_execute_job(id_job:int):
#   return id_job
