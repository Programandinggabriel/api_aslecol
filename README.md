# aslecol_base_masivos
Realizar API que realice analizis de datos usando librerías de análisis de datos en Python

#preparar ambiente
entorno virtual:
pip install virtualenv

crear entorno virtual
python -m venv base_masivos_env

activarlo win
base_masivos_env\Scripts\activate.bat

activarlo lin
source base_masivos_env/bin/activate


#dentro de la carpeta del proyecto
modulos python:
pip install flask

iniciar servicio
python app.py

controlador postgresql
pip install psycopg2

libreria pandas
pip install pandas







uvicorn main:app --reload
