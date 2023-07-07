from flask import Response, jsonify, send_file
from os import getcwd
import base64

class download():
    def __init__(self:object, FilePath:str):
        PathDest = getcwd()
        FileName = Path.split('\\')[Path.split('\\').__len__() - 1]
        
        FileName = FileName

        Path = base64.b64decode(Path)
        sPathName = Path.split('\\')[Path.split('\\').__len__() - 1]
        
        oSend = send_file(Path, as_attachment=True, attachment_filename=sPathName) 
        
        return oSend

print