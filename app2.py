from flask import Flask, jsonify, request , flash,redirect,url_for
from flask_restx import Resource, Api, fields, model
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename
import os

import becha as bc
import sqlite3

ALLOWED_EXTENSIONS = set(['csv', 'xlsx', 'xls'])

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


app = Flask(__name__)
api = Api(app, default='Binar Challenge Gold',default_label='Hadi Setiawan',title='BINAR - (DSC-HADI)', version='limited-edition')
UPLOAD_FOLDER ='/data/'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


resource_fields = api.model('Request Post Using Json Model', {
    'old': fields.String,
})

upload_parser = api.parser()
upload_parser.add_argument('file', location='files',
                           type=FileStorage, required=True)

# 1 POST entry text
@api.route('/create')
class OneText(Resource):
    
    # @api.expect(resource_fields)
    @api.doc(body=resource_fields)
    def post(self):
        old = {'old': request.json['old']}
        text = request.json['old']
        print(text)
        
        conn = sqlite3.connect('1.db')
        cur = conn.cursor()
        clean = bc.inputOneText(connect=conn, value=text)

        return jsonify(clean)

# 2 POST upload
@api.route('/upload')
@api.expect(upload_parser)
class UploadFile(Resource):

    def post(self):
        
        success = {"message":"success upload file !"}
        failed = {"message":"failed upload file !"}

        if request.method == 'POST':
        # check if the post request has the file part
            if 'file' not in request.files:
                flash('No file part')
                return failed ,400
            file = request.files['file']
            
            # if user does not select file, browser also
            # submit an empty part without filename
            if file.filename == '':
                flash('No selected file')
                return failed ,400
                
            if file and allowed_file(file.filename):
                filename = secure_filename(file.filename)
                file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
                
                conn = sqlite3.connect('1.db')
                cur = conn.cursor()
                directory = os.path.dirname(__file__)
                tempfilepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)

                filepath = os.path.join(directory,tempfilepath)

                if filename == 'data.csv':
                    bc.putTableOri(connect=conn, file_path = filepath)
                    success['filename'] = filename
                    return jsonify(success)

                if filename == 'abusive.csv':
                    bc.putTableAbusive(connect=conn, file_path=filepath)
                    bc.removeAbusive(connect=conn)
                    success['filename'] = filename
                    return jsonify(success)
                
                if filename == 'new_kamusalay.csv':
                    bc.putKamusAlay(connect=conn , file_path=filepath)
                    bc.replaceKamusAlay(connect=conn)
                    success['filename'] = filename
                    return jsonify(success)
                

# 3 GET ALL DATA
@api.route('/read')
class GetAllData(Resource):
    def get(self):
        
        conn = sqlite3.connect('1.db')
        cur = conn.cursor()
        all_data = bc.getAll(connect=conn)
        return jsonify(all_data)

# 4 GET SPECIFIED ID
@api.route('/read/<id>', endpoint='read', doc={'params':{'id':'Masukan ID'}})
class GetIdData(Resource):
    def get(self,id):
        conn = sqlite3.connect('1.db')
        cur = conn.cursor()
        id_data = bc.getIdData(connect=conn,id=id)
        return jsonify(id_data)

if __name__ == "__main__":
    app.run()