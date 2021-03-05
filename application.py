import requests
import json
from wtforms import Form, BooleanField, StringField, PasswordField, validators
from wtforms.fields.html5 import EmailField
from werkzeug.datastructures import MultiDict
from flask import Flask, request


class DataForm(Form):
    name = StringField('Name', [validators.Length(min=4, max=25)])
    address =  StringField('Address', [validators.Length(min=4, max=25)])
    email = EmailField('Email Address', [validators.DataRequired(), validators.Email()])


API_URI = "http://127.0.0.1:9200"





application = Flask(__name__)

@application.route("/ingest", methods=['POST'])
def ingest():
    json_data = request.get_json()

    if 'index' in json_data.keys() and 'payload' in json_data.keys():
        index = json_data['index']
        data = MultiDict(json_data['payload'])
    else:
        return json.dumps({'errors':'missing index or paylod'}), 200, {'ContentType':'application/json'}

    form = DataForm(data)
    if form.validate():
        r = requests.post(API_URI+'/'+index+'/_doc', json=data)


    return json.dumps({'errors':form.errors}), 200, {'ContentType':'application/json'}


if __name__ == "__main__":
    application.run(debug= True)
