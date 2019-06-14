from flask import Flask, g, jsonify, make_response
from flask_restplus import Api, Resource, fields
import sqlite3
from os import path

app = Flask(__name__)
api = Api(app, version='1.0', title='Data Service for NSW opal card type monthly figures. July 2016 to April 2019.',
          description='This is a Flask-Restplus data service that allows a client to consume APIs related to NSW opal card type monthly figures. July 2016 to April 2019.',
          )

# Database helper
ROOT = path.dirname(path.realpath(__file__))


def connect_db():
    sql = sqlite3.connect(path.join(ROOT, "NSW_OPAL_CARD_TYPE_JULY_2016_APRIL_2019.sqlite"))
    sql.row_factory = sqlite3.Row
    return sql


def get_db():
    if not hasattr(g, 'sqlite_db'):
        g.sqlite_db = connect_db()
    return g.sqlite_db


@api.route('/all')
class NSWOpalAll(Resource):
    @api.response(200, 'SUCCESSFUL: Contents successfully loaded')
    @api.response(204, 'NO CONTENT: No content in database')
    @api.doc(description='Retrieving all records from the database for all train lines.')
    def get(self):
        db = get_db()
        details_cur = db.execute(
            'select TRAIN_LINE, CARD_TYPE, PERIOD, COUNT from NSW_OPAL_CARD_TYPE_JULY_2016_APRIL_2019')
        details = details_cur.fetchall()

        return_values = []

        for detail in details:
            detail_dict = {}
            detail_dict['TRAIN_LINE'] = detail['TRAIN_LINE']
            detail_dict['CARD_TYPE'] = detail['CARD_TYPE']
            detail_dict['PERIOD'] = detail['PERIOD']
            detail_dict['COUNT'] = detail['COUNT']

            return_values.append(detail_dict)

        return make_response(jsonify(return_values), 200)


@api.route('/all/TRAIN_LINE/<string:TRAIN_LINE>', methods=['GET'])
class NSWOpalTrainLine(Resource):
    @api.response(200, 'SUCCESSFUL: Contents successfully loaded')
    @api.response(204, 'NO CONTENT: No content in database')
    @api.doc(description='Retrieving all records from the database for selected train line.')
    def get(self, TRAIN_LINE):
        db = get_db()
        details_cur = db.execute(
            'select TRAIN_LINE, CARD_TYPE, PERIOD, COUNT from NSW_OPAL_CARD_TYPE_JULY_2016_APRIL_2019 where TRAIN_LINE like ? COLLATE NOCASE',
            ["%" + TRAIN_LINE + "%"])
        details = details_cur.fetchall()

        return_values = []

        for detail in details:
            detail_dict = {}
            detail_dict['TRAIN_LINE'] = detail['TRAIN_LINE']
            detail_dict['CARD_TYPE'] = detail['CARD_TYPE']
            detail_dict['PERIOD'] = detail['PERIOD']
            detail_dict['COUNT'] = detail['COUNT']

            return_values.append(detail_dict)

        return make_response(jsonify(return_values), 200)


@api.route('/all/CARD_TYPE/<string:CARD_TYPE>', methods=['GET'])
class NSWOpalCardType(Resource):
    @api.response(200, 'SUCCESSFUL: Contents successfully loaded')
    @api.response(204, 'NO CONTENT: No content in database')
    @api.doc(description='Retrieving all records from the database selected CARD_TYPE.')
    def get(self, CARD_TYPE):
        db = get_db()
        details_cur = db.execute(
            'select TRAIN_LINE, CARD_TYPE, PERIOD, COUNT from NSW_OPAL_CARD_TYPE_JULY_2016_APRIL_2019 where CARD_TYPE = ? COLLATE NOCASE',
            [CARD_TYPE])
        details = details_cur.fetchall()

        return_values = []

        for detail in details:
            detail_dict = {}
            detail_dict['TRAIN_LINE'] = detail['TRAIN_LINE']
            detail_dict['CARD_TYPE'] = detail['CARD_TYPE']
            detail_dict['PERIOD'] = detail['PERIOD']
            detail_dict['COUNT'] = detail['COUNT']

            return_values.append(detail_dict)

        return make_response(jsonify(return_values), 200)


@api.route('/all/PERIOD/<string:PERIOD>', methods=['GET'])
class NSWOpalPeriod(Resource):
    @api.response(200, 'SUCCESSFUL: Contents successfully loaded')
    @api.response(204, 'NO CONTENT: No content in database')
    @api.doc(description='Retrieving all records from the database selected PERIOD.')
    def get(self, PERIOD):
        db = get_db()
        details_cur = db.execute(
            'select TRAIN_LINE, CARD_TYPE, PERIOD, COUNT from NSW_OPAL_CARD_TYPE_JULY_2016_APRIL_2019 where PERIOD = ? COLLATE NOCASE',
            [PERIOD])
        details = details_cur.fetchall()

        return_values = []

        for detail in details:
            detail_dict = {}
            detail_dict['TRAIN_LINE'] = detail['TRAIN_LINE']
            detail_dict['CARD_TYPE'] = detail['CARD_TYPE']
            detail_dict['PERIOD'] = detail['PERIOD']
            detail_dict['COUNT'] = detail['COUNT']

            return_values.append(detail_dict)

        return make_response(jsonify(return_values), 200)


@api.route('/all/TRAIN_LINE/PERIOD_or_CARD/<string:TRAIN_LINE>/<string:PERIOD_or_CARD>', methods=['GET'])
class NSWOpalTrainLinePeriodCardType(Resource):
    @api.response(200, 'SUCCESSFUL: Contents successfully loaded')
    @api.response(204, 'NO CONTENT: No content in database')
    @api.doc(description='Retrieving all records from the database selected TRAIN LINE and PERIOD or CARD.')
    def get(self, TRAIN_LINE, PERIOD_or_CARD):
        db = get_db()
        details_cur = db.execute(
            'select TRAIN_LINE, PERIOD, CARD_TYPE, COUNT from NSW_OPAL_CARD_TYPE_JULY_2016_APRIL_2019 where (TRAIN_LINE like ?) AND (PERIOD = ? OR CARD_TYPE = ?) COLLATE NOCASE',
            ["%" + TRAIN_LINE + "%", PERIOD_or_CARD, PERIOD_or_CARD])
        details = details_cur.fetchall()

        return_values = []

        for detail in details:
            detail_dict = {}
            detail_dict['TRAIN_LINE'] = detail['TRAIN_LINE']
            detail_dict['CARD_TYPE'] = detail['CARD_TYPE']
            detail_dict['PERIOD'] = detail['PERIOD']
            detail_dict['COUNT'] = detail['COUNT']

            return_values.append(detail_dict)

        return make_response(jsonify(return_values), 200)


if __name__ == '__main__':
    app.run()
