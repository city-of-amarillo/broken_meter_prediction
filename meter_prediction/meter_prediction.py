import csv
import sqlite3
import parse_cfg
from sqlite3 import *

"""
    Class: MeterPrediction
    Purpose: This will run predictive analysis on failing meters
    Author: Chris Heller
    Date: 10.23.19
"""


class MeterPrediction:
    dbs = None
    pc = parse_cfg.ParseConfig

    def __init__(self):
        self.create_connection()
        self.create_meter_table()

    @staticmethod
    def create_connection():
        if MeterPrediction.pc.dbfile is not None:
            MeterPrediction.dbs = sqlite3.connect(MeterPrediction.pc.dbfile)
        else:
            MeterPrediction.dbs = sqlite3.connect(":memory:")
        return MeterPrediction.dbs

    @staticmethod
    def create_meter_table(sql_create_meter_table=None):
        try:
            curs = MeterPrediction.dbs.cursor()
            curs.execute(sql_create_meter_table())
        except Error as e:
            return e

    @staticmethod
    def sql_create_meter_table():
        return '''CREATE TABLE IF NOT EXISTS ''' + str(MeterPrediction.pc.table_db) + '''.''' + str(
            MeterPrediction.pc.table_schema) + \
               '''.''' + str(MeterPrediction.pc.table_name) + '''(''' + str(
            MeterPrediction.pc.fields_collection()) + ''');'''

    @staticmethod
    def import_data():
        with open('meter_data.csv', 'rb') as fin:
            dr = csv.DictReader(fin)
            to_db = [(i['acctno'], i['custno'], i['mtrno'], i['mtrsz'], i['mtrusg'], i['readdt']) for i in dr]

        curs = MeterPrediction.dbs.cursor()
        curs.executemany("INSERT INTO meter_data (acctno, custno, mtrno, mtrsz, mtrusg, readdt "
                         "VALUES(?,?,?,?,?,?);", to_db)
        MeterPrediction.dbs.commit()


def truncate_meter_table():
    return """TRUNCATE TABLE meter_data;"""
