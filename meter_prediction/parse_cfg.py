import json

"""
    Class: ParseConfig
    Purpose: This will load the JSON config file and parse the elements for configurable table 
    Author: Chris Heller
    Date: 10.23.19
"""


class ParseConfig:
    """Declare instance variables"""
    config = None
    dblang = None
    db = None
    dbfile = None
    table_name = None
    table_db = None
    table_schema = None
    table_fields = None
    table_pk = None
    table_indexes = None

    """Class constructor"""

    def __init__(self):
        self.load_config_file()
        self.parse_config_file()

    """Load json config file"""

    @staticmethod
    def load_config_file():
        with open('meter_config.json') as json_cfg_file:
            ParseConfig.config = json.load(json_cfg_file)
            return ParseConfig.config

    """Parse json config file"""

    @staticmethod
    def parse_config_file():
        ParseConfig.dblang = ParseConfig.config.get('database')['dblang']
        ParseConfig.db = ParseConfig.config.get('database')['db']
        ParseConfig.dbfile = ParseConfig.config.get('database')['dbfile']
        ParseConfig.table_name = ParseConfig.config.get('meterconsumptiontbl')['name']
        ParseConfig.table_db = ParseConfig.config.get('meterconsumptiontbl')['db']
        ParseConfig.table_schema = ParseConfig.config.get('meterconsumptiontbl')['schema']
        ParseConfig.table_pk = ParseConfig.config.get('meterconsumptiontbl')['primarykey']
        ParseConfig.table_indexes = ParseConfig.index_collection()
        ParseConfig.table_fields = ParseConfig.fields_collection()

    ''' Parse primary key and create statement'''

    @staticmethod
    def get_primary_key():
        if ParseConfig.table_pk is not None:
            pk_statement = '''CREATE UNIQUE INDEX pkidx ON ''' + str(ParseConfig.table_db) + '''.''' + str(
                ParseConfig.table_schema) + '''.''' \
                           + str(ParseConfig.table_name) + '''(''' + str(ParseConfig.table_pk) + ''');'''
            return pk_statement

    """Parse indexes from json config"""

    @staticmethod
    def index_collection():
        index_sql = '''CREATE INDEX idx ON ''' + str(ParseConfig.table_db) + '''.''' + str(
            ParseConfig.table_schema) + '''.''' \
                    + str(ParseConfig.table_name) + ''' ( fields );'''
        indexes_collection = ParseConfig.config['meterconsumptiontbl']['indexes']
        x = 0

        for indexes_json in indexes_collection.values():
            x += 1
            index_statement = index_sql.replace('idx', 'idx' + str(x)).replace('field', indexes_json)
            return index_statement

    """Parse fields from json config"""

    @staticmethod
    def fields_collection():
        fields_collection = ParseConfig.config['meterconsumptiontbl']['fields']
        fields = None

        for fields_json in fields_collection.values():
            fields += fields_json + str(', \n')

        fields_sql = fields[:-3]
        return fields_sql
