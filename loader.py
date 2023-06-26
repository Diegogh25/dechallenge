import pyodbc
import csv
# import configparser

class CSVLoader:
    def __init__(self, server, database, username, password):
        # config_connections = configparser.ConfigParser()
        # config_connections.read('credentials.ini')
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.connection = self.create_connection()

    def create_connection(self):
        conn_str = f"Trusted_Connection=Yes;DRIVER={{SQL Server}};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password}"
        return pyodbc.connect(conn_str)

    def get_schema(self, table_name):
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('{table_name}')")
        return [column[0] for column in cursor.fetchall()]

    def load_csv_to_database(self,csv_file,table_name):
        cursor = self.connection.cursor()
        with open(csv_file, 'r') as file:
            csv_reader = csv.reader(file)
            ls_culumns = self.get_schema(table_name)
            column_names = ', '.join(ls_culumns)
            placeholders = ', '.join('?' * len(ls_culumns))

            insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
            for row in csv_reader:
                cursor.execute(insert_query, row)

            self.connection.commit()
            cursor.close()

