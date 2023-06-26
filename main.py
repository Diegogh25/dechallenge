from flask import Flask,jsonify,render_template
from loader import CSVLoader
from csv_names import tables
from credentials import server, database, username, password
from queries import query1,query2

app = Flask(__name__)
csv_loader = CSVLoader(server, database, username, password)

@app.route('/',methods=['GET'])
def index():
    return jsonify({'Mesagge':'Welcome'})

@app.route('/upload/<string:table_name>')
def upload(table_name):
    csv_loader.load_csv_to_database(tables[table_name], table_name)
    return jsonify({'message': f'Table {table_name} uploaded'})


@app.route('/show/dep-job')
def analysis_01():
    conn = csv_loader.create_connection()
    with conn.cursor() as cursor:
        sql = query1
        cursor.execute(sql)
        result = cursor.fetchall()
    conn.close()
    return render_template("analysis_01.html", result = result)

@app.route('/show/dep-hir')
def analysis_02():
    conn = csv_loader.create_connection()
    with conn.cursor() as cursor:
        sql = query2
        cursor.execute(sql)
        result = cursor.fetchall()
    conn.close()
    return render_template("analysis_02.html", result = result)

if __name__ == "__main__":
    app.run(debug = True)
