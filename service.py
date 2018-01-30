import os

from flask import *
import sqlite3

app = Flask(__name__)
app.config['SESSION_TYPE'] = 'memcached'
app.config['SECRET_KEY'] = 'my super secret key'


@app.route('/')
def show_entries():
    db = get_db()
    cur = db.execute('select * FROM streams name')
    entries = cur.fetchall()
    return render_template('show_credentials.html', entries=entries)


@app.route('/c', methods=['GET'])
def show_credentials():
    db = get_db()
    cur = db.execute('select * FROM credentials ORDER BY name')
    entries = cur.fetchall()
    return render_template('show_credentials.html', entries=entries)


@app.route('/c', methods=['POST'])
def add_credential():
    db = get_db()

    if request.form['name'].strip() == "":
        return printr("Please fill in the name")

    if request.form['access_key'].strip() == "":
        return printr("Please fill in the access_key")

    if request.form['secret_key'].strip() == "":
        return printr("Please fill in the access_key")

    db.execute('insert into credentials (name, access_key, secret_key) values (?, ?, ?)',
                 [request.form['name'], request.form['access_key'],  request.form['secret_key']])

    db.commit()
    flash('New credential was successfully added')
    return redirect(url_for('show_entries'))


def connect_db():
    """Connects to the specific database."""
    rv = sqlite3.connect(os.path.join(app.root_path, 'app.db'),)
    rv.row_factory = sqlite3.Row
    return rv


def get_db():
    """Opens a new database connection if there is none yet for the
    current application context.
    """
    if not hasattr(g, 'sqlite_db'):
        g.sqlite_db = connect_db()
    return g.sqlite_db


@app.teardown_appcontext
def close_db(error):
    """Closes the database again at the end of the request."""
    if hasattr(g, 'sqlite_db'):
        g.sqlite_db.close()


def printr(s):
    h = """<pre>""" + s + """</pre><br><br><a href="javascript:history.back()">Go Back</a>"""
    return Response(h, mimetype='text/html')


if __name__ == "__main__":
    app.debug = True
    app.run()
