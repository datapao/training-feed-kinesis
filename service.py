import os

from flask import *
import sqlite3

app = Flask(__name__)

@app.route('/')
def show_entries():
    db = get_db()
    cur = db.execute('select * FROM streams ORDER BY started_at DESC, id')
    entries = cur.fetchall()
    return render_template('show_entries.html', entries=entries)

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
