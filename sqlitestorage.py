#!/usr/bin/python3
import sqlite3, threading
from datetime import datetime

DB_NAME = 'database.db'
FILE_NAME = 'data.txt'

class sqlite_storage:
    _conn, _c, _counter, commit_limit = None, None, 0, 0
    _counter_lock = threading.Lock()
    def __init__(self, db_name, commit_limit = 100):
        self._commit_limit = commit_limit
        self._conn = sqlite3.connect(db_name, detect_types=sqlite3.PARSE_DECLTYPES)
        self._c = self._conn.cursor()
        self._c.execute('''CREATE TABLE IF NOT EXISTS pins
             (id INTEGER PRIMARY KEY,
              pin INTEGER UNIQUE NOT NULL,
              timestamp timestamp)''')
        self._c.execute('CREATE UNIQUE INDEX IF NOT EXISTS pin_index on pins (pin)')
    def exists(self, pin):
        t = (pin, )
        self._c.execute('SELECT * FROM pins where pin=?', t)
        if self._c.fetchone() is not None: return True
        return False
    def add(self, pin):
        t = (pin, datetime.now())
        self._c.execute('INSERT INTO pins VALUES (NULL, ?, ?)', t)
        with self._counter_lock: 
            self._counter += 1
            if self._counter >= self._commit_limit: 
                self._conn.commit()
                self._counter = 0
    def __del__(self):
        self._conn.commit()
        self._conn.close()

p = sqlite_storage(DB_NAME)
for line in open(FIEL_NAME):
    p.add(line.strip())
