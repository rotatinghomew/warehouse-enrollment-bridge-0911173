import json
from pathlib import Path

from .parquet import query, write_rows
from .replica import rows
from .storage import atomic, connect

SCHEMA = """
CREATE TABLE IF NOT EXISTS enrollment(enrollment_id TEXT PRIMARY KEY,region TEXT,school TEXT,status TEXT);
CREATE TABLE IF NOT EXISTS snapshots(name TEXT PRIMARY KEY,cut INTEGER,checksum TEXT);
CREATE TABLE IF NOT EXISTS transactions(name TEXT PRIMARY KEY,position INTEGER UNIQUE,checksum TEXT);
CREATE TABLE IF NOT EXISTS source_state(id INTEGER PRIMARY KEY CHECK(id=1),position INTEGER);
INSERT OR IGNORE INTO source_state VALUES(1,0);
CREATE TABLE IF NOT EXISTS generations(id INTEGER PRIMARY KEY,path TEXT,position INTEGER,summary TEXT);
"""


class Lake:
    def __init__(self, root):
        self.root = Path(root)
        self.db = connect(root, SCHEMA)

    def close(self):
        self.db.close()

    def publish(self):
        with atomic(self.db):
            current_rows = rows(self.db)
            path = write_rows(self.root / 'facts', current_rows)
            summary = query([path], "SELECT region,COUNT(*) people FROM payload WHERE status='active' GROUP BY region ORDER BY region") if path else []
            position = self.db.execute("SELECT position FROM source_state").fetchone()[0]
            cursor = self.db.execute("INSERT INTO generations(path,position,summary) VALUES(?,?,?)", (path,position,json.dumps(summary)))
        return cursor.lastrowid

    def read(self, generation=None):
        row = self.db.execute("SELECT * FROM generations WHERE id=?", (generation,)).fetchone() if generation else self.db.execute("SELECT * FROM generations ORDER BY id DESC LIMIT 1").fetchone()
        if not row:
            return {'position':None,'rows':[],'summary':[]}
        return {'position':row['position'],'rows':query([row['path']], 'SELECT * FROM payload ORDER BY enrollment_id') if row['path'] else [],'summary':json.loads(row['summary'])}
