from .intake import snapshot, transaction
from .storage import atomic, records


def _no_active_handoff(database):
    if database.execute(
            "SELECT 1 FROM snapshot_handoff WHERE status='receiving' LIMIT 1").fetchone():
        raise ValueError("快照交接进行中，整体快照入口不能交错接入")


def import_snapshot(database, document):
    rows, checksum = snapshot(document)
    with atomic(database):
        _no_active_handoff(database)
        prior = database.execute("SELECT checksum FROM snapshots WHERE name=?", (document['snapshot_id'],)).fetchone()
        if prior:
            if prior[0] != checksum:
                raise ValueError("快照标识与内容冲突")
            return False
        database.execute("DELETE FROM enrollment")
        database.executemany("INSERT INTO enrollment VALUES(:enrollment_id,:region,:school,:status)", rows)
        database.execute("INSERT INTO snapshots VALUES(?,?,?)", (document['snapshot_id'], document['cut'], checksum))
        database.execute("UPDATE source_state SET position=? WHERE id=1", (document['cut'],))
    return True


def import_transaction(database, document):
    changes, checksum = transaction(document)
    with atomic(database):
        _no_active_handoff(database)
        previous = database.execute("SELECT checksum FROM transactions WHERE name=?", (document['transaction_id'],)).fetchone()
        if previous:
            if previous[0] != checksum:
                raise ValueError("事务标识与内容冲突")
            return False
        current = database.execute("SELECT position FROM source_state WHERE id=1").fetchone()[0]
        if document['position'] != current + 1:
            raise ValueError("事务日志位置不连续")
        for operation, row in changes:
            if operation == 'delete':
                database.execute("DELETE FROM enrollment WHERE enrollment_id=?", (row['enrollment_id'],))
            else:
                database.execute("INSERT OR REPLACE INTO enrollment VALUES(:enrollment_id,:region,:school,:status)", row)
        database.execute("INSERT INTO transactions VALUES(?,?,?)", (document['transaction_id'], document['position'], checksum))
        database.execute("UPDATE source_state SET position=? WHERE id=1", (document['position'],))
    return True


def rows(database):
    return records(database, "SELECT * FROM enrollment ORDER BY enrollment_id")
