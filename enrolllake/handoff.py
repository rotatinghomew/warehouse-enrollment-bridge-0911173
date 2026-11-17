"""快照分片与事务片的连续交接入口。

设计要点：
- 快照分片先入暂存区，只有清单声明的分片全部到齐且各自校验通过，才在一个
  原子事务中替换可见副本；未闭合前下游发布仍读到上一份完整事实。
- 变更通道独立到达。切点以内（position<=cut）的已闭合事务记入held，切点
  应用时统一标记为superseded，绝不重放到副本；切点以后的事务按position
  顺序闭合应用，缺片或日志有缺口时停在最后一个连续位点，下游不丢不跳。
- 所有输入按供应方标识做幂等，重复文件不重复计入；校验失败的分片不入账，
  可原样补传，不重置已核实进度。
"""
import json
import sqlite3
from datetime import datetime, timezone

from .intake import (snapshot_manifest, snapshot_shard, transaction_digest,
                     transaction_piece)
from .storage import atomic


def _now():
    return datetime.now(timezone.utc).isoformat()


def _record_input(database, kind, ref_id, checksum, state, detail, file_id=None):
    database.execute(
        "INSERT INTO inputs(file_id,kind,ref_id,checksum,state,detail,received_at) "
        "VALUES(?,?,?,?,?,?,?)",
        (file_id, kind, ref_id, checksum, state, detail, _now()))


def begin_snapshot(database, document):
    """登记一次新的一致性快照交接（源端清单）。"""
    manifest, manifest_checksum = snapshot_manifest(document)
    with atomic(database):
        existing = database.execute(
            "SELECT manifest_checksum,status FROM snapshot_handoff WHERE snapshot_id=?",
            (manifest['snapshot_id'],)).fetchone()
        if existing:
            if existing[0] != manifest_checksum:
                raise ValueError("快照标识与清单内容冲突")
            return False
        if database.execute(
                "SELECT 1 FROM snapshot_handoff WHERE status='receiving' LIMIT 1").fetchone():
            raise ValueError("已有进行中的快照交接，不能交错")
        database.execute("INSERT INTO snapshot_handoff VALUES(?,?,?,?,?,?)",
                         (manifest['snapshot_id'], manifest_checksum,
                          manifest['cut'], len(manifest['shards']),
                          'receiving', None))
        database.executemany(
            "INSERT INTO snapshot_shard_state VALUES(?,?,?,0)",
            [(manifest['snapshot_id'], shard['shard_id'], shard['checksum'])
             for shard in manifest['shards']])
        return True


def _handoff(database, snapshot_id):
    row = database.execute(
        "SELECT * FROM snapshot_handoff WHERE snapshot_id=?",
        (snapshot_id,)).fetchone()
    if not row:
        raise ValueError("快照清单未登记，不能接收分片")
    return row


def receive_snapshot_shard(database, document):
    """接收并暂存一个快照分片，校验通过才入账；失败可补传。"""
    shard, actual_digest = snapshot_shard(document)
    with atomic(database):
        if shard['checksum'] != actual_digest:
            raise ValueError("分片校验未通过，请补传")
        handoff = _handoff(database, shard['snapshot_id'])
        declared = database.execute(
            "SELECT checksum,staged FROM snapshot_shard_state WHERE snapshot_id=? AND shard_id=?",
            (shard['snapshot_id'], shard['shard_id'])).fetchone()
        if not declared:
            raise ValueError("分片不在源端清单内")
        if declared['checksum'] != shard['checksum']:
            raise ValueError("分片摘要与清单不一致")
        if declared['staged']:
            return False
        if handoff['status'] != 'receiving':
            raise ValueError("该快照交接已完成，不能再接收分片")
        try:
            database.executemany(
                "INSERT INTO snapshot_rows VALUES(?,?,?,?,?,?)",
                [(shard['snapshot_id'], shard['shard_id'], row['enrollment_id'],
                  row['region'], row['school'], row['status'])
                 for row in shard['rows']])
        except sqlite3.IntegrityError:
            raise ValueError("跨分片报名键重复，快照不一致")
        database.execute(
            "UPDATE snapshot_shard_state SET staged=1,checksum=? WHERE snapshot_id=? AND shard_id=?",
            (shard['checksum'], shard['snapshot_id'], shard['shard_id']))
        _record_input(database, 'snapshot_shard',
                      f"{shard['snapshot_id']}/{shard['shard_id']}",
                      shard['checksum'], 'staged', f"{len(shard['rows'])}行",
                      document.get('file_id'))
        if handoff['status'] == 'receiving':
            _try_apply_snapshot(database, handoff)
        return True


def _try_apply_snapshot(database, handoff):
    """分片齐全时原子切换副本并追上切点之后的连续已闭合事务。"""
    snapshot_id = handoff['snapshot_id']
    missing = database.execute(
        "SELECT COUNT(*) FROM snapshot_shard_state WHERE snapshot_id=? AND staged=0",
        (snapshot_id,)).fetchone()[0]
    if missing:
        return False
    cut = handoff['cut']
    database.execute("DELETE FROM enrollment")
    database.execute(
        "INSERT INTO enrollment SELECT enrollment_id,region,school,status "
        "FROM snapshot_rows WHERE snapshot_id=?", (snapshot_id,))
    database.execute("DELETE FROM snapshots")
    database.execute("INSERT INTO snapshots VALUES(?,?,?)",
                     (snapshot_id, cut, handoff['manifest_checksum']))
    database.execute("UPDATE source_state SET position=? WHERE id=1", (cut,))
    database.execute(
        "UPDATE held_transactions SET state='superseded' WHERE position<=?", (cut,))
    database.execute(
        "DELETE FROM transaction_pieces WHERE transaction_id IN "
        "(SELECT transaction_id FROM held_transactions WHERE state='superseded')")
    database.execute("DELETE FROM snapshot_rows WHERE snapshot_id=?", (snapshot_id,))
    database.execute(
        "UPDATE snapshot_handoff SET status='applied',applied_at=? WHERE snapshot_id=?",
        (_now(), snapshot_id))
    _drain_transactions(database, cut)
    return True


def _assemble(database, transaction_id):
    """从传输片组装一个闭合事务，校验片序号、片数和最终摘要。"""
    pieces = database.execute(
        "SELECT * FROM transaction_pieces WHERE transaction_id=? ORDER BY piece_no",
        (transaction_id,)).fetchall()
    if not pieces:
        return None
    expected = pieces[0]['piece_count']
    if len(pieces) < expected:
        return None
    if [piece['piece_no'] for piece in pieces] != list(range(1, expected + 1)):
        raise ValueError("事务片序号不连续")
    if len({piece['final_checksum'] for piece in pieces}) != 1:
        raise ValueError("事务片最终摘要不一致")
    changes = []
    for piece in pieces:
        changes.extend(json.loads(piece['changes']))
    checksum = transaction_digest(transaction_id, pieces[0]['position'], changes)
    if checksum != pieces[0]['final_checksum']:
        raise ValueError("事务最终摘要不匹配，请补传")
    return pieces[0]['position'], changes, checksum


def _apply_transaction(database, transaction_id, position, changes, checksum):
    for item in changes:
        if item['op'] == 'delete':
            database.execute("DELETE FROM enrollment WHERE enrollment_id=?",
                             (item['enrollment_id'],))
        else:
            row = item['row']
            database.execute("INSERT OR REPLACE INTO enrollment VALUES(?,?,?,?)",
                             (row['enrollment_id'], row['region'],
                              row['school'], row['status']))
    database.execute("INSERT INTO transactions VALUES(?,?,?)",
                     (transaction_id, position, checksum))
    database.execute("UPDATE source_state SET position=? WHERE id=1", (position,))
    database.execute("DELETE FROM held_transactions WHERE transaction_id=?",
                     (transaction_id,))
    database.execute("DELETE FROM transaction_pieces WHERE transaction_id=?",
                     (transaction_id,))
    _record_input(database, 'transaction', transaction_id, checksum,
                  'applied', f"position={position}")


def _drain_transactions(database, base_position):
    """从base_position+1开始，按连续位点应用所有已闭合事务。"""
    current = base_position
    while True:
        held = database.execute(
            "SELECT transaction_id FROM held_transactions WHERE position=? AND state='pending'",
            (current + 1,)).fetchone()
        if not held:
            return current
        assembled = _assemble(database, held['transaction_id'])
        if assembled is None:
            return current
        position, changes, checksum = assembled
        _apply_transaction(database, held['transaction_id'],
                           position, changes, checksum)
        current = position


def receive_transaction_piece(database, document):
    """接收一个事务传输片，自动组装、排队，并在连续时向下游应用。"""
    piece = transaction_piece(document)
    with atomic(database):
        applied = database.execute(
            "SELECT checksum FROM transactions WHERE name=?",
            (piece['transaction_id'],)).fetchone()
        if applied:
            if applied[0] != piece['final_checksum']:
                raise ValueError("事务标识与内容冲突")
            return False
        handoff = database.execute(
            "SELECT * FROM snapshot_handoff WHERE status='receiving' LIMIT 1").fetchone()
        cut = handoff['cut'] if handoff else None
        held = database.execute(
            "SELECT * FROM held_transactions WHERE transaction_id=?",
            (piece['transaction_id'],)).fetchone()
        if held:
            if held['position'] != piece['position'] or held['checksum'] != piece['final_checksum']:
                raise ValueError("事务标识与内容冲突")
            if held['state'] == 'superseded':
                return False
        else:
            if cut is None and database.execute(
                    "SELECT 1 FROM transactions WHERE position=? LIMIT 1",
                    (piece['position'],)).fetchone():
                raise ValueError("事务日志位置与已应用事务冲突")
            try:
                database.execute("INSERT INTO held_transactions VALUES(?,?,?,?)",
                                 (piece['transaction_id'], piece['position'],
                                  piece['final_checksum'], 'pending'))
            except sqlite3.IntegrityError:
                raise ValueError("事务日志位置与已排队事务冲突")
        existing = database.execute(
            "SELECT 1 FROM transaction_pieces WHERE transaction_id=? AND piece_no=?",
            (piece['transaction_id'], piece['piece_no'])).fetchone()
        if existing:
            return False
        database.execute("INSERT INTO transaction_pieces VALUES(?,?,?,?,?,?)",
                         (piece['transaction_id'], piece['position'],
                          piece['piece_no'], piece['piece_count'],
                          piece['final_checksum'], json.dumps(piece['changes'])))
        _record_input(database, 'transaction_piece',
                      f"{piece['transaction_id']}#{piece['piece_no']}",
                      piece['final_checksum'], 'held',
                      f"position={piece['position']}", document.get('file_id'))
        assembled = _assemble(database, piece['transaction_id'])
        if assembled is None:
            return True
        position, _changes, _checksum = assembled
        if cut is not None and position <= cut:
            return True
        # 交接进行中source_state仍是旧副本位点（通常为0），事务只能在其后连续应用
        base = database.execute(
            "SELECT position FROM source_state WHERE id=1").fetchone()[0]
        _drain_transactions(database, base)
        return True


def advance(database):
    """显式推进：补片或补传后尝试应用齐全的快照和连续事务。"""
    with atomic(database):
        handoff = database.execute(
            "SELECT * FROM snapshot_handoff WHERE status='receiving' LIMIT 1").fetchone()
        if handoff:
            _try_apply_snapshot(database, handoff)
        base = database.execute(
            "SELECT position FROM source_state WHERE id=1").fetchone()[0]
        return _drain_transactions(database, base)


def ingestion_status(database):
    """运营视图：当前可见位点、交接阶段以及仍在等待的输入。"""
    position = database.execute(
        "SELECT position FROM source_state WHERE id=1").fetchone()[0]
    handoff_row = database.execute(
        "SELECT * FROM snapshot_handoff ORDER BY rowid DESC LIMIT 1").fetchone()
    handoff = None
    if handoff_row:
        handoff = {'snapshot_id': handoff_row['snapshot_id'],
                   'cut': handoff_row['cut'],
                   'status': handoff_row['status'],
                   'total_shards': handoff_row['total_shards']}
        if handoff_row['status'] == 'receiving':
            handoff['staged_shards'] = database.execute(
                "SELECT COUNT(*) FROM snapshot_shard_state WHERE snapshot_id=? AND staged=1",
                (handoff_row['snapshot_id'],)).fetchone()[0]
            handoff['missing_shards'] = [
                row['shard_id'] for row in database.execute(
                    "SELECT shard_id FROM snapshot_shard_state WHERE snapshot_id=? AND staged=0 ORDER BY shard_id",
                    (handoff_row['snapshot_id'],))]
    open_transactions = []
    for row in database.execute(
            "SELECT h.transaction_id,h.position,h.state,"
            "(SELECT COUNT(*) FROM transaction_pieces p WHERE p.transaction_id=h.transaction_id) pieces,"
            "(SELECT piece_count FROM transaction_pieces p WHERE p.transaction_id=h.transaction_id LIMIT 1) total "
            "FROM held_transactions h WHERE h.state='pending' ORDER BY h.position"):
        open_transactions.append({'transaction_id': row['transaction_id'],
                                  'position': row['position'],
                                  'received_pieces': row['pieces'],
                                  'total_pieces': row['total']})
    next_position = position + 1
    gap = None
    if open_transactions:
        # 交接进行中，切点以内的等待事务将被快照取代，缺口只从cut+1算起
        floor = handoff_row['cut'] + 1 if handoff_row and handoff_row['status'] == 'receiving' else next_position
        first_waiting = open_transactions[0]['position']
        if first_waiting > floor:
            gap = {'from': floor, 'to': first_waiting - 1}
    return {'visible_position': position,
            'next_expected_position': next_position,
            'snapshot_handoff': handoff,
            'waiting_transactions': open_transactions,
            'log_gap': gap,
            'superseded_transactions': database.execute(
                "SELECT COUNT(*) FROM held_transactions WHERE state='superseded'").fetchone()[0]}
