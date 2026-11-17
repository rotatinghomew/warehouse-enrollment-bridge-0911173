from .storage import fingerprint, require_text


def enrollment(row):
    for name in ('enrollment_id', 'region', 'school'):
        require_text(row[name], name)
    if row['status'] not in ('active', 'cancelled'):
        raise ValueError("未知报名状态")
    return {name: row[name] for name in ('enrollment_id','region','school','status')}


def change(document):
    if document['op'] == 'upsert':
        return {'op': 'upsert', 'row': enrollment(document['row'])}
    if document['op'] == 'delete':
        return {'op': 'delete',
                'enrollment_id': require_text(document['enrollment_id'], '报名键')}
    raise ValueError("未知变更类型")


def snapshot(document):
    require_text(document['snapshot_id'], "快照标识")
    rows = [enrollment(row) for row in document['rows']]
    keys = [row['enrollment_id'] for row in rows]
    if len(keys) != len(set(keys)):
        raise ValueError("快照报名键重复")
    if not isinstance(document['cut'], int) or document['cut'] < 0:
        raise ValueError("无效源端切点")
    return rows, fingerprint(document)


def snapshot_manifest(document):
    """校验源端快照清单：快照标识、切点和全部分片标识/摘要。"""
    require_text(document['snapshot_id'], "快照标识")
    if not isinstance(document['cut'], int) or document['cut'] < 0:
        raise ValueError("无效源端切点")
    declared = document['shards']
    if not isinstance(declared, list) or not declared:
        raise ValueError("快照清单缺少分片")
    shards = []
    for entry in declared:
        shards.append({'shard_id': require_text(entry['shard_id'], '分片标识'),
                       'checksum': require_text(entry['checksum'], '分片摘要')})
    if len({entry['shard_id'] for entry in shards}) != len(shards):
        raise ValueError("清单分片标识重复")
    return ({'snapshot_id': document['snapshot_id'], 'cut': document['cut'],
             'shards': shards}, fingerprint(document))


def _without_checksum(document):
    return {name: value for name, value in document.items() if name != 'checksum'}


def snapshot_shard(document):
    """校验一个快照分片；摘要约定为去掉checksum字段后的规范SHA256。"""
    snapshot_id = require_text(document['snapshot_id'], "快照标识")
    shard_id = require_text(document['shard_id'], "分片标识")
    require_text(document['checksum'], "分片摘要")
    if not isinstance(document.get('rows'), list):
        raise ValueError("分片缺少报名行")
    rows = [enrollment(row) for row in document['rows']]
    keys = [row['enrollment_id'] for row in rows]
    if len(keys) != len(set(keys)):
        raise ValueError("分片内报名键重复")
    digest = fingerprint(_without_checksum(document))
    return ({'snapshot_id': snapshot_id, 'shard_id': shard_id,
             'checksum': document['checksum'], 'rows': rows}, digest)


def transaction(document):
    require_text(document['transaction_id'], "事务标识")
    if not isinstance(document['position'], int) or document['position'] < 1:
        raise ValueError("无效日志位置")
    normalized = [change(item) for item in document['changes']]
    changes = [(item['op'],
                item.get('row') or {'enrollment_id': item['enrollment_id']})
               for item in normalized]
    return changes, fingerprint(document)


def transaction_digest(transaction_id, position, changes):
    """组装后的事务摘要，与完整transaction入口的指纹口径一致。"""
    return fingerprint({'transaction_id': transaction_id,
                        'position': position, 'changes': changes})


def transaction_piece(document):
    """校验一个事务传输片（含事务边界、片序号/片数和最终摘要）。"""
    transaction_id = require_text(document['transaction_id'], "事务标识")
    position = document['position']
    if not isinstance(position, int) or position < 1:
        raise ValueError("无效日志位置")
    piece_no = document['piece_no']
    piece_count = document['piece_count']
    if not isinstance(piece_no, int) or piece_no < 1:
        raise ValueError("无效事务片序号")
    if not isinstance(piece_count, int) or piece_count < 1 or piece_no > piece_count:
        raise ValueError("无效事务片数")
    final_checksum = require_text(document['final_checksum'], "事务最终摘要")
    if not isinstance(document.get('changes'), list):
        raise ValueError("事务片缺少变更")
    changes = [change(item) for item in document['changes']]
    return {'transaction_id': transaction_id, 'position': position,
            'piece_no': piece_no, 'piece_count': piece_count,
            'final_checksum': final_checksum, 'changes': changes}
