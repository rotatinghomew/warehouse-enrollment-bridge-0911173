from .storage import fingerprint, require_text


def enrollment(row):
    for name in ('enrollment_id', 'region', 'school'):
        require_text(row[name], name)
    if row['status'] not in ('active', 'cancelled'):
        raise ValueError("未知报名状态")
    return {name: row[name] for name in ('enrollment_id','region','school','status')}


def snapshot(document):
    require_text(document['snapshot_id'], "快照标识")
    rows = [enrollment(row) for row in document['rows']]
    keys = [row['enrollment_id'] for row in rows]
    if len(keys) != len(set(keys)):
        raise ValueError("快照报名键重复")
    if not isinstance(document['cut'], int) or document['cut'] < 0:
        raise ValueError("无效源端切点")
    return rows, fingerprint(document)


def transaction(document):
    require_text(document['transaction_id'], "事务标识")
    if not isinstance(document['position'], int) or document['position'] < 1:
        raise ValueError("无效日志位置")
    changes = []
    for change in document['changes']:
        if change['op'] == 'upsert':
            changes.append(('upsert', enrollment(change['row'])))
        elif change['op'] == 'delete':
            changes.append(('delete', {'enrollment_id':require_text(change['enrollment_id'], '报名键')}))
        else:
            raise ValueError("未知变更类型")
    return changes, fingerprint(document)
