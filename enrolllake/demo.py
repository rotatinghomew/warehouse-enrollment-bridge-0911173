from .handoff import (begin_snapshot, receive_snapshot_shard,
                      receive_transaction_piece)
from .lake import Lake
from .storage import fingerprint


def _shard(snapshot_id, shard_id, rows):
    document = {'snapshot_id': snapshot_id, 'shard_id': shard_id, 'rows': rows}
    document['checksum'] = fingerprint(
        {key: value for key, value in document.items() if key != 'checksum'})
    return document


def _manifest(snapshot_id, cut, shards):
    return {'snapshot_id': snapshot_id, 'cut': cut,
            'shards': [{'shard_id': shard['shard_id'],
                        'checksum': shard['checksum']} for shard in shards]}


def _piece(transaction_id, position, changes):
    return {'transaction_id': transaction_id, 'position': position,
            'piece_no': 1, 'piece_count': 1,
            'final_checksum': fingerprint(
                {'transaction_id': transaction_id, 'position': position,
                 'changes': changes}),
            'changes': changes}


def _active(enrollment_id, region='东', school='S1', status='active'):
    return {'enrollment_id': enrollment_id, 'region': region,
            'school': school, 'status': status}


def run(root):
    lake = Lake(root)
    story = {}
    try:
        snapshot_id, cut = 'S2026', 20
        shards = [
            _shard(snapshot_id, 'p1', [_active('A'), _active('B', region='西')]),
            _shard(snapshot_id, 'p2', [_active('C')]),
            _shard(snapshot_id, 'p3', [])]
        begin_snapshot(lake.db, _manifest(snapshot_id, cut, shards))

        # 变更通道独立先到：切点以内事务（将被快照取代）与切点以后事务并存
        lake.receive_transaction_piece(
            _piece('T19', 19, [{'op': 'delete', 'enrollment_id': 'A'}]))
        lake.receive_transaction_piece(
            _piece('T21', 21, [{'op': 'upsert',
                                'row': _active('D', region='西')}]))
        story['交接等待中'] = lake.ingestion_status()

        # 快照通道乱序、重复、坏片补传
        lake.receive_snapshot_shard(shards[2])
        corrupt = dict(shards[0], checksum='0' * 64)
        try:
            lake.receive_snapshot_shard(corrupt)
        except ValueError as problem:
            story['坏片被拒'] = str(problem)
        lake.receive_snapshot_shard(shards[1])
        lake.receive_snapshot_shard(shards[0])
        lake.receive_snapshot_shard(shards[0])  # 重复，幂等忽略
        story['切点切换后'] = lake.ingestion_status()
        cut_generation = lake.publish()
        story['切点事实'] = lake.read(cut_generation)

        # 交接结束后继续普通增量
        lake.receive_transaction_piece(
            _piece('T22', 22, [{'op': 'delete', 'enrollment_id': 'B'}]))
        generation = lake.publish()
        story['增量后发布'] = lake.read(generation)
        story['最终接入状态'] = lake.ingestion_status()
        return story
    finally:
        lake.close()
