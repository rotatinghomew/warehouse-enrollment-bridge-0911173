import tempfile
import unittest

from enrolllake.handoff import (advance, begin_snapshot, ingestion_status,
                                receive_snapshot_shard,
                                receive_transaction_piece)
from enrolllake.intake import transaction_digest
from enrolllake.lake import Lake
from enrolllake.replica import rows
from enrolllake.storage import fingerprint


class HandoffWorkspaceTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = self.temporary.name
        self.resources = []

    def open(self, factory=Lake):
        resource = factory(self.directory)
        self.resources.append(resource)
        return resource

    def tearDown(self):
        for resource in reversed(self.resources):
            resource.close()
        self.temporary.cleanup()


def shard_doc(snapshot_id, shard_id, shard_rows):
    document = {'snapshot_id': snapshot_id, 'shard_id': shard_id,
                'rows': shard_rows}
    document['checksum'] = fingerprint(
        {key: value for key, value in document.items() if key != 'checksum'})
    return document


def manifest(snapshot_id, cut, shards):
    return {'snapshot_id': snapshot_id, 'cut': cut,
            'shards': [{'shard_id': shard['shard_id'],
                        'checksum': shard['checksum']} for shard in shards]}


def piece_doc(transaction_id, position, changes, piece_no=1, piece_count=1,
              final_checksum=None):
    return {'transaction_id': transaction_id, 'position': position,
            'piece_no': piece_no, 'piece_count': piece_count,
            'final_checksum': (final_checksum if final_checksum is not None
                               else transaction_digest(transaction_id, position, changes)),
            'changes': changes}


def split_pieces(transaction_id, position, all_changes):
    """同一事务拆成多个传输片，所有片共享整事务的最终摘要。"""
    checksum = transaction_digest(transaction_id, position, all_changes)
    halfway = max(1, len(all_changes) // 2)
    return (piece_doc(transaction_id, position, all_changes[:halfway], 1, 2, checksum),
            piece_doc(transaction_id, position, all_changes[halfway:], 2, 2, checksum))


def active(enrollment_id, region='东', school='S1'):
    return {'enrollment_id': enrollment_id, 'region': region,
            'school': school, 'status': 'active'}


SNAPSHOT_ID = 'S2'
CUT = 10


def three_shards():
    return [shard_doc(SNAPSHOT_ID, 'p1', [active('A'), active('B')]),
            shard_doc(SNAPSHOT_ID, 'p2', [active('C')]),
            shard_doc(SNAPSHOT_ID, 'p3', [])]


class SnapshotHandoff(HandoffWorkspaceTest):
    def test_shards_accumulate_and_atomically_replace(self):
        lake = self.open()
        shards = three_shards()
        begin_snapshot(lake.db, manifest(SNAPSHOT_ID, CUT, shards))
        # 乱序到达：最后一片先到，缺片期间下游仍是空的上一份事实
        self.assertTrue(lake.receive_snapshot_shard(shards[2]))
        self.assertEqual(lake.ingestion_status()['snapshot_handoff']['missing_shards'],
                         ['p1', 'p2'])
        self.assertEqual(rows(lake.db), [])
        published = lake.publish()
        self.assertTrue(lake.receive_snapshot_shard(shards[1]))
        self.assertTrue(lake.receive_snapshot_shard(shards[0]))
        # 齐全后自动切换
        self.assertEqual([row['enrollment_id'] for row in rows(lake.db)],
                         ['A', 'B', 'C'])
        self.assertEqual(lake.db.execute(
            "SELECT position FROM source_state").fetchone()[0], CUT)
        self.assertEqual(lake.ingestion_status()['snapshot_handoff']['status'],
                         'applied')
        # 切换完成后补到的重复分片仍按供应方标识幂等忽略
        self.assertFalse(lake.receive_snapshot_shard(shards[0]))
        # 旧代次冻结在上一份事实，不被暂存过程污染
        self.assertEqual(lake.read(published)['rows'], [])

    def test_duplicate_shard_is_idempotent_and_corrupt_shard_can_be_retransmitted(self):
        lake = self.open()
        shards = three_shards()
        begin_snapshot(lake.db, manifest(SNAPSHOT_ID, CUT, shards))
        self.assertTrue(lake.receive_snapshot_shard(shards[0]))
        self.assertFalse(lake.receive_snapshot_shard(shards[0]))
        bad = dict(shards[1], checksum='0' * 64)
        with self.assertRaises(ValueError):
            lake.receive_snapshot_shard(bad)
        status = lake.ingestion_status()['snapshot_handoff']
        self.assertEqual(status['staged_shards'], 1)
        self.assertIn('p2', status['missing_shards'])
        # 补传正确分片，不重置p1
        self.assertTrue(lake.receive_snapshot_shard(shards[1]))
        self.assertTrue(lake.receive_snapshot_shard(shards[2]))
        self.assertEqual(lake.ingestion_status()['snapshot_handoff']['status'],
                         'applied')

    def test_cross_shard_duplicate_key_rejected_without_losing_progress(self):
        lake = self.open()
        shards = [shard_doc(SNAPSHOT_ID, 'p1', [active('A')]),
                  shard_doc(SNAPSHOT_ID, 'p2', [active('A')])]
        begin_snapshot(lake.db, manifest(SNAPSHOT_ID, CUT, shards))
        lake.receive_snapshot_shard(shards[0])
        with self.assertRaises(ValueError):
            lake.receive_snapshot_shard(shards[1])
        self.assertEqual(lake.ingestion_status()['snapshot_handoff']['staged_shards'], 1)

    def test_manifest_replay_and_conflict(self):
        lake = self.open()
        shards = three_shards()
        document = manifest(SNAPSHOT_ID, CUT, shards)
        self.assertTrue(begin_snapshot(lake.db, document))
        self.assertFalse(begin_snapshot(lake.db, document))
        with self.assertRaises(ValueError):
            begin_snapshot(lake.db, manifest(SNAPSHOT_ID, 11, shards))
        with self.assertRaises(ValueError):
            begin_snapshot(lake.db, manifest('S3', CUT, shards))


class TransactionChannel(HandoffWorkspaceTest):
    def begin(self, shards=None):
        lake = self.open()
        shards = shards or three_shards()
        begin_snapshot(lake.db, manifest(SNAPSHOT_ID, CUT, shards))
        return lake, shards

    def test_changes_before_cut_are_never_replayed(self):
        lake, shards = self.begin()
        # 变更通道先到：切点以内的事务即使闭合也先挂起
        piece = piece_doc('T9', 9, [{'op': 'delete', 'enrollment_id': 'A'}])
        self.assertTrue(lake.receive_transaction_piece(piece))
        self.assertEqual(lake.ingestion_status()['waiting_transactions'][0]['position'], 9)
        for shard in reversed(shards):
            lake.receive_snapshot_shard(shard)
        # 切换后T9被标记superseded，A仍在，位点停在cut
        self.assertEqual(
            lake.db.execute("SELECT state FROM held_transactions WHERE transaction_id='T9'").fetchone()[0],
            'superseded')
        self.assertIn('A', [row['enrollment_id'] for row in rows(lake.db)])
        self.assertEqual(lake.ingestion_status()['visible_position'], CUT)

    def test_changes_after_cut_apply_in_order_including_cross_piece_transaction(self):
        lake, shards = self.begin()
        # 跨片事务（两片乱序到达），position=11
        full_changes = [{'op': 'upsert', 'row': active('D')},
                        {'op': 'delete', 'enrollment_id': 'A'}]
        first, second = split_pieces('T11', 11, full_changes)
        lake.receive_transaction_piece(second)
        status = lake.ingestion_status()
        self.assertEqual(status['waiting_transactions'][0]['received_pieces'], 1)
        # 更早的位点12先到（完整），不能越过未闭合的11
        lake.receive_transaction_piece(piece_doc('T12', 12, [{'op': 'delete', 'enrollment_id': 'B'}]))
        for shard in shards:
            lake.receive_snapshot_shard(shard)
        keys = [row['enrollment_id'] for row in rows(lake.db)]
        self.assertEqual(keys, ['A', 'B', 'C'])
        lake.receive_transaction_piece(first)
        keys = [row['enrollment_id'] for row in rows(lake.db)]
        self.assertEqual(keys, ['C', 'D'])
        self.assertEqual(lake.ingestion_status()['visible_position'], 12)

    def test_log_gap_keeps_last_complete_fact_and_advances_after_fill(self):
        lake, shards = self.begin()
        for shard in shards:
            lake.receive_snapshot_shard(shard)
        # 11 缺失，12先到：不跳位应用，下游仍是cut处完整事实
        lake.receive_transaction_piece(piece_doc('T12', 12, [{'op': 'delete', 'enrollment_id': 'B'}]))
        self.assertEqual(lake.ingestion_status()['log_gap'], {'from': 11, 'to': 11})
        # 交接进行中，切点以内（cut+1之前）的缺口不展示为日志缺口
        with tempfile.TemporaryDirectory() as isolated:
            other = Lake(isolated)
            other_shards = [shard_doc('S9', 'p1', [active('A')])]
            begin_snapshot(other.db, manifest('S9', 10, other_shards))
            other.receive_transaction_piece(piece_doc('T13', 13, []))
            self.assertEqual(other.ingestion_status()['log_gap'],
                             {'from': 11, 'to': 12})
            other.close()
        self.assertEqual(lake.ingestion_status()['visible_position'], CUT)
        self.assertEqual(len(rows(lake.db)), 3)
        lake.receive_transaction_piece(piece_doc('T11', 11, [{'op': 'delete', 'enrollment_id': 'A'}]))
        self.assertEqual(lake.ingestion_status()['visible_position'], 12)
        self.assertEqual(
            [row['enrollment_id'] for row in rows(lake.db)], ['C'])

    def test_duplicate_pieces_and_replayed_transactions_are_idempotent(self):
        lake, shards = self.begin()
        for shard in shards:
            lake.receive_snapshot_shard(shard)
        piece = piece_doc('T11', 11, [{'op': 'delete', 'enrollment_id': 'A'}])
        self.assertTrue(lake.receive_transaction_piece(piece))
        self.assertFalse(lake.receive_transaction_piece(piece))
        self.assertEqual(lake.ingestion_status()['visible_position'], 11)
        # 同事务不同内容 -> 冲突
        with self.assertRaises(ValueError):
            lake.receive_transaction_piece(
                piece_doc('T11', 11, [{'op': 'delete', 'enrollment_id': 'B'}]))

    def test_bad_checksum_transaction_needs_retransmission(self):
        lake, shards = self.begin()
        for shard in shards:
            lake.receive_snapshot_shard(shard)
        bad = piece_doc('T11', 11, [{'op': 'delete', 'enrollment_id': 'A'}])
        bad['final_checksum'] = 'f' * 64
        with self.assertRaises(ValueError):
            lake.receive_transaction_piece(bad)
        self.assertEqual(lake.ingestion_status()['visible_position'], CUT)
        good = piece_doc('T11', 11, [{'op': 'delete', 'enrollment_id': 'A'}])
        lake.receive_transaction_piece(good)
        self.assertEqual(lake.ingestion_status()['visible_position'], 11)

    def test_published_generation_tracks_position_through_handoff(self):
        lake, shards = self.begin()
        before = lake.publish()
        for shard in shards:
            lake.receive_snapshot_shard(shard)
        at_cut = lake.publish()
        self.assertEqual(lake.read(at_cut)['position'], CUT)
        self.assertEqual(lake.read(before)['position'], 0)
        lake.receive_transaction_piece(piece_doc('T11', 11, [{'op': 'delete', 'enrollment_id': 'A'}]))
        lake.receive_transaction_piece(piece_doc('T12', 12, [{'op': 'delete', 'enrollment_id': 'B'}]))
        after = lake.publish()
        reading = lake.read(after)
        self.assertEqual(reading['position'], 12)
        self.assertEqual(reading['summary'], [{'region': '东', 'people': 1}])

    def test_state_survives_process_restart_and_advance(self):
        lake, shards = self.begin()
        lake.receive_snapshot_shard(shards[0])
        lake.receive_transaction_piece(
            piece_doc('T11', 11, [{'op': 'delete', 'enrollment_id': 'A'}]))
        lake.close()
        self.resources.remove(lake)
        reopened = self.open()
        status = reopened.ingestion_status()
        self.assertEqual(status['snapshot_handoff']['staged_shards'], 1)
        self.assertEqual(status['waiting_transactions'][0]['transaction_id'], 'T11')
        self.assertEqual(status['waiting_transactions'][0]['total_pieces'], 1)
        reopened.receive_snapshot_shard(shards[1])
        reopened.receive_snapshot_shard(shards[2])
        self.assertEqual(reopened.ingestion_status()['visible_position'], 11)
        # advance在无新输入时幂等
        self.assertEqual(advance(reopened.db), 11)

    def test_normal_incremental_continues_after_handoff(self):
        lake, shards = self.begin()
        for shard in shards:
            lake.receive_snapshot_shard(shard)
        for position, target in ((11, 'A'), (12, 'B'), (13, 'C')):
            lake.receive_transaction_piece(
                piece_doc(f'T{position}', position,
                          [{'op': 'delete', 'enrollment_id': target}]))
        self.assertEqual(lake.ingestion_status()['visible_position'], 13)
        self.assertEqual(rows(lake.db), [])
        status = lake.ingestion_status()
        self.assertIsNone(status['log_gap'])
        self.assertEqual(status['next_expected_position'], 14)
        self.assertEqual(status['waiting_transactions'], [])

    def test_legacy_entries_are_blocked_during_handoff(self):
        from enrolllake.replica import import_snapshot, import_transaction
        lake, _ = self.begin()
        with self.assertRaises(ValueError):
            import_snapshot(lake.db, {'snapshot_id': 'OLD', 'cut': 5, 'rows': []})
        with self.assertRaises(ValueError):
            import_transaction(lake.db, piece_doc('T1', 1, []))
