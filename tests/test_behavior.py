import tempfile
import unittest


class WorkspaceTest(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory()
        self.directory=self.temporary.name
        self.resources=[]

    def open(self, factory):
        resource=factory(self.directory)
        self.resources.append(resource)
        return resource

    def tearDown(self):
        for resource in reversed(self.resources):
            resource.close()
        self.temporary.cleanup()

from enrolllake.lake import Lake
from enrolllake.replica import import_snapshot, import_transaction, rows


def source_snapshot():
    return {'snapshot_id':'S1','cut':10,'rows':[
        {'enrollment_id':'A','region':'东','school':'S1','status':'active'},
        {'enrollment_id':'B','region':'东','school':'S2','status':'active'}]}


class Behavior(WorkspaceTest):
    def test_snapshot_replay(self):
        lake = self.open(Lake)
        self.assertTrue(import_snapshot(lake.db, source_snapshot()))
        self.assertFalse(import_snapshot(lake.db, source_snapshot()))
        self.assertEqual(len(rows(lake.db)), 2)
        with self.assertRaises(ValueError):
            import_snapshot(lake.db, dict(source_snapshot(), cut=11))

    def test_transaction_boundary(self):
        lake = self.open(Lake)
        import_snapshot(lake.db, source_snapshot())
        transaction = {'transaction_id':'T11','position':11,'changes':[{'op':'delete','enrollment_id':'A'}]}
        self.assertTrue(import_transaction(lake.db, transaction))
        self.assertFalse(import_transaction(lake.db, transaction))
        with self.assertRaises(ValueError):
            import_transaction(lake.db, dict(transaction, transaction_id='T13', position=13))
        self.assertEqual([row['enrollment_id'] for row in rows(lake.db)], ['B'])

    def test_failed_batch_not_visible(self):
        lake = self.open(Lake)
        import_snapshot(lake.db, source_snapshot())
        with self.assertRaises(ValueError):
            import_transaction(lake.db, {'transaction_id':'T11','position':11,'changes':[{'op':'delete','enrollment_id':'A'},{'op':'unknown'}]})
        self.assertEqual(len(rows(lake.db)), 2)
        self.assertEqual(lake.db.execute('SELECT position FROM source_state').fetchone()[0], 10)

    def test_parquet_generation_frozen(self):
        lake = self.open(Lake)
        import_snapshot(lake.db, source_snapshot())
        first = lake.publish()
        self.assertEqual(lake.read(first)['summary'][0]['people'], 2)
        import_transaction(lake.db, {'transaction_id':'T11','position':11,'changes':[{'op':'delete','enrollment_id':'A'}]})
        lake.publish()
        second = self.open(Lake)
        self.assertEqual(second.read()['summary'][0]['people'], 1)
        self.assertEqual(second.read(first)['position'], 10)
