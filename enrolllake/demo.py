from .lake import Lake
from .replica import import_snapshot, import_transaction


def run(root):
    lake = Lake(root)
    try:
        import_snapshot(lake.db, {'snapshot_id':'S1','cut':10,'rows':[
            {'enrollment_id':'A','region':'东','school':'S1','status':'active'},
            {'enrollment_id':'B','region':'西','school':'S2','status':'active'}]})
        before = lake.publish()
        import_transaction(lake.db, {'transaction_id':'T11','position':11,'changes':[{'op':'delete','enrollment_id':'A'}]})
        after = lake.publish()
        return {'初次入湖':lake.read(before),'事务后':lake.read(after)}
    finally:
        lake.close()
