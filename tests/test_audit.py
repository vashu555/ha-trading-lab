import ast
from pathlib import Path
import threading
import types
import unittest

tree=ast.parse((Path(__file__).resolve().parents[1] / 'app/control.py').read_text(encoding='utf-8'))
function=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='audit_once')


class LiveAudit(unittest.TestCase):
    def run_check(self,values):
        queries=[]
        class Connection:
            def __enter__(self): return self
            def __exit__(self,*args): pass
            def execute(self,sql,params=None):
                queries.append(sql)
                if 'outbox WHERE published_at IS NULL' in sql:
                    return types.SimpleNamespace(fetchone=lambda:(2,.1))
                if 'LEFT JOIN notifications' in sql:
                    return types.SimpleNamespace(fetchone=lambda:(10,10))
                raise AssertionError('Historical scan ran in the live audit: '+sql)
        times=iter([1000.,1005.])
        env={'time':types.SimpleNamespace(time=lambda:next(times)),
             'LOCK':threading.Lock(),'AUDIT_CACHE_LOCK':threading.Lock(),
             'ACKED':{},'CONFIRMED':set(),'LOST':{},
             'POOL':types.SimpleNamespace(connection=Connection),
             'BOOKS':{'v':values,'t':900 if values else 0,'running':True,'last_error':''},
             'HISTORY':{'by_status':{'FILLED':100},'t':990},
             'XA':{'seq':0,'boot':1,'pending':[],'missing':set(),'fills_ok':0,'dup_base':0,'orders_base':0},
             'http_json':lambda *args,**kwargs:{'boot':1,'fills':[],'event_seq':0,'duplicate_refs':0,'orders':0}}
        exec(compile(ast.Module(body=[function],type_ignores=[]),'<audit_once>','exec'),env)
        return env['audit_once'](),queries

    def test_live_checks_complete_while_first_books_check_runs(self):
        result,queries=self.run_check({})
        self.assertIsNone(result['books_wrong'])
        self.assertTrue(result['books_running'])
        self.assertEqual(result['notify_missing'],0)
        self.assertEqual(len(queries),2)

    def test_last_completed_books_values_remain_available(self):
        result,_=self.run_check(dict(cash_wrong=0,positions_wrong=0,reserve_wrong=0,overfilled=0))
        self.assertEqual(result['books_wrong'],0)
        self.assertEqual(result['books_checked_at'],900)

    def test_timestamp_describes_completion_not_start(self):
        result,_=self.run_check({})
        self.assertEqual(result['started_at'],1000)
        self.assertEqual(result['t'],1005)
        self.assertEqual(result['duration_ms'],5000)


if __name__=='__main__': unittest.main()
