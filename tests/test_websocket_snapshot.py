import ast
import base64
import hashlib
import json
from pathlib import Path
import threading
import time
import types
import unittest

tree=ast.parse((Path(__file__).resolve().parents[1] / 'app/api.py').read_text(encoding='utf-8'))
handler=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='Handler')
function=next(n for n in handler.body if isinstance(n,ast.FunctionDef) and n.name=='prices_stream')


class ImmediateSnapshot(unittest.TestCase):
    def stream(self,prices):
        draining=threading.Event()
        def forbidden_wait(*args,**kwargs):
            raise AssertionError('The initial snapshot waited for another tick')
        class Condition:
            def __enter__(self): return self
            def __exit__(self,*args): pass
            wait=forbidden_wait
        counter=types.SimpleNamespace(add=lambda *args:None)
        env={'base64':base64,'hashlib':hashlib,'WS_GUID':'258EAFA5-E914-47DA-95CA-C5AB0DC85B11',
             'WS':{'open':0},'C':counter,'DRAINING':draining,'TICK_COND':Condition(),
             'select':types.SimpleNamespace(select=lambda *args:([],[],[])),
             'WS_CONFLATE':.1,'time':time,'PRICES':prices,'SERVICE':'api-test',
             'json':json,'ws_frame':lambda p:p}
        exec(compile(ast.Module(body=[function],type_ignores=[]),'<prices_stream>','exec'),env)
        messages=[]
        def write(payload):
            messages.append(json.loads(payload))
            draining.set()
        fake=types.SimpleNamespace(headers={'Sec-WebSocket-Key':'test','Upgrade':'websocket'},
             send_response=lambda *args:None,send_header=lambda *args:None,end_headers=lambda:None,
             connection=object(),wfile=types.SimpleNamespace(write=write),reply=lambda *args:self.fail('Upgrade failed'))
        env['prices_stream'](fake)
        self.assertEqual(env['WS']['open'],0)
        return messages

    def test_cached_prices_sent_before_waiting(self):
        messages=self.stream({'TCS':(4050,time.time(),(123,7),time.time())})
        self.assertEqual(messages[0]['type'],'ticks')
        self.assertEqual(messages[0]['ticks'][0]['price'],4050)

    def test_empty_cache_sends_heartbeat_without_waiting_or_spinning(self):
        self.assertEqual(self.stream({})[0]['type'],'heartbeat')


if __name__=='__main__': unittest.main()
