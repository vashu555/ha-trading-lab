"""Start a short simulated-user test, verify unique accounts, and stop it."""
import argparse
from datetime import datetime,timezone
import json
from pathlib import Path
import time
import urllib.parse
import urllib.request

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--users',type=int,default=100)
parser.add_argument('--spawn-rate',type=float,default=10)
parser.add_argument('--locust',default='http://127.0.0.1:8089')
parser.add_argument('--dashboard',default='http://127.0.0.1:9700')
args=parser.parse_args()
if not 1<=args.users<=160 or args.spawn_rate<=0:
    parser.error('Use 1–160 users and a positive spawn rate')


def api(base,path,body=None):
    request=urllib.request.Request(base.rstrip('/')+path,data=None if body is None else urllib.parse.urlencode(body).encode())
    with urllib.request.urlopen(request,timeout=15) as response:
        return json.load(response)


if api(args.locust,'/stats/requests')['user_count']!=0:
    raise SystemExit('Another Locust test is running; leave it untouched')
if api(args.dashboard,'/api/state')['traders']['n']!=0:
    raise SystemExit('Set internal dashboard traders to 0 before testing')
if api(args.locust,'/lab/accounts')['assigned']!=0:
    raise SystemExit('Account leases remain allocated; inspect the stopped Locust workers')
out=Path(__file__).resolve().parents[1]/'docs/testing'/datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%SZ-account-check')
out.mkdir(parents=True,exist_ok=False)
try:
    started=api(args.locust,'/swarm',{'user_count':args.users,'spawn_rate':args.spawn_rate})
    if started.get('success') is False: raise RuntimeError(started)
    deadline=time.monotonic()+args.users/args.spawn_rate+40
    while time.monotonic()<deadline:
        stats=api(args.locust,'/stats/requests')
        accounts=api(args.locust,'/lab/accounts')
        if stats['user_count']==args.users and accounts['assigned']==args.users:
            break
        time.sleep(1)
    else:
        raise RuntimeError('Requested user/account counts were not reached')
    if len({a['account'] for a in accounts['accounts']})!=args.users:
        raise AssertionError('Duplicate active accounts found')
    time.sleep(3)
    stats=api(args.locust,'/stats/requests')
    if any('trader-' in r['name'] for r in stats.get('stats',[])):
        raise AssertionError('Request statistics were split by account')
    (out/'accounts.json').write_text(json.dumps(accounts,indent=2),encoding='utf-8')
    (out/'stats.json').write_text(json.dumps(stats,indent=2),encoding='utf-8')
    print(f'PASS: {args.users} users, {args.users} distinct active accounts; evidence: {out}')
finally:
    api(args.locust,'/stop')
    deadline=time.monotonic()+25
    while time.monotonic()<deadline:
        if api(args.locust,'/stats/requests')['user_count']==0 and api(args.locust,'/lab/accounts')['assigned']==0:
            break
        time.sleep(.5)
    else:
        raise RuntimeError('Not all users/accounts stopped; inspect Locust')
