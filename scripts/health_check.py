"""Read-only lab readiness check. Does not start, stop, or change services."""
import argparse
import json
import time
import urllib.request

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--url',default='http://127.0.0.1:9700')
parser.add_argument('--wait',type=float,default=0,help='Seconds to wait for readiness')
args=parser.parse_args()
deadline=time.monotonic()+args.wait
while True:
    problems=[]
    try:
        with urllib.request.urlopen(args.url.rstrip('/')+'/api/state',timeout=10) as response:
            state=json.load(response)
        status=state.get('status',{})
        audit=state.get('audit',{})
        containers=status.get('containers',{})
        if not state.get('ready'): problems.append('Control bootstrap not ready')
        if len(containers)!=38: problems.append(f'Expected 38 lab containers, found {len(containers)}')
        problems += [f'{name}: {c.get("state")}' for name,c in containers.items() if c.get('state')!='running']
        members=(status.get('patroni') or {}).get('members',[])
        if len(members)!=3 or sum(m.get('role')=='leader' for m in members)!=1:
            problems.append('Expected one PostgreSQL leader and two replicas')
        for member in members:
            if member.get('role')!='leader' and member.get('state')!='streaming':
                problems.append(member['name']+' not streaming yet')
        if audit.get('ok') is not True: problems.append('Live audit not ready or failed')
        if audit.get('t') is None or state['t']-audit['t']>10: problems.append('Audit result is missing/stale')
        for field in ('books_wrong','sent_twice','fills_missing','notify_missing','overfilled'):
            if audit.get(field)!=0: problems.append(f'{field}: {audit.get(field)!r}')
        result={'passed':not problems,'problems':problems,'containers':len(containers),'postgres':members,
                'audit_duration_ms':audit.get('duration_ms'),'books_checked_at':audit.get('books_checked_at'),
                'books_running':audit.get('books_running')}
    except Exception as exc:
        result={'passed':False,'problems':[str(exc)]}
    if result['passed'] or time.monotonic()>=deadline:
        print(json.dumps(result,indent=2))
        raise SystemExit(0 if result['passed'] else 1)
    time.sleep(2)
