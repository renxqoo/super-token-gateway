import csv,json,itertools,math,sys
from collections import Counter,defaultdict
from contextlib import ExitStack
from pathlib import Path

base=Path(sys.argv[1])
a=json.loads(Path(str(base)+'.json').read_text())
start=a['accounting']['startEpochMs']
end=a['configuration']['seconds']
lanes=a['configuration']['poolSize']
cohorts=defaultdict(lambda:defaultdict(Counter))
msec=defaultdict(Counter)
completed=defaultdict(Counter)
def cohort(t):
    sec=t/1000
    return next((f'{a}-{b}s' for a,b in [(0,10),(10,20),(20,30),(30,40),(40,60)] if a<=sec<b),'drain')
def quant(h,p):
    target=max(1,math.ceil(sum(h.values())*p));seen=0
    for t,n in sorted(h.items()):
        seen+=n
        if seen>=target:return t
def info(h):
    n=sum(h.values())
    return {'n':n,'mean':round(sum(k*v for k,v in h.items())/n,2),'p50':quant(h,.5),'p95':quant(h,.95),'p99':quant(h,.99),'max':max(h)}
def read(stack,suffix):
    return csv.DictReader(stack.enter_context(Path(str(base)+suffix).open()),delimiter='\t')
with ExitStack() as stack:
    xs=read(stack,'.identities.tsv')
    stages=read(stack,'.stages.tsv')
    events=read(stack,'.events.tsv')
    for x in xs:
        seq=int(x['seq']);id=x['request_id'];offered=int(float(x['scheduled_epoch_ms'])-start)
        msec['offered'][offered]+=1
        if not x['complete_epoch_ms']:
            assert float(x['dispatched_epoch_ms'] or 0)==0
            msec['shed'][int(x['http_validated_epoch_ms'])-start]+=1
            continue
        def t(k):return int(x[k+'_epoch_ms'])-start
        stage=next(stages);e1=next(events);e2=next(events);assert id==stage['request_id']==e1['request_id']==e2['request_id'] and int(e1['revision'])==1 and int(e2['revision'])==2
        times={'send':t('dispatched'),'arrival':t('public_arrival'),'grant':t('grant_witness'),'upstream':t('upstream_send'),'feed':int(stage['first_feed_started_ms'])-start,'sign1':int(e1['signed_epoch_ms'])-start,'durable1':int(e1['durable_witness_epoch_ms'])-start,'outcome':t('evidence_verified'),'terminal':t('terminal_witness'),'http':t('http_validated'),'ack':t('last_ack_observed'),'finish':t('complete')}
        assert times['finish']==max(times['http'],times['ack'])
        for k,v in times.items():msec[k][v]+=1
        c=cohort(times['arrival'])
        for group in [c,'all']:
            last=None
            for k in ['send','arrival','grant','upstream','feed','sign1','durable1','outcome','terminal','http']:
                if last is not None:cohorts[group][last+'_'+k][times[k]-times[last]]+=1
                last=k
            for k,l in [('terminal','ack'),('http','finish'),('arrival','http')]:cohorts[group][k+'_'+l][times[l]-times[k]]+=1
            cohorts[group]['lane'+str(seq%lanes)+'_feed_sign1'][times['sign1']-times['feed']]+=1
            cohorts[group]['shard'+str((seq%1000)%2)+'_grant'][times['grant']-times['arrival']]+=1
        completed[cohort(times['finish'])][seq%lanes]+=1
    assert next(stages,None) is None and next(events,None) is None
out={'wallSeconds':end,'cohortByArrivalMs':{c:{k:info(h) for k,h in cats.items()} for c,cats in cohorts.items()},'completedByLane':{c:dict(v) for c,v in completed.items()},'cpuByRoleSeconds':{k:v['cpu_seconds'] for k,v in a['metrics']['cpu'].items()}}
received=max(msec['ack'])
cum=Counter();backlog={k:defaultdict(Counter) for k in ['driver','transport','public','beforeGrant','pluginPreFeed','computeFeed','progressWitness','handoff','outcomeWitness','afterSettle','externalACK']}
def cat(t):return cohort(t)
persec=defaultdict(Counter)
for name,h in msec.items():
    for t,n in h.items():
        if t>=0:persec[t//1000][name]+=n
for t in range(received+1):
    for k,h in msec.items():cum[k]+=h[t]
    q={q:cum[left]-cum[right] for q,left,right in [('driver','send','http'),('transport','send','arrival'),('public','arrival','http'),('beforeGrant','arrival','grant'),('pluginPreFeed','grant','feed'),('computeFeed','feed','sign1'),('progressWitness','sign1','durable1'),('handoff','durable1','outcome'),('outcomeWitness','outcome','terminal'),('afterSettle','terminal','http'),('externalACK','terminal','ack')]}
    for k,v in q.items():
        backlog[k][cat(t)][v]+=1
        persec[t//1000]['sum_'+k]+=v
        if msec['shed'][t]:backlog[k]['millisecondWithShed'][v]+=1
out['phaseBacklogRequests']={k:{c:info(h) for c,h in cats.items()} for k,cats in backlog.items()}
out['perSecond']=dict(persec)
out['shedFirstLastMs']=[min(msec['shed'],default=0),max(msec['shed'],default=0)]
print(json.dumps(out,sort_keys=True,indent=2))
