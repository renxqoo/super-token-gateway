import json,pathlib,collections,re
root=pathlib.Path(__file__).resolve().parent
p=json.loads(next(root.glob('*.cpuprofile')).read_text())
src=(root/'gateway-main.js.raw').read_text().splitlines()
modules=[];last='bundle/bootstrap'
for line in src:
 m=re.match(r'^// (\S+)',line)
 if m:last=m.group(1)
 modules.append(last)
nodes={n['id']:n for n in p['nodes']}
parent={c:n['id'] for n in p['nodes'] for c in n.get('children',[])}
def frames(i):
 result=[]
 while i in nodes:
  f=nodes[i]['callFrame'];line=f.get('lineNumber',-1)
  result.append((f.get('functionName',''),modules[line] if 'dist/gateway/main.js' in f.get('url','') and 0<=line<len(modules) else f.get('url',''),line+1))
  if i not in parent:break
  i=parent[i]
 return result

def category(i):
 chain=frames(i);name=chain[0][0];mods=[f[1] for f in chain];names=[f[0] for f in chain]
 if name in ('update','digest','Hash') and any(f[0]=='load' and f[1].endswith('plugin-host/src/supervisor.ts') for f in chain):return 'module_sha256'
 if name=='structuredClone':return 'package_snapshot_clone'
 if name in ('sign','verify'):
  return 'rpc_ed25519' if 'signed' in names else 'evidence_ed25519'
 if any('/zod/' in m for m in mods):return 'schema_validation'
 if name in ('stringify','parse') or any(m.endswith('kernel/src/canonical.ts') for m in mods) or (name in ('update','digest','Hash') and any(m.endswith('runtime/src/crypto.ts') or m.endswith('plugin-sdk/src/abi.ts') for m in mods)):return 'json_canonical_other_sha256'
 if any('node:http2' in m or m.endswith('runtime/src/h2.ts') for m in mods):return 'http2'
 if any(m.endswith('plugin-host/src/supervisor.ts') for m in mods):return 'plugin_ipc_supervision'
 if any(m.endswith('plugin-host/src/json-documents.ts') for m in mods):return 'plugin_json_documents'
 return 'other_runtime_application_unattributed'
counts=collections.Counter();wall=collections.Counter();leaves=collections.defaultdict(collections.Counter)
for i,t in zip(p['samples'],p['timeDeltas']):
 cat=category(i);counts[cat]+=1;wall[cat]+=t;leaves[cat][' <- '.join(':'.join(map(str,f)) for f in frames(i)[:5])]+=1
requests=2492;cpu_ms=20.42*1000/requests
result={'method':'Exclusive leaf samples classified by ancestry; sample-count proportions, not wall-time timeDeltas. Absolute ms are estimates scaled to observed process CPU. async/native gaps and profiler overhead limit precision. Historical 7.1 is a normalization, not a recovered historical measurement.','samples':len(p['samples']),'requests':requests,'observed_cpu_ms_per_request':cpu_ms,'wall_duration_seconds':(p['endTime']-p['startTime'])/1e6,'groups':[]}
for cat,n in counts.most_common():result['groups'].append({'category':cat,'samples':n,'sample_share':n/len(p['samples']),'estimated_profile_cpu_ms_per_request':cpu_ms*n/len(p['samples']),'normalized_historical_7_1_ms':7.1*n/len(p['samples']),'wall_attributed_seconds':wall[cat]/1e6,'top_stacks':leaves[cat].most_common(3)})
(root/'analysis.json').write_text(json.dumps(result,indent=2)+'\n')
for r in result['groups']:print(r['category'],r['samples'],round(r['sample_share']*100,2),round(r['estimated_profile_cpu_ms_per_request'],3),round(r['normalized_historical_7_1_ms'],3))
