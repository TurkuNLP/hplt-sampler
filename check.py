import json,os
plan='eng_Latn_plan.jsonl'
samples='samples'
if not os.path.exists(plan):
    print('Plan missing'); raise SystemExit(1)
tot_plan=0; tot_have=0
for ln in open(plan):
    j=json.loads(ln)
    fn=j['filename']; n=j['n_take']
    tot_plan+=n
    p=os.path.join(samples, fn+'.jsonl')
    if os.path.exists(p):
        cnt=sum(1 for _ in open(p))
        tot_have+=cnt
        if cnt!=n:
            print(fn, 'plan', n, 'have', cnt)
    else:
        print(fn, 'MISSING (plan', n, ')')
print('total plan', tot_plan, 'total found', tot_have)
