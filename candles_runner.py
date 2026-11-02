# GitHub-Actions runner: Dukascopy HOURLY candles (bid + ask), one symbol, 2003..2026 -> parquet on branch data-candles.
# Prices are stored as raw integers (scale decided later by matching broker prices). Research only.
import urllib.request, lzma, os, sys, time, subprocess, shutil
import numpy as np, pandas as pd
from concurrent.futures import ThreadPoolExecutor
SYM=sys.argv[1]; Y0=int(sys.argv[2]) if len(sys.argv)>2 else 2003; Y1=2026
def fetch(url,tries=10):
    for k in range(tries):
        try:
            req=urllib.request.Request(url,headers={'User-Agent':'Mozilla/5.0'})
            with urllib.request.urlopen(req,timeout=60) as r: return r.read()
        except urllib.error.HTTPError as e:
            if e.code==404: return b''
            time.sleep(5+10*k)
        except Exception: time.sleep(5+10*k)
    return None
DT=np.dtype([('t','>u4'),('o','>u4'),('c','>u4'),('l','>u4'),('h','>u4'),('v','>f4')])
def month(ym):
    y,m=ym; out={}
    for side in ('BID','ASK'):
        raw=fetch(f'https://datafeed.dukascopy.com/datafeed/{SYM}/{y}/{m-1:02d}/{side}_candles_hour_1.bi5')
        if raw is None: return ym,'FAILED',None
        if not raw: return ym,'none',None
        a=np.frombuffer(lzma.decompress(raw),dtype=DT)
        base=int(pd.Timestamp(y,m,1,tz='UTC').timestamp())
        out[side]=pd.DataFrame({'t':base+a['t'].astype(np.int64),'o':a['o'],'h':a['h'],'l':a['l'],'c':a['c'],'v':a['v']})
    b,a=out['BID'],out['ASK']
    d=b.merge(a,on='t',suffixes=('','_a'))
    return ym,'ok',d
ms=[(y,m) for y in range(Y0,Y1+1) for m in range(1,13) if (y,m)<=(2026,10)]
old=None
try:
    subprocess.run('git fetch -q origin data-candles',shell=True,check=True)
    raw=subprocess.run(f'git show origin/data-candles:candles/{SYM}_H1.parquet',shell=True,capture_output=True).stdout
    if raw:
        import io; old=pd.read_parquet(io.BytesIO(raw))
        have=set(zip(pd.to_datetime(old.t,unit='s').dt.year,pd.to_datetime(old.t,unit='s').dt.month))
        ms=[ym for ym in ms if ym not in have]; print(SYM,'resume: months still missing',len(ms),flush=True)
except Exception as e: print('no existing file',e,flush=True)
def push(D):
    os.makedirs('/tmp/out',exist_ok=True); fn=f'/tmp/out/{SYM}_H1.parquet'; D.to_parquet(fn,compression='zstd',index=False)
    for i in range(10):
        try:
            subprocess.run('git fetch -q origin data-candles && git checkout -q -B data-candles origin/data-candles || (git checkout -q --orphan data-candles && git rm -rfq . >/dev/null 2>&1 || true)',shell=True,check=True)
            os.makedirs('candles',exist_ok=True); shutil.copy(fn,'candles/')
            subprocess.run(f'git add candles && git commit -qm "candles {SYM}" && git push -q origin data-candles',shell=True,check=True)
            print('pushed',SYM,'rows',len(D),flush=True); return True
        except subprocess.CalledProcessError as e:
            print('push retry',i,e,flush=True); time.sleep(10+i*7)
    return False
# download in chunks, push after every chunk, stop before the job timeout (partial progress is kept; the hourly cron resumes)
T0=time.time(); BUDGET=float(os.environ.get('BUDGET_MIN','100'))*60; CH=12
res=[]; cur=old; new_ok=0
for k in range(0,len(ms),CH):
    if time.time()-T0>BUDGET: print(SYM,'time budget reached, months left',len(ms)-k,flush=True); break
    with ThreadPoolExecutor(max_workers=2) as ex: r=list(ex.map(month,ms[k:k+CH]))
    res+=r; parts=[d for _,s,d in r if s=='ok']
    if parts:
        new_ok+=len(parts)
        if cur is not None: parts.append(cur)
        cur=pd.concat(parts).sort_values('t').drop_duplicates('t')
        cur=cur[cur.v>0]   # Dukascopy writes flat zero-volume hours for closed market
        push(cur)
st={}
for ym,s,_ in res: st[s]=st.get(s,0)+1
print(SYM,'months',st,flush=True)
failed=[ym for ym,s_,_ in res if s_=='FAILED']; print(SYM,'FAILED months',failed,flush=True)
if cur is None: print('NO DATA',SYM); sys.exit(0)
if not new_ok: print('nothing new'); sys.exit(0)
D=cur; bad=(D.h<np.maximum(D.o,D.c))|(D.l>np.minimum(D.o,D.c))
print(SYM,'rows',len(D),'first',pd.to_datetime(D.t.iloc[0],unit='s'),'last',pd.to_datetime(D.t.iloc[-1],unit='s'),'bad OHLC',int(bad.sum()),'median raw close',int(D.c.median()),'median raw spread',float((D.c_a-D.c).median()),flush=True)
