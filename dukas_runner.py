# GitHub-Actions runner: Dukascopy XAUUSD ticks -> 1-second bars (bid OHLC, last ask, tick count) -> monthly parquet. Research only.
import urllib.request, lzma, struct, os, sys, time, datetime as dt
import numpy as np, pandas as pd
from concurrent.futures import ThreadPoolExecutor
SYM='XAUUSD'; SCALE=None; OUT=sys.argv[2] if len(sys.argv)>2 else 'out'; os.makedirs(OUT,exist_ok=True)
def fetch(url,tries=8):
    for k in range(tries):
        try:
            req=urllib.request.Request(url,headers={'User-Agent':'Mozilla/5.0'})
            with urllib.request.urlopen(req,timeout=60) as r: return r.read()
        except urllib.error.HTTPError as e:
            if e.code==404: return b''
            time.sleep(3+5*k)
        except Exception: time.sleep(3+5*k)
    return None
def hour_ticks(d,h):
    global SCALE
    raw=fetch(f'https://datafeed.dukascopy.com/datafeed/{SYM}/{d.year}/{d.month-1:02d}/{d.day:02d}/{h:02d}h_ticks.bi5')
    if raw is None: print('FAILED',d,h,flush=True); return []
    if len(raw)==0: return []
    try: data=lzma.decompress(raw)
    except Exception as e: print('decode error',d,h,e,flush=True); return []
    n=len(data)//20; base=int(dt.datetime(d.year,d.month,d.day,h,tzinfo=dt.timezone.utc).timestamp())*1000
    a=np.frombuffer(data[:n*20],dtype=np.dtype([('ms','>u4'),('ask','>u4'),('bid','>u4'),('av','>f4'),('bv','>f4')]))
    if SCALE is None and n: SCALE=next((sc for sc in (1000.0,100000.0,100.0) if 500<=a['ask'][0]/sc<=20000),1000.0); print('scale',SCALE,flush=True)
    return [(base+a['ms'].astype(np.int64),a['bid']/SCALE,a['ask']/SCALE)]
def month(y,m):
    d0=dt.date(y,m,1); d1=(d0.replace(day=28)+dt.timedelta(days=4)).replace(day=1)
    T=[];B=[];A=[]; d=d0
    while d<d1:
        if d.weekday()!=5:
            with ThreadPoolExecutor(max_workers=3) as ex: res=list(ex.map(lambda h: hour_ticks(d,h),range(24)))
            for r in res:
                for ms,bid,ask in r: T.append(ms);B.append(bid);A.append(ask)
        d+=dt.timedelta(days=1)
    if not T: print('no data',y,m); return
    ms=np.concatenate(T); bid=np.concatenate(B); ask=np.concatenate(A); o=np.argsort(ms,kind='stable'); ms,bid,ask=ms[o],bid[o],ask[o]
    sec=ms//1000; df=pd.DataFrame({'t':sec,'bid':bid,'ask':ask})
    g=df.groupby('t',sort=True); out=pd.DataFrame({'o':g.bid.first(),'h':g.bid.max(),'l':g.bid.min(),'c':g.bid.last(),'ask':g.ask.last(),'n':g.bid.size()}).reset_index()
    out['o']=out.o.astype('float32'); out['h']=out.h.astype('float32'); out['l']=out.l.astype('float32'); out['c']=out.c.astype('float32'); out['ask']=out.ask.astype('float32'); out['n']=out.n.astype('int32')
    out.to_parquet(f'{OUT}/{SYM}_1s_{y}{m:02d}.parquet',compression='zstd',index=False)
    print(y,m,'ticks',len(ms),'seconds',len(out),flush=True)
def push_month(fn):
    # commit one finished month to the data branch straight away (resumable, nothing lost on a job timeout)
    import subprocess
    for i in range(8):
        try:
            subprocess.run('git fetch -q origin data-dukas && git checkout -q -B data-dukas origin/data-dukas || (git checkout -q --orphan data-dukas && git rm -rfq . >/dev/null 2>&1 || true)',shell=True,check=True)
            os.makedirs('dukas',exist_ok=True); import shutil; shutil.copy(fn,'dukas/'); 
            subprocess.run(f'git add dukas && git commit -qm "dukas {os.path.basename(fn)}" && git push -q origin data-dukas',shell=True,check=True)
            print('pushed',fn,flush=True); return True
        except subprocess.CalledProcessError as e:
            print('push retry',i,e,flush=True); time.sleep(10+i*10)
    return False
if __name__=='__main__':
    y=int(sys.argv[1]); import subprocess
    months=[int(x) for x in sys.argv[3].split(',')] if len(sys.argv)>3 else list(range(1,13))
    have=set()
    try:
        subprocess.run('git fetch -q origin data-dukas',shell=True,check=True)
        have=set(subprocess.run('git ls-tree -r --name-only origin/data-dukas',shell=True,capture_output=True,text=True).stdout.split())
    except Exception: pass
    for m in months:
        fn=f'{OUT}/{SYM}_1s_{y}{m:02d}.parquet'
        if f'dukas/{SYM}_1s_{y}{m:02d}.parquet' in have: print('have',y,m,flush=True); continue
        month(y,m)
        if os.path.exists(fn): push_month(fn)
