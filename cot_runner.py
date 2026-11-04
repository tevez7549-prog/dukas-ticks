# CFTC Commitments of Traders, LEGACY futures-only (1986+), yearly zip files -> one parquet on branch data-cot. Public data, research only.
import urllib.request, zipfile, io, os, sys, time, subprocess, pandas as pd
Y0,Y1=int(sys.argv[1]),int(sys.argv[2]); frames=[]
for y in range(Y0,Y1+1):
    url=f'https://www.cftc.gov/files/dea/history/deacot{y}.zip'
    for k in range(6):
        try:
            req=urllib.request.Request(url,headers={'User-Agent':'Mozilla/5.0'}); raw=urllib.request.urlopen(req,timeout=120).read(); break
        except Exception as e: print(y,'retry',k,e,flush=True); time.sleep(10+10*k); raw=None
    if not raw: print('FAILED',y,flush=True); continue
    z=zipfile.ZipFile(io.BytesIO(raw)); name=[n for n in z.namelist() if n.lower().endswith('.txt')][0]
    d=pd.read_csv(z.open(name),low_memory=False); d.columns=[c.strip() for c in d.columns]; frames.append(d); print(y,len(d),flush=True)
D=pd.concat(frames,ignore_index=True)
keep=[c for c in D.columns if any(k in c for k in ('Market and Exchange Names','As of Date in Form YYYY-MM-DD','CFTC Contract Market Code','Open Interest (All)','Noncommercial Positions-Long (All)','Noncommercial Positions-Short (All)','Commercial Positions-Long (All)','Commercial Positions-Short (All)','Nonreportable Positions-Long (All)','Nonreportable Positions-Short (All)'))]
D=D[keep]; D.columns=['market','code','date','oi','nc_long','nc_short','c_long','c_short','nr_long','nr_short'][:len(keep)] if len(keep)==10 else D.columns
print('rows',len(D),'markets',D.iloc[:,0].nunique(),flush=True)
os.makedirs('/tmp/out',exist_ok=True); D.to_parquet('/tmp/out/cot_legacy.parquet',compression='zstd',index=False)
for i in range(8):
    try:
        subprocess.run('git fetch -q origin data-cot && git checkout -q -B data-cot origin/data-cot || (git checkout -q --orphan data-cot && git rm -rfq . >/dev/null 2>&1 || true)',shell=True,check=True)
        os.makedirs('cot',exist_ok=True); import shutil; shutil.copy('/tmp/out/cot_legacy.parquet','cot/')
        subprocess.run('git add cot && git commit -qm "cot legacy" && git push -q origin data-cot',shell=True,check=True); print('pushed'); break
    except subprocess.CalledProcessError as e: print('push retry',i,e); time.sleep(10)
