# dukas-ticks
Research data only: Dukascopy XAUUSD public tick feed → 1-second bars (bid OHLC, last ask, tick count, UTC), one parquet per month on branch `data-dukas` (`dukas/XAUUSD_1s_YYYYMM.parquet`). Built by `.github/workflows/dukas.yml` + `dukas_runner.py`; resumable (months already on the data branch are skipped), auto-resumes every 6 h. No trading code here.
