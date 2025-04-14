timeout /t 10
start "Data Collector" cmd /k "cd /d Z:\btc-orderflow-analysis && python data_collector.py"
start "OrderFlow Dashboard" cmd /k "cd /d Z:\btc-orderflow-analysis && python orderflow_dashboard.py"