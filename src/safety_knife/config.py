DATA_DIR = 'data/bronze'
SILVER_DATA_DIR = 'data/silver'
ticker_symbol = "VET.TO"
#   | Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
#   | Default: 1mo
#   | Can combine with start/end e.g. end = start + periods
# interval : str
#   | Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
#   | Intraday data cannot extend last 60 days
period='5y' #1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
minute_period = '5d'
output_path = f"/Users/michaeltsumura/Workspace/safety-knife/{DATA_DIR}"
output_path_historical = f"{output_path}/historicals"
output_path_minute = f"{output_path}/historicals_minute"
company_output_path = f"{output_path}/company"  
silver_output_path = f"/Users/michaeltsumura/Workspace/safety-knife/{SILVER_DATA_DIR}/vault"
hive_warehouse_dir = "file:/Users/michaeltsumura/Workspace/safety-knife/spark-warehouse"