from safety_knife.config import DATA_DIR, ticker_symbol, output_path
from safety_knife.bronze.store_data import load_from_delta, show_table_history

df0 = load_from_delta(output_path)
print(df0.count())
df0.show()

show_table_history(output_path)
