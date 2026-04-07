from pyspark.sql import functions as F
from pyspark.sql import types as T
from safety_knife.spark_utils import get_or_create_spark_session

def main() -> None:
    spark = get_or_create_spark_session(app_name="silver_analysis")
    spark.sql(f"""
        SELECT 
        t.Date,
        i.Symbol,
        p.Open,
        p.Close,
        CASE WHEN p.Close > p.Open THEN 'up' ELSE 'down' END as Movement,
        p.High,
        p.Low,
        p.High - p.Low AS Intraday_Range,
        p.Volume
        FROM dv_yfinance.link_instrument_day lim
        JOIN dv_yfinance.sat_instrument_day_prices p 
            ON lim.Instrument_Day_LK = p.Instrument_Day_LK
        JOIN dv_yfinance.hub_calendar t ON lim.Calendar_HK = t.Calendar_HK
        JOIN dv_yfinance.hub_instrument i ON lim.Instrument_HK = i.Instrument_HK
        WHERE DATE(t.Date) > '2026-03-01'
        ORDER BY t.Date;
""").show() 



if __name__ == "__main__":
    main()
