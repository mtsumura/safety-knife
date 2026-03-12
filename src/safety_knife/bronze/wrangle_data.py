from safety_knife.bronze.store_data import load_to_spark

def pd_rename_column(df, old_name, new_name):
    """
    Renames a column in a pandas dataframe.

    Parameters:
        df (pandas.DataFrame): The dataframe to rename the column in.
        old_name (str): The current name of the column.
        new_name (str): The new name for the column.

    Returns:
        None
    """
    df.rename(columns={old_name: new_name}, inplace=True)

def rename_column(df, old_name, new_name):
    return df.withColumnRenamed(old_name, new_name)

def rename_stock_splits_column(df):
    """
    Renames the column "Stock Splits" to "Stock_Splits" in a Spark dataframe.

    Args:
        df (pyspark.sql.DataFrame): The input Spark dataframe.

    Returns:
        pyspark.sql.DataFrame: A new dataframe with the renamed column.
    """
    return df.withColumnRenamed("Stock Splits", "Stock_Splits")


def convert_date_column(df):
    """
    Converts the Date column which is of String type to DateTime type in a Spark dataframe.

    Args:
        df (pyspark.sql.DataFrame): The input Spark dataframe.

    Returns:
        pyspark.sql.DataFrame: A new dataframe with the converted column.
    """
    from pyspark.sql.functions import col, to_date
    return df.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd HH:mm:ssXXX"))

def convert_date_column_to_datetime(df):
    """
    Converts the specified date column from string format to datetime format.

    Parameters:
    df (pyspark.sql.DataFrame): The DataFrame containing the date column.
    date_column (str): The name of the column to be converted.

    Returns:
    pyspark.sql.DataFrame: The DataFrame with the converted date column.
    """
    from pyspark.sql.functions import col, to_timestamp
    # Convert the date column to timestamp
    df = df.withColumn("DateTime", to_timestamp(col("Datetime"), "yyyy-MM-dd HH:mm:ssXXX"))
    
    return df

def add_symbol_column(df, symbol):
    """
    Adds a new string column called Symbol with the given value in a Spark dataframe.

    Args:
        df (pyspark.sql.DataFrame): The input Spark dataframe.
        symbol (str): The stock ticker symbol to be added as a new column.

    Returns:
        pyspark.sql.DataFrame: A new dataframe with the added column.
    """
    from pyspark.sql.functions import lit
    return df.withColumn("Symbol", lit(symbol))


def prepare_data(df, is_minute):
    """
    Selects specific columns from the input Spark dataframe.

    Args:
        df (pyspark.sql.DataFrame): The input Spark dataframe.

    Returns:
        pyspark.sql.DataFrame: A new dataframe with selected columns.
    """
    return df.select(
        "Date" if not is_minute else "DateTime",
        "Symbol", 
        "Open", 
        "High", 
        "Low", 
        "Close", 
        "Volume", 
        "Dividends", 
        "Stock_Splits"
    )

def add_price_to_earnings_column(df):
    """
    Adds a new column called priceToEarnings that is calculated by currentPrice / trailingEps.

    Args:
        df (pyspark.sql.DataFrame): The input Spark dataframe.

    Returns:
        pyspark.sql.DataFrame: A new dataframe with the added column.
    """
    from pyspark.sql.functions import col
    return df.withColumn("priceToEarnings", col("currentPrice") / col("trailingEps"))

def prepare_company_data(df):
    """
    Selects specific columns from the input Spark dataframe.

    Args:
        df (pyspark.sql.DataFrame): The input Spark dataframe.
    """
    return df.select("symbol", "longName", "marketCap", "priceToEarnings", "dividendYield", "totalRevenue", "netIncome", "bookValuePerShare", "priceToBook", "forwardEps")


def prepare_bronze_output(spark, pd_df, ticker_symbol, is_minute):
    print(pd_df)
    print(pd_df.index.name)
    spark_df = load_to_spark(pd_df, spark=spark)

    # spark_df.show()   
    spark_df = rename_stock_splits_column(spark_df)
    # spark_df.show()
    spark_df = convert_date_column_to_datetime(spark_df) if is_minute else convert_date_column(spark_df)
    spark_df.show()

    spark_df = add_symbol_column(spark_df, ticker_symbol)

    spark_df = prepare_data(spark_df, is_minute=is_minute)
    spark_df.show()
    spark_df.printSchema()
    return spark_df