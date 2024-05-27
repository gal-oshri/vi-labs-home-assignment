from pyspark.sql import SparkSession
from pyspark.sql.functions import lag, col, avg, to_date, regexp_replace, stddev, sqrt, lit
from pyspark.sql.window import Window

# Create a Spark session
spark = SparkSession.builder \
    .appName("Gal Oshri Home assignment Vi Labs") \
    .config("spark.hadoop.fs.s3a.access.key", "****") \
    .config("spark.hadoop.fs.s3a.secret.key", "*****") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

# Define the path to the CSV file
stocks_file_path = "s3://aws-glue-gal-osh-home-assignment/stocks_input/stock_prices.csv"
output_base_path= "s3://aws-glue-gal-osh-home-assignment/objectives/"

# Load the CSV file into a DataFrame
stocks_prices_df = spark.read.csv(stocks_file_path, header=True, inferSchema=True)

# Cast date column into date type
date_format = "MM/dd/yyyy"
# Preprocess the string date column to ensure consistent formatting
stocks_prices_df = stocks_prices_df.withColumn("date", regexp_replace(col("date"), r"^(\d)/", r"0$1/")) \
    .withColumn("date", regexp_replace(col("date"), r"/(\d)/", r"/0$1/"))
stocks_prices_df = stocks_prices_df.withColumn("date", to_date(stocks_prices_df["date"], date_format))

# Check for rows with no close or open price
stocks_wo_close_df = stocks_prices_df.filter(col("close").isNull())

if stocks_wo_close_df.head(1):
    print("There are stocks without closing price. It should be handled here\n")
else:
    print("There are no stocks without closing price.\n")

#### Objective 1 #####
# Define a window specification partitioned by ticker and ordered by date
window_ticker_order_date = Window.partitionBy("ticker").orderBy("date")

# Calculate the previous day's close price
stocks_prices_prev_close_df = stocks_prices_df.withColumn("prev_close", lag("close").over(window_ticker_order_date))

# Calculate the daily return: (today's close - yesterday's close) / yesterday's close
stocks_daily_return_df = stocks_prices_prev_close_df.withColumn("daily_return",
                                                                ((col("close") - col("prev_close")) / col(
                                                                    "prev_close")) * 100.0)

# Filter out rows where 'prev_close' is null (i.e., the first date for each stock)
stocks_daily_return_df = stocks_daily_return_df.filter(col("prev_close").isNotNull())

# Calculate the average daily return for each date
average_daily_return_df = stocks_daily_return_df.groupBy("date").agg(avg("daily_return").alias("avg_daily_return"))

# Show the resulting DataFrame
average_daily_return_df.show(250)
average_daily_return_df.write.csv(f"{output_base_path}/objective_1/", header=False, mode='overwrite')
#### Objective 1 #####


#### Objective 2 #####
# Calculate frequency for each stock
stocks_prices_freq_df = stocks_prices_df.withColumn("frequency", col("close") * col("volume"))
stocks_prices_avg_freq_df = stocks_prices_freq_df.groupBy("ticker").agg(avg("frequency").alias("avg_frequency"))

# Find the ticker with the highest average price-volume product
most_frequent_stock = stocks_prices_avg_freq_df.orderBy(col("avg_frequency").desc())
most_frequent_stock.show(1)
most_frequent_stock.limit(1).write.csv(f"{output_base_path}/objective_2/", header=False, mode='overwrite')
#### Objective 2 #####


#### Objective 3 #####
# Calculate the standard deviation of daily returns from objective 1
stddev_df = stocks_daily_return_df.groupBy("ticker").agg(stddev("daily_return").alias("daily_return_stddev"))

# Annualize the standard deviation (assuming 252 trading days per year)
annualized_stddev_df = stddev_df.withColumn("annualized_stddev", col("daily_return_stddev") * sqrt(lit(252)))

# Find the ticker with the highest annualized standard deviation
most_volatile_stock = annualized_stddev_df.orderBy(col("annualized_stddev").desc())
most_volatile_stock = most_volatile_stock.drop(col("daily_return_stddev"))
most_volatile_stock.show(1)
most_volatile_stock.limit(1).write.csv(f"{output_base_path}/objective_3/", header=False, mode='overwrite')

#### Objective 3 #####


#### Objective 4 #####
# Calculate the previous day's close price
stocks_prices_prev_30_days_close_df = stocks_prices_df.withColumn("prev_30_days_close", lag("close", 30).over(window_ticker_order_date))

# Calculate the daily return: (today's close - yesterday's close) / yesterday's close
stocks_prices_prev_30_days_close_df = stocks_prices_prev_30_days_close_df.withColumn("30_daily_return",
                                                                ((col("close") - col("prev_30_days_close")) / col(
                                                                    "prev_30_days_close")) * 100.0)

# Filter out rows where 'prev_close' is null (i.e., the first date for each stock)
stocks_prices_prev_30_days_close_df = stocks_prices_prev_30_days_close_df.filter(col("30_daily_return").isNotNull())



# Calculate the average daily return for each date
stocks_prices_prev_30_days_close_df_ordered = stocks_prices_prev_30_days_close_df.orderBy(col("30_daily_return").desc())
stocks_prices_prev_30_days_close_df_ordered = stocks_prices_prev_30_days_close_df_ordered.select(["ticker", "date"])
stocks_prices_prev_30_days_close_df_ordered.show(3)
stocks_prices_prev_30_days_close_df_ordered.limit(3).write.csv(f"{output_base_path}/objective_4/", header=False, mode='overwrite')

#### Objective 4 #####

# Stop the Spark session
spark.stop()
