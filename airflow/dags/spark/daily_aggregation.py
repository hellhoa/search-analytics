from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, hour, abs, row_number
from pyspark.sql.window import Window

def aggregate_daily_data():
    spark = SparkSession.builder \
        .appName("DailySearchVolumeAggregation") \
        .master("local[*]") \
        .getOrCreate()

    # Read from MySQL
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://user:password@db:3306/search_analytics") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "keyword_search_volume") \
        .load()

    # Calculate the closest record to 9 AM for each day
    window = Window.partitionBy("keyword_id", to_date("created_datetime")) \
        .orderBy(abs(hour("created_datetime") - 9))

    daily_data = df \
        .withColumn("date", to_date("created_datetime")) \
        .withColumn("rank", row_number().over(window)) \
        .where(col("rank") == 1) \
        .drop("rank")

    # Write back to MySQL
    daily_data.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://user:password@db:3306/search_analytics") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "daily_keyword_search_volume") \
        .mode("overwrite") \
        .save()

if __name__ == "__main__":
    aggregate_daily_data()