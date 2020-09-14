from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create Spark session
spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb") \
    .appName("Exercise1") \
    .getOrCreate()

# Read Source tables
products_table = spark.read.parquet("./data/products_parquet")
sales_table = spark.read.parquet("./data/sales_parquet")
sellers_table = spark.read.parquet("./data/sellers_parquet")

sales_table.groupby(col("date")) \
		   .agg(countDistinct(col("product_id")).alias("distinct_products_sold")) \
		   .orderBy(col("distinct_products_sold").desc()) \
		   .show()

# Output 
#+----------+----------------------+                                             
#|      date|distinct_products_sold|
#----------+----------------------+
#|2020-07-04|                  2074|
#|2020-07-03|                  2030|
#|2020-07-01|                  2015|
#|2020-07-10|                  1992|
#|2020-07-08|                  1981|
#|2020-07-02|                  1973|
#|2020-07-07|                  1962|
#|2020-07-06|                  1958|
#|2020-07-05|                  1954|
#|2020-07-09|                  1908|
#+----------+----------------------+
