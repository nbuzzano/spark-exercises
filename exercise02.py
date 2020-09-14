#
# For each seller find the average % of the target amount brought by each order
# =======================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql.types import IntegerType

# Create the Spark session
spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "3g") \
    .appName("Exercise2") \
    .getOrCreate()

# Read the source tables
products_table = spark.read.parquet("./data/products_parquet")
sales_table = spark.read.parquet("./data/sales_parquet")
sellers_table = spark.read.parquet("./data/sellers_parquet")

#   Wrong way to do this - Skewed 
#   (Note that Spark will probably broadcast the table anyway, unless we forbid it throug the configuration parameters)
print(sales_table.join(sellers_table, sales_table["seller_id"] == sellers_table["seller_id"], "inner") \
				 .withColumn("ratio", sales_table["num_pieces_sold"]/sellers_table["daily_target"]) \
				 .groupBy(sales_table["seller_id"]).agg(avg("ratio")) \
				 .show())

#   Correct way through broarcasting
print(sales_table.join(broadcast(sellers_table), sales_table["seller_id"] == sellers_table["seller_id"], "inner") \
				 .withColumn("ratio", sales_table["num_pieces_sold"]/sellers_table["daily_target"]) \
				 .groupBy(sales_table["seller_id"]).agg(avg("ratio")) \
				 .show())

# ======

# Output:
# +---------+--------------------+                                                
# |seller_id|          avg(ratio)|
# +---------+--------------------+
# |        7|5.096169354650682E-5|
# |        3|0.001531373548747...|
# |        8|4.831318310478898...|
# |        0|2.020865052631253E-5|
# |        5|2.546697568425875...|
# |        6|4.574446301376821E-5|
# |        9|1.411814354035687...|
# |        1|3.544151775256803E-5|
# |        4|4.586505612899644...|
# |        2|4.539248383848967E-5|
# +---------+--------------------+
