from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#	Initialize the Spark session
spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb") \
    .appName("Exercise1") \
    .getOrCreate()

#   Read the source tables in Parquet format
products_table = spark.read.parquet("./data/products_parquet")
sales_table = spark.read.parquet("./data/sales_parquet")
sellers_table = spark.read.parquet("./data/sellers_parquet")

#   Print the number of orders
print("Number of Orders: {}".format(sales_table.count()))

#   Print the number of sellers
print("Number of sellers: {}".format(sellers_table.count()))

#   Print the number of products
print("Number of products: {}".format(products_table.count()))

# ======================

#   Output how many products have been actually sold at least once
print("Number of products sold at least once")
sales_table.agg(countDistinct(col("product_id"))).show()

#   Output which is the product that has been sold in more order
print("Product present in more orders")
sales_table.groupBy(col("product_id"))\
		   	.agg(count("*").alias("cnt")).orderBy(col("cnt").desc())\
		   	.limit(1)\
		   	.show()	

# ======================
#	Output:

# 	Number of Orders: 400040                                                        
#	Number of sellers: 10
#	Number of products: 100000

#	Number of products sold at least once
#	+-----------------+                                                             
#	|count(product_id)|
#	+-----------------+
#	|            18083|
#	+-----------------+

#	Product present in more orders
#	+----------+------+                                                             
#	|product_id|   cnt|
#	+----------+------+
#	|         0|380000|
#	+----------+------+

