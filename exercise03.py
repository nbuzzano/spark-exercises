#
# Who are the second most selling and the least selling persons (sellers) for each product? 
# Who are those for the product with product_id = 0 ?
#

# Edge cases:
# If a product has been sold by only one seller, we’ll put it into a special category (category: Only seller or multiple sellers with the same quantity).
# If a product has been sold by more than one seller, but all of them sold the same quantity, we are going to put them in the same category as if they were only a single seller for that product (category: Only seller or multiple sellers with the same quantity).
# If the “least selling” is also the “second selling”, we will count it only as “second seller”
#
# ========================================
#

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row, Window
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "3g") \
    .appName("Exercise1") \
    .getOrCreate()

# Read the source tables
products_table = spark.read.parquet("./data/products_parquet")
sales_table = spark.read.parquet("./data/sales_parquet")
sellers_table = spark.read.parquet("./data/sellers_parquet")

# Calcuate the number of pieces sold by each seller for each product
sales_table = sales_table.groupby(col("product_id"), col("seller_id")) \
                         .agg(sum("num_pieces_sold").alias("num_pieces_sold"))

# Create the window functions, one will sort ascending the other one descending. Partition by the product_id
# and sort by the pieces sold
window_desc = Window.partitionBy(col("product_id")).orderBy(col("num_pieces_sold").desc())
window_asc = Window.partitionBy(col("product_id")).orderBy(col("num_pieces_sold").asc())

# Create a Dense Rank (to avoid holes)
sales_table = sales_table.withColumn("rank_asc", dense_rank().over(window_asc)). \
    withColumn("rank_desc", dense_rank().over(window_desc))

# Get products that only have one row OR the products in which multiple sellers sold the same amount
# (i.e. all the employees that ever sold the product, sold the same exact amount)
single_seller = sales_table.where(col("rank_asc") == col("rank_desc")) \
                           .select(col("product_id").alias("single_seller_product_id"), \
                                   col("seller_id").alias("single_seller_seller_id"), \
                                   lit("Only seller or multiple sellers with the same results").alias("type"))

# Get the second top sellers
second_seller = sales_table.where(col("rank_desc") == 2) \
                           .select(col("product_id").alias("second_seller_product_id"), \
                                   col("seller_id").alias("second_seller_seller_id"), \
                                   lit("Second top seller").alias("type"))

# Get the least sellers and exclude those rows that are already included in the first piece
# We also exclude the "second top sellers" that are also "least sellers"
least_seller = sales_table.where(col("rank_asc") == 1).select(
    col("product_id"), col("seller_id"),
    lit("Least Seller").alias("type")
).join(single_seller, (sales_table["seller_id"] == single_seller["single_seller_seller_id"]) & (
        sales_table["product_id"] == single_seller["single_seller_product_id"]), "left_anti"). \
    join(second_seller, (sales_table["seller_id"] == second_seller["second_seller_seller_id"]) & (
        sales_table["product_id"] == second_seller["second_seller_product_id"]), "left_anti")

# Union all the pieces
union_table = least_seller.select(
    col("product_id"),
    col("seller_id"),
    col("type")
).union(second_seller.select(
    col("second_seller_product_id").alias("product_id"),
    col("second_seller_seller_id").alias("seller_id"),
    col("type")
)).union(single_seller.select(
    col("single_seller_product_id").alias("product_id"),
    col("single_seller_seller_id").alias("seller_id"),
    col("type")
))
union_table.show()

# Which are the second top seller and least seller of product 0?
union_table.where(col("product_id") == 0).show()

# ==============

# Windows in pySpark 
# https://medium.com/@sergey.ivanchuk/practical-pyspark-window-function-examples-cb5c7e1a3c41
# https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html


# Output:
# +----------+---------+--------------------+                                     
# |product_id|seller_id|                type|
# +----------+---------+--------------------+
# |         0|        0|Only seller or mu...|
# +----------+---------+--------------------+
