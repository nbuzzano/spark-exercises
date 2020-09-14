#
# Create a new column called "hashed_bill" defined as follows:
# - if the order_id is even: apply MD5 hashing iteratively to the bill_raw_text field, once for each 'A' (capital 'A') present in the text. E.g. if the bill text is 'nbAAnllA', you would apply hashing three times iteratively (only if the order number is even)
# - if the order_id is odd: apply SHA256 hashing to the bill text
# Finally, check if there are any duplicate on the new column
#
# Clue: In the final dataset, all the hashes should be different, so the query should return an empty dataset.
#

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row, Window
from pyspark.sql.types import IntegerType
import hashlib

#   Init spark session
spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "1g") \
    .appName("Exercise1") \
    .getOrCreate()

#   Load source data
products_table = spark.read.parquet("./data/products_parquet")
sales_table = spark.read.parquet("./data/sales_parquet")
sellers_table = spark.read.parquet("./data/sellers_parquet")

#   Define the UDF function
def algo(order_id, bill_text):
    #   If number is even
    ret = bill_text.encode("utf-8")
    if int(order_id) % 2 == 0:
        #   Count number of 'A'
        cnt_A = bill_text.count("A")
        for _c in range(0, cnt_A):
            ret = hashlib.md5(ret).hexdigest().encode("utf-8")
        ret = ret.decode('utf-8')
    else:
        ret = hashlib.sha256(ret).hexdigest()
    return ret

#   Register the UDF function.
algo_udf = spark.udf.register("algo", algo)

#   Use the `algo_udf` to apply the aglorithm and then check if there is any duplicate hash in the table
sales_table.withColumn("hashed_bill", algo_udf(col("order_id"), col("bill_raw_text")))\
           .groupby(col("hashed_bill")).agg(count("*").alias("cnt")).where(col("cnt") > 1)\
           .show()