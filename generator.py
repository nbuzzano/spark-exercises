import pandas as pd
from tqdm import tqdm
import csv
import random
import string
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

random.seed(1999)

letters = string.ascii_lowercase
letters_upper = string.ascii_uppercase
for _i in range(0, 10):
    letters += letters

for _i in range(0, 10):
    letters += letters_upper


def random_string(stringLength=10):
    """Generate a random string of fixed length """
    return ''.join(random.sample(letters, stringLength))


prods_size = 100000#75000000
print("Products between {} and {}".format(1, prods_size))
product_ids = [x for x in range(1, prods_size)]
dates = ['2020-07-01', '2020-07-02', '2020-07-03', '2020-07-04', '2020-07-05', '2020-07-06', '2020-07-07', '2020-07-08',
         '2020-07-09', '2020-07-10']
seller_ids = [x for x in range(1, 10)]


#   Generate products
products = [[0, "product_0", 22]]
for p in tqdm(product_ids):
    products.append([p, "product_{}".format(p), random.randint(1, 150)])
#   Save dataframe
df = pd.DataFrame(products)
df.columns = ["product_id", "product_name", "price"]
df.to_csv("products.csv", index=False)
del df
del products

#   Generate sellers
sellers = [[0, "seller_0", 2500000]]
for s in tqdm(seller_ids):
    sellers.append([s, "seller_{}".format(s), random.randint(12000, 2000000)])
#   Save dataframe
df = pd.DataFrame(sellers)
df.columns = ["seller_id", "seller_name", "daily_target"]
df.to_csv("sellers.csv", index=False)

#   Generate sales
total_rows = 10000#500000
prod_zero = int(total_rows * 0.95)
prod_others = total_rows - prod_zero + 1
df_array = [["order_id", "product_id", "seller_id", "date", "num_pieces_sold", "bill_raw_text"]]
with open('sales.csv', 'w', newline='') as f:
    csvwriter = csv.writer(f)
    csvwriter.writerows(df_array)

order_id = 0
for i in tqdm(range(0, 40)):
    df_array = []

    for i in range(0, prod_zero):
        order_id += 1
        df_array.append([order_id, 0, 0, random.choice(dates), random.randint(1, 100), random_string(500)])

    with open('sales.csv', 'a', newline='') as f:
        csvwriter = csv.writer(f)
        csvwriter.writerows(df_array)

    df_array = []
    for i in range(0, prod_others):
        order_id += 1
        df_array.append(
            [order_id, random.choice(product_ids), random.choice(seller_ids), random.choice(dates),
             random.randint(1, 100), random_string(500)])

    with open('sales.csv', 'a', newline='') as f:
        csvwriter = csv.writer(f)
        csvwriter.writerows(df_array)

print("Done")

spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .appName("Exercise1") \
    .getOrCreate()

products = spark.read.csv(
    "products.csv", header=True, mode="DROPMALFORMED"
)
products.show()
products.write.parquet("products_parquet", mode="overwrite")

sales = spark.read.csv(
    "sales.csv", header=True, mode="DROPMALFORMED"
)
sales.show()
sales.repartition(200, col("product_id")).write.parquet("sales_parquet", mode="overwrite")

sellers = spark.read.csv(
    "sellers.csv", header=True, mode="DROPMALFORMED"
)
sellers.show()
sellers.write.parquet("sellers_parquet", mode="overwrite")

#================

# https://spark.apache.org/docs/latest/sql-data-sources-parquet.html

# DataFrames can be saved as Parquet files, maintaining the schema information.

# Parquet files are self-describing so the schema is preserved.
# The result of loading a parquet file is also a DataFrame.
# Example -> parquetFile = spark.read.parquet("people.parquet")

#===

# https://www.tutorialspoint.com/apache_spark/apache_spark_quick_guide.htm
# https://spark.apache.org/docs/latest/rdd-programming-guide.html#shuffle-operations

# repartition(numPartitions)
# Reshuffle the data in the RDD randomly to create either more or fewer partitions and 
# balance it across them. This always shuffles all data over the network.

# Spark actions are executed through a set of stages, separated by distributed “shuffle” operations. 
# Spark automatically broadcasts the common data needed by tasks within each stage.

#==

#SparkSession -> .config("spark.sql.autoBroadcastJoinThreshold", -1)
#https://spark.apache.org/docs/latest/sql-performance-tuning.html
#https://stackoverflow.com/questions/43984068/does-spark-sql-autobroadcastjointhreshold-work-for-joins-using-datasets-join-op

# autoBroadcastJoinThreshold: 
# Configures the maximum size in bytes for a table that will be broadcast to 
# all worker nodes when performing a join. By setting this value to -1 broadcasting can be disabled. 

# The join strategy hints, namely BROADCAST, MERGE, SHUFFLE_HASH and SHUFFLE_REPLICATE_NL, 
# instruct Spark to use the hinted strategy on each specified relation when joining them with another relation. 
# For example, when the BROADCAST hint is used on table ‘t1’, broadcast join (either broadcast hash join or
# broadcast nested loop join depending on whether there is any equi-join key) with ‘t1’ as the build side will be
# prioritized by Spark even if the size of table ‘t1’ suggested by the statistics is above the configuration spark.sql.autoBroadcastJoinThreshold.

