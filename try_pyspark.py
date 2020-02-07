# This is just a basic pyspark "smoke" test


import os
from pyspark.sql import SparkSession

# os.environ['SPARK_MASTER_IP'] = "127.0.0.1"
os.environ['SPARK_LOCAL_IP'] = "127.0.0.1"
# os.environ['HADOOP_HOME'] = ''

spark = SparkSession\
    .builder\
    .getOrCreate()

print("Let's sum the numbers from 0 to 100")
df = spark.range(101)

print(df.groupBy().sum('id').collect())
print(df)
