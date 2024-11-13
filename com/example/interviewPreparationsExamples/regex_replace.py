from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("regular_exp_replace").getOrCreate()
data=[('1-A-12-2-B-23-3-C-34-4-D-15')]
columns=["values"]
df = spark.createDataFrame(data,columns)
df.show()
