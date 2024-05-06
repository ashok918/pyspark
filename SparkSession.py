from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("sample").getOrCreate()
#creating pySPark DataFrame

df = spark.createDataFrame([("scala",25000),("python",5000),("java",6000)])
df.printSchema()
df.show()

#working with SPark sql

df.createOrReplaceTempView("sample")
df1 = spark.sql("select _1,_2 from sample")
df1.show()
