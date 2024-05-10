from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("day1").getOrCreate()
rdd = spark.sparkContext.parallelize([1,2,3,4,5,6])
print(rdd.collect())

path="C://Users//user//Documents//Data//examples.csv"
rdd1 = spark.sparkContext.textFile(path)
print(rdd1.collect())

print("the number of partitions in rdd",rdd1.getNumPartitions())
