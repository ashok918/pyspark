from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark  = SparkSession.builder.appName("filterTranformation").getOrCreate()
rdd = spark.sparkContext.parallelize([1,2,3,4,5,6,7,8,9,10])

def isEven(num):
    return num%2==0

rdd1 = rdd.filter(isEven)
print(rdd1.collect())
