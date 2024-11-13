from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("flatMap").getOrCreate()


rdd = spark.sparkContext.parallelize([3,4,5])
rdd1 = rdd.flatMap(lambda x: range(1,x)).collect()
print(rdd1)


data=["Big Data learning New to spark .exp in python and sql learning RDD good skills in Python Pandas is what keep my job intresting scaling is still a prob hoping spark can help. \
Intro to big data Good database skills sql and no sql"]

rdd = spark.sparkContext.parallelize(data)
rdd1 = rdd.flatMap(lambda line: line.split(" "))
rdd2 = rdd1.map(lambda line: (line,1))
rdd3 = rdd2.reduceByKey(lambda x,y :x+y)
print(rdd3.collect())






"""
d1 = ["This is an sample application to see the FlatMap operation in PySpark"]
rdd = spark.sparkContext.parallelize(d1)
print(rdd.collect())

rdd1 = rdd.flatMap(lambda line: line.split(" "))
print(rdd1.collect())

 """




""" 

data = ["Project Gutenberg’s",
        "Alice’s Adventures in Wonderland",
        "Project Gutenberg’s",
        "Adventures in Wonderland",
        "Project Gutenberg’s"]

rdd = spark.sparkContext.parallelize(data)
for line in rdd.collect():
    print(line)

rdd2 = rdd.flatMap(lambda x:x.split(" "))
print(rdd2.collect())
for line in rdd2.collect():
    print(line)

"""
