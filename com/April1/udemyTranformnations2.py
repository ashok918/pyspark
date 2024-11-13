from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import collections

spark = SparkSession.builder.appName("rating").getOrCreate()


path="C://Users//user//Documents//Data//marks.csv"

rdd = spark.sparkContext.textFile(path)
for i in rdd.collect():
    print(i)

#rdd3 = rdd2.map(lambda x: sum(num for num in x[1].split(";")))
#print(rdd3.collect())

#rdd1 = rdd.map(lambda x: x.split(";"))
#print(rdd1.collect())



""" 
path="C://Users//user//Documents//Data//sample.csv"
df= spark.read.csv(path,header=True,inferSchema=True)
df.printSchema()
df.show()
"""
""" 
#nullcnt = df.select([count(col(c).isNull(),c)) for c in df.columns])
# df.select([count(col(c).isNull()).alias(c) for c in df.columns])

#import pandas as pd
from pyspark.sql.functions import col, count

# Assuming df is your DataFrame
null_count = df.select([count(col(c).isNull()).alias(c) for c in df.columns])
print(null_count.show())

res = df.agg(*[count(col(c)).alias(c) for c in df.columns])
res.show()

"""

#broadcastVariable = spark.sparkContext.broadcast([1,2,3])
#print(broadcastVariable.value)
""" 

def square(num):
    return num*num

spark.udf.register("squarewithPython",square(5))


"""

"""
acc = spark.sparkContext.accumulator(0)
rdd = spark.sparkContext.parallelize([1,2,3,4])

def f(x):
    global acc
    acc += x

rdd.foreach(f)
print(acc.value)


"""



""" 
path="C://Users//user//Documents//Data//u.data"

lines = spark.sparkContext.textFile(path)
rating=lines.map(lambda x: x.split()[2])
res=rating.countByValue()

sortedRes = collections.OrderedDict(sorted(res.items()))

for k,y in sortedRes.items():
    print("$s %i",(k,y))
"""
