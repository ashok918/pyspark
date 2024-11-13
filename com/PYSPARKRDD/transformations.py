from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("transformations_examples").getOrCreate()
path="C://Users//user//Documents//Data//examples.csv"

""" 
rdd = spark.sparkContext.parallelize([1,2,3,4],2)
def sum_partion(iterator):
    yield sum(iterator)

sum_rdd = rdd.mapPartitions(sum_partion)
print(sum_rdd.collect())

"""

rdd = spark.sparkContext.parallelize([(1, 2), (3, 4), (5, 6), (7, 8)], 2)
def average_partition(iterator):
     x_sum = 0
     y_sum = 0
     count = 0
     for (x, y) in iterator:
         x_sum += x
         y_sum += y
         count += 1
     yield (x_sum/count, y_sum/count)

avg_rdd = rdd.mapPartitions(average_partition)
print(avg_rdd.collect())









""" 
rdd = spark.sparkContext.parallelize([1,2,3,4])
squaredrdd = rdd.map(lambda x: x**2)
print(squaredrdd.collect())

rdd1 = spark.sparkContext.parallelize(["This is a sentence.", "Another sentence.", "Yet another sentence."])
filterRdd = rdd1.filter(lambda line: "sentence" in line)
print(filterRdd.collect())
"""








""" 
rdd = spark.sparkContext.parallelize([1,2,3,4,5,6,7,8])
rdd1 = rdd.map(lambda x: x*2)
print(rdd1.collect())


from operator import add
a = spark.sparkContext.parallelize(["Bob","Sam","Peter","Mona","SHyam","Bob"])
rdd10 = a.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
print(rdd10.collect())

for (w,c) in rdd10.collect():
    print("{}: {}".format(w,c))

sortRdd = a.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).collect()
#res = sortRdd.sortBy(lambda x:x[1], ascending = False)
sorted_rdd = a.sortBy(lambda x: x[1], ascending=False)
for item in sorted_rdd.collect():
    print(item)

"""







def lowercaseName(row):
    name=row[1]
    country=row[2]
    return (name.lower(),country)


def addprefix(row,prefix="Mr."):
    name=row[1]
    country=row[2]
    return (f"{prefix} {name}", country)


""" 
rdd = spark.sparkContext.textFile(path)
header=rdd.filter(lambda x: x.startswith('Year'))
data=rdd.filter(lambda x: not x.startswith('Year'))

rows = data.map(lambda x: x.split(","))
print(rows)
for row in rows.collect():
    #print(row[1].lower(),row[2])
    print(row)

rdd1= rows.map(lowercaseName)
for rowsrdd1 in rdd1.collect():
    print(rowsrdd1)

rdd2 = rows.map(addprefix)
for rowsrdd2 in rdd2.collect():
    print(rowsrdd2)
"""
""" map output
['2012', 'DOMINIC', 'ONONDAGA', 'F', '14']
['2012', 'ADDISON', 'ONONDAGA', 'F', '14']
['2012', 'JULIA', 'ONONDAGA', 'F', '15']

"""
