import json

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("map_tranformations").getOrCreate()

# implement the x2+2x+1



















""" 

example 1
data = [1,2,3,4,5,6,7]
rdd = spark.sparkContext.parallelize(data)
doubleRdd = rdd.map(lambda x: x*2)
res = doubleRdd.collect()
for num in res:
    print("double of number of each is that Rdd ", num)
    
    
example 2    
data = ["apple","banana","cherry"]
rdd =  spark.sparkContext.parallelize(data)
uppecase_rdd = rdd.map(lambda str: str.upper())
res = uppecase_rdd.collect()

for item in res:
    print("upper case ",item)    
    
example 3
data = ["hello world","apache spark","map tranformations"]
rdd = spark.sparkContext.parallelize(data)

words_rdd = rdd.map(lambda x: x.split())
for word in words_rdd.collect():
    print("the word looks ", word)   
    
    
example 4
data = [("a",1),("b",2),("c",3)]
rdd = spark.sparkContext.parallelize(data)

keys_rdd = rdd.map(lambda x: x[0])
for item in keys_rdd.collect():
    print(item)    
example 5

data = ["apple","banana","cherry"]

rdd = spark.sparkContext.parallelize(data)
legofstrrdd = rdd.map(lambda x: len(x))

for item in legofstrrdd.collect():
    print("item of length is ", item)

example 6
data = [1,2,3,4,5]
rdd = spark.sparkContext.parallelize(data)
resrdd = rdd.map(lambda x:x+10)

for item in resrdd.collect():
    print(item)
    
example 7

data = ['{"name": "Alice", "age": 25}',
        '{"name": "Bob", "age": 30}',
        '{"name": "Charlie", "age": 35}']
rdd = spark.sparkContext.parallelize(data)
parsedrdd = rdd.map(lambda x: json.loads(x))
for item in parsedrdd.collect():
    print(item)

exmaple 8

data = [1,2,3,4,5]

rdd = spark.sparkContext.parallelize(data)

def complex_fun(x):
    return x**2 + 2*x + 1

comple_rdd = rdd.map(complex_fun)

for item in comple_rdd.collect():
    #print("complex_rdd: ")
    print(item)

     
"""
