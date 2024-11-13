from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("Tranformations").getOrCreate()

rdd = spark.sparkContext.parallelize(["Hello Worlds","Pyspark is awesome"])
def split_words(sentance):
    return sentance.split(" ")


def generate_combinations(num):
    return [(num,num*num),(num,num*num*num)]


#words = rdd.map(split_words).collect()
#[['Hello', 'Worlds'], ['Pyspark', 'is', 'awesome']]
#words = rdd.flatMap(split_words).collect()
#['Hello', 'Worlds', 'Pyspark', 'is', 'awesome']
#print(words)

""" 
numsrdd = spark.sparkContext.parallelize([1,2,3])
res1 = numsrdd.map(generate_combinations).collect()
res2 = numsrdd.flatMap(generate_combinations).collect()
print(res1)
print(res2)

"""

data = spark.sparkContext.parallelize(["", "hello", "", "world", ""])

def removeEmptyStr(string):
    if string:
        return [string]
    else:
        return []

emptyrdd = data.flatMap(removeEmptyStr).collect()
print(emptyrdd)




""" 
rdd = spark.sparkContext.parallelize([2,3,4]).map(lambda x: [x,x,x])
rdd1 = spark.sparkContext.parallelize([2,3,4]).flatMap(lambda x: [x,x,x])

print(rdd.collect())
print(rdd1.collect())
"""
