
"""
 Rdd creations
# there are two ways to create rdd in pyspark
# 1 . by using Parallelize method
# 2 . reading data from disk (means extrenal storage like hdfs ,s2 and azure blob storage


"""

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("rdd creationg example").getOrCreate()

# By using Parallelize method
#list= [1,2,3,4,5,6,7,8]
#rdd = spark.sparkContext.parallelize(list)
#print(rdd.collect())

# By using textfile method
path="C://Users//user//Documents//Data//examples.csv"
rdd1 = spark.sparkContext.textFile(path)
#print(rdd1.collect())

# How create empty rdd

#emptyRdd = spark.sparkContext.emptyRDD()
#print(type(emptyRdd))

# rdd Partitions
#print(rdd1.getNumPartitions())
""" 
collected_data = rdd1.collect()
for item in collected_data:
    print(item)

for item in rdd1.take(3):
    print(item)
"""

rows = rdd1.map(lambda line: line.split(","))
for row in rows.take(rows.count()):
    #print(row)
    print(row[1])

















"""
Rdd stands for resileant Distrubuted dataset is building block of pyspark.
it has own advantages like fault tolarance, immutable and distrubured collection of objects .
immutable means once you create an rdd , you can'r change it .
data with in rdd segmented into logical partitions  and distrubuted acorss the cluster .

rdds are collection of objects  similar to list in python . the differenc is that rdd is computed on servral process . and list in python run's on single process .

PySpark Benifits 
in memory processing 

pyspark loads the data from disk and process it in memory  and keep the data iun memepry .
we can cache/ persists the rdd in memory to reuse the previous tranformations

immutable 

fault tolerance
 lazy evolutions 
Partitions 




"""
