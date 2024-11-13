from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Interview").getOrCreate()

""" 
evet_count = spark.sparkContext.accumulator(0.0)

def process_eve(event):
    global evet_count
    if event.startswith("success"):
        evet_count += 1

rdd = spark.sparkContext.parallelize(["success", "failure", "success", "success"])
rdd.foreach(process_eve)
print(evet_count.value)
"""

""" 
broadcast = spark.sparkContext.broadcast([1,2,3])
print(broadcast.value)

broadcast.unpersist()
"""

"""
acc = spark.sparkContext.accumulator(0)
rdd = spark.sparkContext.parallelize([1,2,3,4])

def sum(x):
    global acc
    acc += x

rdd.foreach(sum)
print(acc.value)
 """
