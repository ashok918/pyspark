"""
from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("firstexample").getOrCreate()
    print(spark.sparkContext)
    print("SPark App name is " + spark.sparkContext.appName)
"""


from pyspark import SparkConf, SparkContext
if __name__ == '__main__':
    print("welcome to pyspark coding examples ")
    conf = SparkConf()
    conf.setMaster("local").setAppName("firstexample")
    sc = SparkContext.getOrCreate(conf)
    print(sc.appName)

    rdd = sc.range(1,5)
    print(rdd.collect())
    sc.stop()
