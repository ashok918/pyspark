from pyspark import SparkConf,SparkContext
conf = SparkConf()
conf.setMaster("local").setAppName("example")

sc = SparkContext.getOrCreate(conf)
print(sc.appName)





""" 
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("first application").getOrCreate()
print(spark.sparkContext)
print(spark.sparkContext.appName)


"""


















"""
* pyspark.sparkContext is an entry point to the  pyspark functionality that  is used to communcate with cluster and to create rdd, accumulator and broadcast variables .
in this lession we will lean how to create pyspark.sparkContext with some examples.
note That you can create only one sparkContext per JVM , in order to create another one first you stop existing one using stop() method .





"""
