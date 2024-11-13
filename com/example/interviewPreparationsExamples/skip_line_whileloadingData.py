from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("skiping line ").getOrCreate()

file_path = r"C:\Users\user\Desktop\Data_sample\sampledata.txt"

rdd = spark.sparkContext.textFile(file_path).zipWithIndex()
rdd1 = rdd.collect()[0:4]






