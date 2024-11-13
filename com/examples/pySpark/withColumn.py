from pyspark.sql import SparkSession

if __name__ == '__main__':
    print("welcome to withColumn examples")
    spark=SparkSession.builder.appName("withColumn").getOrCreate()


