from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("count_rows_where_null_present").getOrCreate()
file_path = r"C:\Users\user\Desktop\Data_sample\Test.csv"

df = spark.read.csv(file_path,header=True, inferSchema=True)
df.printSchema()
df.show()

df1 = df.withColumn("Physics",split(df.marks,"\\|")[0]).withColumn("Chemisty",split(df.marks,"\\|")[1]).withColumn("Math",split(df.marks,"\\|")[2]).drop("marks")
df1.printSchema()
df1.show()


df2 = df.select("Id","Name","Age",explode(split(df.marks,"\\|")))
df2.printSchema()
df2.show()
