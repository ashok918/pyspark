from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("count_rows_where_null_present").getOrCreate()
file_path = r"C:\Users\user\Desktop\Data_sample\Test.csv"

df=spark.read.csv(file_path, header=True, inferSchema=True)
df.printSchema()
df.show()

print(df.columns)
df1 = df.select([count(when(col(i).isNull(),i)).alias(i) for i in df.columns])
df1.show()

"""
Id,Name,Age
1,A,23
2,B,null
3,C,56
4,null,null
5,null,null


"""
