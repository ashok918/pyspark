from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("regex").getOrCreate()

data = [(2,"shivam","U6753679iy"),(1,"sagar","7654326798"),(3,"muni","897654321")]
columns =["Id","Student_name","Phone_num"]

df=spark.createDataFrame(data,columns)
df.printSchema()
df.show()

df1=df.select("*").filter(col("Phone_num").rlike('^[0-9]*$'))
df1.show()
