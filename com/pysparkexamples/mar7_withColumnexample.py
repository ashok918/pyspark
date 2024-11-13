from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("withColumn").getOrCreate()

# withColumns is Tranformation function of dataframe which is used to change the Value
# convert the datatype
# adding new Value
# in this examples i will walk you through commonly used Pyspark dataframe operations


data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
from pyspark.sql import SparkSession
#spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
df = spark.createDataFrame(data=data, schema = columns)
df.printSchema()
df.show(truncate=False)

#111##################Change the Data Type of Given Column##############

# By using withColumn in Pyspark we can cast or change the data type of columns
# in order change data type we can use cast() function along with withColumn



df1 = df.withColumn("salary", col("salary").cast("Integer"))
df1.printSchema()

#112 updateing the exiting columns

df1.withColumn("salary",col("salary")*100).show()
df1.withColumn("Country",lit("usa")).show()




