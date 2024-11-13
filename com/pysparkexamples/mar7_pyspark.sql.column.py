from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("mar7").getOrCreate()

# Pyspark.sql.class provide several methods .
# to work Dataframe to manipluate columns values
# evaluate the Boolean expression to filter the rows
# retrive a value or part of Value from dataframe columns

## i will cover in this lession How to Create Columns Object , access them to perform the operations

####   PySpark Columns class represents single columns in DataFrame
# it privide the functions that are used to maniplaute Dataframe row  and columns
# evaluate the Boolean experssion thatare  used with filter transformation to fiter Dataframe Rows and columns


####################Create Columns Class Objects##########using lit()########

""" 
from pyspark.sql.functions import lit,col
#litobj = lit("examples_test")

data = [("james",20),("anna",40)]
df = spark.createDataFrame(data).toDF("name.fname","gender")
df.printSchema()
#df.show(truncate=False)

df.select(df["`name.fname`"]).show()
#df.select(df.name.fname).show()
df.select(col("`name.fname`")).show()

"""

# create Dataframe with struct using Row Class

from pyspark.sql import Row

data = [Row(name="james",prop=Row(hair="black",eye="brown")),
        Row(name="ann",prop=Row(hair="black",eye="brown"))]


df = spark.createDataFrame(data)
df.printSchema()
#df.show()

# accessing Struct columns Values
from pyspark.sql.functions import *

#df.select(df.prop.hair).show()
#df.select(df["prop.hair"]).show()
#df.select(col("prop.hair")).show()

df.select(col("prop.*")).show()










