from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("filter_where").getOrCreate()

# filter function is used filter row from rdd or dataframe based on given condition
# you can use where instead of filter() if you come from sql background
# filter function return new Dataframe
# you can use isin method part of filter Conditions

from pyspark.sql.types import StructType,StructField
from pyspark.sql.types import StringType, IntegerType, ArrayType
from pyspark.sql.functions import *
data = [
    (("James","","Smith"),["Java","Scala","C++"],"OH","M"),
    (("Anna","Rose",""),["Spark","Java","C++"],"NY","F"),
    (("Julia","","Williams"),["CSharp","VB"],"OH","F"),
    (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M"),
    (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M"),
    (("Mike","Mary","Williams"),["Python","VB"],"OH","M")
 ]

schema = StructType([
     StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
     ])),
     StructField('languages', ArrayType(StringType()), True),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
 ])

df = spark.createDataFrame(data = data, schema = schema)
df.printSchema()
df.show(truncate=False)

""" 
# filter the rows based on list values using isin methos###################################
#df.filter(df.state== "OH").show()
#df.filter((df.state != "OH") & (col("gender") == "M")).show()

# filter based on list values
li =['OH','CA','DE']
df.filter(~df.state.isin(li)).show()

"""


""" 
#filter rows by using startsWith,endsWith and contains

df.filter(df.state.startswith("N")).show()
df.filter(df.state.endswith("H")).show()
df.filter(df.state.contains("O")).show()

"""

# filter the rows using like and rlike
#df.filter(df.state.like("OH")).show()
df.filter(array_contains(df.languages,"Java")).show()



















