import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]
columns = ["firstname","lastname","country","state"]
df = spark.createDataFrame(data = data, schema = columns)
#df.show(truncate=False)



""" 
df.select("firstname","lastname").show()

df.select(df.firstname,df.lastname).show()

from pyspark.sql.functions import *

df.select(col("firstname"),col("lastname")).show()
"""

"""
#selecting single and multiple columns from dataframe using regexpre

df.select(df.colRegex("`^.*name*`")).show()

 """

""" 
# sometimes you may need to select all columns from python list

df.select(*columns).show()
df.select("*").show()

df.select([col for col in df.columns]).show()

"""
# You can use columns by index

df.select(df.columns[:]).show()













