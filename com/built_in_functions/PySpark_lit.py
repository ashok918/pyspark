import pyspark
from pyspark.sql import SparkSession
#from pyspark.sql.functions import lit,col,when,typedLit

from pyspark.sql.functions import *

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

"""
data = [("111",50000),("222",60000),("333",40000)]
columns= ["EmpId","salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()
df.show()
"""
""" 
df1 = df.select(df.EmpId,df.Salary,lit("1").alias("lit_value"))
df1.printSchema()
df1.show()
"""

""" 
df1 = df.withColumn("loan_amount", when((df.salary>=40000) & (df.salary <=50000),lit("above2cr_sactioned")).otherwise(lit("5cr")))
df1.printSchema()
df1.show()
"""

# difference between lit() and typedLit() is that :- the typesLit() can handle collection types
# example Array,Dictornary(map)


data = [("John", 25), ("Jane", 30), ("Doe", 35)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Create a new column with a literal array
literal_array = typedLit([1, 2, 3])
df_with_array = df.withColumn("LiteralArray", literal_array)

df_with_array.show()




