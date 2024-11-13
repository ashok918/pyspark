from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark  = SparkSession.builder.appName("example1").getOrCreate()


# explode pyspark interview questions

simpleData=[(1,["sagar","prajapati"]),(2,["Shivam","narayan"]),(3,["muni","mtech"]),(4,["kim"])]
columns = ["id","name"]

df = spark.createDataFrame(simpleData,columns)
df.printSchema()
df.show()

output = df.select("Id",explode("name"))
output.printSchema()
output.show()

""" 
data_region1 = [
    (1, 100, 500),
    (2, 150, 750),
    (3, 200, 1000)
]

data_region2 = [
    (2, 120, 600),
    (3, 180, 900),
    (4, 250, 1250)
]

columns = ["product_id","quantity_sold","revenue"]

df1 = spark.createDataFrame(data_region1,columns)
#df1.printSchema()
#df1.show()

df2 = spark.createDataFrame(data_region2,columns)
#df2.printSchema()
#df2.show()

# Total Quantity and Revenue for Each Product:

unionDf = df1.union(df2)
unionDf.show()
unionDf.groupBy("product_id").agg({"quantity_sold":"sum","revenue":"sum"}) \
    .withColumnRenamed("sum(quantity_sold","Total Quantity sold") \
    .withColumnRenamed("sum(revenue)","Total Revinew").show()

# Calculate total quantity and revenue for each region
region_totals = unionDf.groupBy() \
    .agg(
        {"quantity_sold": "sum", "revenue": "sum"}
    ) \
    .withColumnRenamed("sum(quantity_sold)", "total_quantity_sold") \
    .withColumnRenamed("sum(revenue)", "total_revenue")

region_totals.show()

"""

""" 
#merge the 2 dataframe if not matching number of columns in dataframe 
simpleData=[(1,"sagar","cse","up",80),(2,"Shivam","it","mp",86),(3,"muni","mtech","Ap",70)]
columns1=["Id","Student_name","Deprtment_name","City","marks"]

df1 = spark.createDataFrame(simpleData,columns1)
df1.printSchema()
df1.show()



simpleDat1=[(5,"Raj","cse","Hp"),(7,"kunal","Mtech","Rajiustan")]
columns2 = ["Id","Student_name","Deprtment_name","City"]

df2 = spark.createDataFrame(simpleDat1,columns2)
df2.printSchema()
df2.show()

df2 = df2.withColumn("marks",lit("null"))


df1.union(df2).show()
"""
