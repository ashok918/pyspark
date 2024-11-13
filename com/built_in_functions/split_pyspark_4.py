from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("split_examples").getOrCreate()

data = [("James, A, Smith","2018","M",3000),
            ("Michael, Rose, Jones","2010","M",4000),
            ("Robert,K,Williams","2010","M",4000),
            ("Maria,Anne,Jones","2005","F",4000),
            ("Jen,Mary,Brown","2010","",-1)
            ]

columns=["name","dob_year","gender","salary"]
df=spark.createDataFrame(data,columns)
df.printSchema()
df.show()


#df.select(split(col('name'),',')
#df.select(split("name"),",").alias("ArrayName").drop("name").show()
df1 = df.select(split(col("name"),",").alias("ArrayName")).drop("name")
##########Wrong++++++++++++++++++++df.select(split(df.name),",").alias("ArrayName1").drop("name").show()

df.createOrReplaceTempView("Person")
spark.sql("select split(name,",") as NameArray from Person ").show()

