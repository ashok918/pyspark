from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

""" 
data=[("James","Bond"),("Scott","Varsa")]
df=spark.createDataFrame(data).toDF("col1","col2")

df1 = df.withColumn("Name", expr("col1 ||','|| col2"))
df1.printSchema()
df1.show()
"""

"""
data = [("James","M"),("Michael","F"),("Jen","")]
columns = ["name","gender"]
df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()
df.show()

df1 = df.withColumn("gender",expr("case when gender= 'M' then 'male'"+
                            "when gender='F' then 'female' ELSE 'unkmow' end")

)
df1.printSchema()
df1.show()
"""

#Using an Existing Column Value for Expression#######################################

data=[("2019-01-23",1),("2019-06-24",2),("2019-09-20",3)]
df=spark.createDataFrame(data).toDF("date","increment")

df.printSchema()
df.show()
#############add one moth as increment moth#############using expr()

df1 = df.select(df.date,df.increment,
                    expr("add_months(date,1)").alias("increm_date"))

df1.printSchema()
df1.show()
