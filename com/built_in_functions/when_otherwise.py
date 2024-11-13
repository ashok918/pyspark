from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

""" 
data = [("James","M",60000),("Michael","M",70000),
        ("Robert",None,400000),("Maria","F",500000),
        ("Jen","",None)]

columns = ["name","gender","salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.show()

df1 = df.withColumn("new_gender" , when(df.gender=='M',"Male")
                              .when(df.gender=='F',"female")
                              .when(df.gender.isNull(),"")
                              .otherwise(df.gender)
               )
df1.printSchema()
df1.show()

"""

""" 
data = [("John", 3.5), ("Jane", 4.2), ("Doe", 2.8)]
df = spark.createDataFrame(data, ["Name", "PerformanceRating"])

df1 = df.withColumn("Performance Category", when(df.PerformanceRating >=4,"High Perfoemnace")
                                     .when((df.PerformanceRating >=3) & (df.PerformanceRating < 4),"avgrage")
                                     .otherwise("lowperformance")
              )

df1.printSchema()
df1.show()
"""




data = [("John", 25), ("Jane", 35), ("Doe", 55)]
df = spark.createDataFrame(data, ["Name", "age"])

df1 = df.withColumn("customer_segment", when(df.age < 30,"young")
                                  .when((df.age >=30) & (df.age <50),"middle_age" )
                                  .otherwise("senior")
              )

df1.printSchema()
df1.show()
