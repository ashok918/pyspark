from _ast import In

from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("firstExamples").getOrCreate()
#spark.conf.set("")






""" 
data = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]


schema = StructType(
        [StructField('fname',StringType(),True),
        StructField("middlename",StringType(),True),
        StructField("lname",StringType(),True),
        StructField("id",StringType(),True),
        StructField("gender",StringType(),True),
        StructField("salary",IntegerType(),True)]
)


newSchema = StructType([
        StructField("name",StructType(
                [StructField("fname",StringType(),True),
                 StructField("mname",StringType(),True),
                 StructField("lname",StringType(),True)]
        )),
        StructField("id",StringType(),True),
        StructField("gender",StringType(),True),
        StructField("salary",IntegerType(),True)]
)

df = spark.createDataFrame(data,newSchema)
df.printSchema()
df.show(truncate=False)

"""

""" 
data = [("james","","smith","353425","M",600000),
        ("Michael","Rose","","40288","M",700000),
        ("Robert","","Williams","39192","",4000000),
        ("Maria","Annae","Jones","42114","F",5000000),
        ("Jen","Mary","Brown","","F",0)]

columns =["Fname","mname","lname","dob","gender","sala"]
df1  = spark.createDataFrame(data,columns)
df1.printSchema()
#df1.show(truncate=False)
df1.show()
"""
""" 
df2 = df1.toPandas()
print(df2)

schema = StructType(
    [StructField('firstname',StringType(),True),
    StructField('mname',StringType(),True),
    StructField('lname',StringType(),True)]

)
"""
"""
""" 
# empty rdd
""" 
erdd  = spark.sparkContext.emptyRDD()
print(erdd)

rdd2 = spark.sparkContext.parallelize([])
print(rdd2)


df1  = spark.createDataFrame(erdd,schema)
df1.printSchema()

df2 = rdd2.toDF(schema)
df2.printSchema()
df2.show()


# Convert pyspark rdd to dataframe

dept = [("finance",10),("Marking",20),("Sales",30),("It",40)]
columns = ["dept_name","dept_id"]
rdd = spark.sparkContext.parallelize(dept)
#for items in rdd.collect():
#    print(items)

df = rdd.toDF(columns)
df.printSchema()
df.show()
"""
