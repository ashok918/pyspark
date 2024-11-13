from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("collect").getOrCreate()

# Pyspark rdd/dataframe collect() is an action operation that is used to retrive all the elements of the dataset to
#the driver node

# we should use collect() on smmaler dataset usually after filter and group by operations
# retrive on large dataset we might see Outofmemeory exception

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
#deptDF.show(truncate=False)

# deptDF.collect() is retrive all elememnts in dataframe as an Array of rows to driver Node
dfCOll = deptDF.collect()
print(dfCOll)

for col in dfCOll:
    #print(col['dept_name']+" "+str(col['dept_id']))
    #print(col[0],col[1])
    print(col)


