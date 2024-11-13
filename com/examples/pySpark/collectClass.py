from pyspark.sql import SparkSession

if __name__ == '__main__':
    print("welcome to Python Programmin")

    spark=SparkSession.builder.appName("Collect class example").getOrCreate()
    dept = [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]

    columns=["dept_name","dept_id"]
    df=spark.createDataFrame(dept,columns)
    df.printSchema()
    df.show(truncate=False)

    #resdf=df.collect() # returns Array of Row type
    #resdf=df.collect()[0] # first element in the array
    resdf=df.collect()[0][0]
    print(resdf)
    print(type(resdf))

    """ 
    for col in df.collect():
        print("department name is" ,col["dept_name"] + "department id is " +str(col["dept_id"]))


    #collDf = df.collect()
    #print(collDf)
    #print(type(collDf))
    """
