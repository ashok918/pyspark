from pyspark.sql import SparkSession

""" 
def createSparkSession(self,SparkSession):
    spark = SparkSession.builder.appName("example1").getOrCreate()
    return spark
"""



if __name__ == '__main__':
    print("welcome to Pyspark coding.............")
    spark = SparkSession.builder.appName("PEI").getOrCreate()
    path="C://Users//user//Documents//Data//sample.txt"
    #df = spark.read.csv(path)
    df = spark.read.option("header","true").csv(path)

    # lession 2 :- filter and handling missing values 





    """ 
    df.show()

    #Checking the datatypes of cplumns

    df.printSchema()
    # select columns and indexing from dataframe

    df.select("name").show()

    print(df.columns)
    

    df.select(['name','age']).show()
    print(df.dtypes)
    
    

    df = df.withColumn("exp after 2 years",df['expre']+2)
    df.drop(df['expre']).show()
    df.withColumnRenamed("exp after 2 years","expre").show()
    
    """
