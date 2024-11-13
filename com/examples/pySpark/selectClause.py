from pyspark.sql import SparkSession


if __name__ == '__main__':
    print("welcome to PySpark select Examples")
    spark=SparkSession.builder.appName("select Examples").getOrCreate()
    data = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]
    columns = ["firstname","lastname","country","state"]
    df = spark.createDataFrame(data = data, schema = columns)
    """ 
    df.show(truncate=False)

    #df2=df.select("firstname","lastname").show()
    #df3=df.select(df.firstname,df.lastname).show()
    #df4=df.select(df["firstname"],df["lastname"]).show()

    df5=df.select(df.colRegex("`^.*name*`")).show()
    """


    df.select(df.columns[:3]).show()

    df.select([col for col in df.columns]).show()
