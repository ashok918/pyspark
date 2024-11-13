from pyspark.sql import SparkSession

if __name__ == '__main__':
    print("examples of collect on dataframe")
    spark = SparkSession.builder.appName("collect program examples").getOrCreate()
    """ 
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 22)]
    columns = ["Name", "Age"]

    df=spark.createDataFrame(data,columns)
    df.printSchema()
    df.show()

    collecdDf=df.collect()
    for row in collecdDf:
        print(row)
    """
    """ 
    #Example 2: Handling large datasets with collect()


    large_data = [(i, f"Name_{i}") for i in range(1, 1000001)]
    columns = ["ID", "Name"]
    large_df = spark.createDataFrame(large_data, columns)

    # Perform some transformations on the large DataFrame
    processed_df = large_df.filter(large_df["ID"] % 2 == 0).withColumn("Namee", large_df["Name"].upper())

    # Use take() to retrieve a sample instead of collect() for large datasets
    sample_data = processed_df.take(5)

    # Display the sample data
    for row in sample_data:
        print(row)
    """



    
