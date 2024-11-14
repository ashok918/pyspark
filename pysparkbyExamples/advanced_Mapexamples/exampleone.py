from pyspark.sql import SparkSession
if __name__ == '__main__':
    print("welcome to Pyspark examples practiced")
    ## example one is Converting data types each record in rdd.
    # if your rdd contains records as string but you want convert certain fileds to integers or floats
    spark = SparkSession.builder.master("local[*]").appName("mapAdvancedExamples").getOrCreate()

    data = [("123", "45.67", "89"), ("987", "65.43", "21")]
    rdd = spark.sparkContext.parallelize(data)

    for row in rdd.collect():
        print(row)

    def convert_data_type(record):
        field1 = int(record[0])
        field2 = float(record[1])
        field3 = int(record[2])
        return (field1,field2,field3)
    converted_rdd = rdd.map(convert_data_type)
    for item in converted_rdd.collect():
        print("the item is ", item)





