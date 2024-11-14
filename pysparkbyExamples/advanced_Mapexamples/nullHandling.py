from pyspark.sql import SparkSession

if __name__ == '__main__':
    print("welcome to examples on handling null values")
    spark = SparkSession.builder.master("local[*]").appName("nullvalues").getOrCreate()
    data = [("Alice", None), ("Bob", 25), ("Charlie", None)]
    rdd  = spark.sparkContext.parallelize(data)

    def handling_null(record):
        name, age = record
        if age is None:
            age = 18
        return (name, age)

    res = rdd.map(handling_null)
    for i in res.collect():
        print(i)

