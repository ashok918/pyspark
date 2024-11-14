from pyspark.sql import SparkSession

if __name__ == '__main__':
    print("welcome to flatMap examples")
    spark = SparkSession.builder.master("local[*]").appName("flatMapExamples").getOrCreate()

    """
    data = ["Hello world", "This is PySpark", "flatMap is powerful"]
    rdd = spark.sparkContext.parallelize(data)

    res = rdd.flatMap(lambda x: x.split(" "))
    #res = rdd.map(lambda x: x.split(" "))
    print(res.collect())
    for i in res.collect():
        print(i)
    

    data = [
    "PySpark is an amazing tool for big data processing",
    "Learning PySpark can be fun and rewarding"
]
    stop_words = {"is", "an", "for", "be", "and"}

    rdd = spark.sparkContext.parallelize(stop_words)

    def tokonize_and_filter(record):
        words = record.split(" ")
        return [word for word in words if word.lower() not in stop_words]

    res = rdd.flatMap(tokonize_and_filter)
    print(res.collect())
    for res in res.collect():
        print(res)

    """






