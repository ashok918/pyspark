from pyspark.sql import SparkSession

if __name__ == '__main__':
    print("welcome to example four ")
    spark = SparkSession.builder.master("local[*]").appName("example four").getOrCreate()

    data = [
    {"name": "Alice", "age": 34, "salary": 70000},
    {"name": "Bob", "age": 25, "salary": 48000},
    {"name": "Charlie", "age": 40, "salary": 120000}
]
    rdd = spark.sparkContext.parallelize(data)
    def filter_age(record):
        if record["age"] > 30:
            record["salary"] *= 1.1
        return record

    res = rdd.map(filter_age)
    for record in res.collect():
        print(record)

