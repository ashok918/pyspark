from pyspark.sql import SparkSession
import json

if __name__ == '__main__':
    print("welcome to map json parsing ")
    spark = SparkSession.builder.master("local[*]").appName("json_parse").getOrCreate()
    data = [
    '{"id": 1, "name": "John", "age": 30}',
    '{"id": 2, "name": "Jane", "age": 25}'
]

    rdd = spark.sparkContext.parallelize(data)

    def parse_json(record):
        return json.loads(record)

    resRdd = rdd.map(parse_json)
    for record in resRdd.collect():
        print(record["id"], record["name"], record["age"])



