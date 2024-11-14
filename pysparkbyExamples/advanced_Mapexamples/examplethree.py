from pyspark.sql import SparkSession

if __name__ == '__main__':
    print("welcome to example three which is revelent to Splitting and formatting String")
    spark = SparkSession.builder.master("local[*]").appName("Application name").getOrCreate()

    #data = ["user1:30:Engineer", "user2:25:Doctor", "user3:40:Teacher"]
    data = ["user1,30,Engineer", "user2,25,Doctor", "user3,40,Teacher"]
    rdd = spark.sparkContext.parallelize(data)

    def split_and_format(record):
        record = record.split(",")
        username = record[0]
        age = record[1]
        qualification = record[2]
        return (username,age, qualification)

    splitRdd = rdd.map(split_and_format)
    for record in splitRdd.collect():
        print("the record is ", record)




