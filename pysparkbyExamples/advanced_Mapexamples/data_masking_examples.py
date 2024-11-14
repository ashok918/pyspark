from pyspark.sql import SparkSession
if __name__ == '__main__':
    print("welcome to pyspark data masking examples")
    spark = SparkSession.builder.master("local[*]").appName("data_masking").getOrCreate()

    data = ["john.doe@example.com", "jane.smith@company.org"]
    rdd = spark.sparkContext.parallelize(data)

    def mask_data(email):
        name, domain = email.split("@")
        masked_name = name[0]+"*"*(len(name)-2)+name[-1]
        return masked_name+"@"+domain
    res = rdd.map(mask_data)
    for res in res.collect():
        print(res)




