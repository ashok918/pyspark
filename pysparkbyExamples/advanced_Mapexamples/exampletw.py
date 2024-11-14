from pyspark.sql import SparkSession
if __name__ == '__main__':
    print("welcome to second map tranformation example")
    spark = SparkSession.builder.master("local[*]").appName("mapTranformation_2").getOrCreate()

    products = [("product1", 10, 5),("product2",20,8),("produt3", 15,6)]
    products_rdd = spark.sparkContext.parallelize(products)

    for product in products_rdd.collect():
        print(product)

    def add_total_price(record):
        product, quantity, price_per_unit = record
        total_price = quantity*price_per_unit
        return (product, quantity, price_per_unit,total_price)

    total_price_rdd = products_rdd.map(add_total_price)

    for record in total_price_rdd.collect():
        print("the  item is ",record)
