from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
if __name__ == '__main__':
    print("welcome to python coding exampls")
    spark = SparkSession.builder.master("local[*]").appName("rddfirstApplication").getOrCreate()

    """ 
    conf = SparkConf()
    conf.setMaster("local").setAppName("firstexample")
    sc = SparkContext.getOrCreate(conf)
    print(sc.appName)

    print("the pySpark App name is ", sc.appName)
    
    data = [1,2,3,4,5,6,7,8,9,10]
    spark = SparkSession.builder.master("local[*]").appName("rddfirstApplication").getOrCreate()
    rdd = spark.sparkContext.parallelize(data)
    print(rdd)
    """

    #path="C://Users//user//Documents//Data//examples.csv"
    path = r"C:\Users\user\OneDrive\Documents\Data\examples.csv"
    rdd = spark.sparkContext.textFile(path,10)
    print("the result is ", rdd.collect())
    print("the number of partitions result is ", rdd.getNumPartitions())
    rdd1 = spark.sparkContext.emptyRDD()
    print("empty rdd is ",rdd1.collect())

    repartrdd = rdd.repartition(4)
    print("the number of partitions result is ", repartrdd.getNumPartitions())







