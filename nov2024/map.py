from pyspark.sql import SparkSession

if __name__ == '__main__':
    print("welcome to map tranformations examples...............")
    spark = SparkSession.builder.master("local[*]").appName("mapexamples").getOrCreate()

    #path = r"C:\Users\user\OneDrive\Documents\Data\samplerdd.csv"
    """ 
    rdd = spark.sparkContext.textFile(path)
    header = rdd.first()
    rdd_withoutheader = rdd.filter(lambda line: line!=header)
    print(rdd_withoutheader.collect())


    rows = rdd_withoutheader.map(lambda line: line.split(","))
    #print(rdd_withoutheader.collect())
    for row in rows.collect():
        print("your first name is", row[1])
        
    """


    #example one
    """ 
    rdd = spark.sparkContext.parallelize([1,2,3,4,5,6,7])
    resRdd = rdd.map(lambda num: num+10)
    print(resRdd.collect())
    

    rdd = spark.sparkContext.parallelize(["apple","banana","orange"])
    resRdd = rdd.map(lambda word: word.upper())
    print(resRdd.collect())
    

    rdd = spark.sparkContext.parallelize(["hello world", "welcome to PySpark"])
    resRdd = rdd.map(lambda words: words.split(" "))
    print(resRdd.collect())
    

    rdd  = spark.sparkContext.parallelize([("alis",27),("bob",30),("ashok",35)])
    resRdd  = rdd.map(lambda x: (x[0],x[1]*2))
    for i in resRdd.collect():
        print(i)
    


    rdd = spark.sparkContext.parallelize(["2012,DOMINIC,CAYUGA,M,6", "2013,EMMA,ERIE,F,18"])
    res = rdd.map(lambda word: word.split(","))
    for res in res.collect():
        if res[3] == 'M':
            print("men")
        else:
            print("women")

    """









