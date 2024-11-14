from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == '__main__':
    print("welcome to map tranformations examples...............")
    spark = SparkSession.builder.master("local[*]").appName("mapexamples").getOrCreate()
    data = [
    ("tx001", "user123", 15000.50, "purchase", "2024-11-13 14:23:15"),
    ("tx002", "user456", 75.25, "refund", "2024-11-13 15:45:30"),
    ("tx003", "user789", 350.00, "purchase", "2024-11-14 10:10:10"),
    ("tx004", "user123", 25000.00, "purchase", "2024-11-14 18:15:45"),
]
    rdd = spark.sparkContext.parallelize(data)
    for row in rdd.collect():
        print(row)

    def process_recdords(record):
        transaction_id, user_id, amount, transaction_type, timestamp = record
        high_value = amount > 10000
        date_extract = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        day_of_week=date_extract.strftime("%A")

        enriched_record = {
        "transaction_id": transaction_id,
        "user_id": user_id,
        "amount": amount,
        "transaction_type": transaction_type,
        "timestamp": timestamp,
        "high_value": high_value,
        "day_of_week": day_of_week
         }



        return enriched_record

    finalres  = rdd.map(process_recdords)
    for finalres in finalres.collect():
        print(finalres)




















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

    

    rdd = spark.sparkContext.parallelize(["2012,DOMINIC,CAYUGA,M,6", "2013,EMMA,ERIE,F,18"])
    #resRdd = rdd.map(lambda x : x.split(",")).map(lambda x: (x[0],x[1],x[2],x[3],int(x[4])*10))

    #for rows in resRdd.collect():
    #    print(rows)

    def record_process(record):
        year, name,country, sex, count = record.split(",")
        return (year, name.lower(),country, sex, int(count)+5)

    res = rdd.map(record_process)
    for resset in res.collect():
        print(resset)

    """







