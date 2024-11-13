from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("fakefriendexamples").getOrCreate()

path="C://Users//user//Documents//Data//1800.csv"

rdd = spark.sparkContext.textFile(path)
print(rdd.collect())

def readLines(line):
    fields=line.split(",")
    stationId=fields[0]
    entryType=fields[2]
    temp=float(fields[3]) *0.1*(9.0/5.0)+32.0
    return (stationId,entryType,temp)

rdd1 = rdd.map(readLines)
mintemp=rdd1.filter(lambda x: "TMIN" in x[1])
stationTemp=mintemp.map(lambda x: (x[0],x[2]))
#res=stationTemp.reduceByKey(lambda x, y: min(x,y))
res= stationTemp.reduceByKey(lambda x, y: min(x,y))

print(res.collect())


#print(mintemp.collect())













""" 
path="C://Users//user//Documents//Data//fakefriends.csv"

def parseLine(line):
    fields=line.split(",")
    age=int(fields[2])
    nooffriends=int(fields[3])
    return (age,nooffriends)

rdd = spark.sparkContext.textFile(path)
print(rdd.collect())
lines = rdd.map(parseLine)
print(lines.collect())
totalByAge=lines.mapValues(lambda x:(x,1))
print(totalByAge.collect())
totalByAge_lates = totalByAge.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
print(totalByAge_lates.collect())
avgbuage=totalByAge_lates.mapValues(lambda x:x[0]/x[1])

for res in avgbuage.collect():
    print(res)


"""





