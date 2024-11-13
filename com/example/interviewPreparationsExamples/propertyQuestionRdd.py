from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("rdd").getOrCreate()
file_path = r"C:\Users\user\Desktop\input.txt"
rdd = spark.sparkContext.textFile(file_path)
#print(rdd.collect())

header = rdd.filter(lambda l: l.startswith("Property ID"))
#for i in header.collect():
#    print(i)
data = rdd.filter(lambda l: not l.startswith("Property ID"))
for line in data.collect():
    print(line)

# i am now apply on flatMap on Rdd leter map transformation

#flatRdd = data.flatMap(lambda x:x.split(",")).map(lambda x:x.split("|"))
flatRdd = data.map(lambda x:x.split("|"))
for line in flatRdd.collect():
    print(line)

colList = header.first().split("|")
print(colList)

"""
1461262|Arroyo Grande|795000|3|3|2371|365.3|Short Sale
1478004|Paulo Pablo|399000|4|3|2818|163.59|Short Sale
1486551|Paulo Pablo|545000|4|3|3032|179.75|Short Sale
1492832|Santa Bay|909000|4|4|3540|286.78|Short Sale
['1461262', 'Arroyo Grande', '795000', '3', '3', '2371', '365.3', 'Short Sale']
['1478004', 'Paulo Pablo', '399000', '4', '3', '2818', '163.59', 'Short Sale']
['1486551', 'Paulo Pablo', '545000', '4', '3', '3032', '179.75', 'Short Sale']
['1492832', 'Santa Bay', '909000', '4', '4', '3540', '286.78', 'Short Sale']



"""
