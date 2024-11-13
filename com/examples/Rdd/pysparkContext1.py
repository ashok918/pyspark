from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def main():
    sc = SparkContext(appName="ProgrammCreekexample1")
    ssc = StreamingContext(sc,1)
    rddQueue=[]
    for _ in range(5):
         rddQueue += [ssc.sparkContext.parallelize([j for j in range(1, 1001)], 10)]
         inputStream = ssc.queueStream(rddQueue)

