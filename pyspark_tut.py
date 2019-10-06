import pyspark
from pyspark import SparkContext 
import os
from glob import glob
from operator import add

logfile = r"D:\Spark\spark-2.4.3-bin-hadoop2.7\Readme.md"

sc = SparkContext("local", "first app")
logData = sc.textFile(logfile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print("Lines with a : {}, Lines with b : {}".format(numAs, numBs))

# RDDs introduction (Resilient Distributed Dataset) -> contains transform and action operations
words = sc.parallelize (
   ["scala", 
   "java", 
   "hadoop", 
   "spark", 
   "akka",
   "spark vs hadoop", 
   "pyspark",
   "pyspark and spark"]
)

nums = sc.parallelize([1, 2, 3, 4, 5])

counts = words.count()
# print ("Number of elements in RDD -> %i" % (counts))

def f(x): print(x)
fore = words.foreach(f)

# filter
words_filter = words.filter(lambda x: 'spark' not in x)
filtered = words_filter.collect()
print ("Filtered RDD -> {}".format(filtered))

# reduce
add_nums = nums.reduce(add)
print("Adding all numbers -> {}".format(add_nums))

# accumalators and broadcast
words_new = sc.broadcast(["scala", "java", "hadoop", "spark", "akka"]) 
data = words_new.value 
print ("Stored data -> %s" % (data)) 
elem = words_new.value[2] 
print ("Printing a particular element in RDD -> %s" % (elem))

num = sc.accumulator(0)
def f(x):
    global num
    num += x

rdd = sc.parallelize([20, 30, 40, 50])
rdd.foreach(f)

final = num.value
print("Accumalated value is -> {}".format(final))