import pyspark
from pyspark import SparkContext 
import os
from glob import glob

logfile = r"D:\Spark\spark-2.4.3-bin-hadoop2.7\Readme.md"

sc = SparkContext("local", "first app")
logData = sc.textFile(logfile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print("Lines with a : {}, Lines with b : {}".format(numAs, numBs))
