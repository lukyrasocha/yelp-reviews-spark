import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import countDistinct, col, explode, split
from functools import reduce
from timeit import default_timer as timer

conf = SparkConf()
conf.set("spark.executor.memory", "2G")
conf.set("spark.executor.instances", "4")

spark = SparkSession.builder \
	            .appName('template-application') \
		    .config(conf=conf) \
		    .getOrCreate()
print('Normal load')
start = timer()
bs = spark.read.json("/datasets/yelp/business.json")
#rs = spark.read.json ("/datasets/yelp/review.json")
#us = spark.read.json("/datasets/yelp/user.json")
end = timer()
print(end-start)

print('Parquet load')
start = timer()
bs2 = spark.read.json("/datasets/yelp/parquet/business.parquet")
#rs2 = spark.read.json ("/datasets/yelp/parquet/review.parquet")
#us2 = spark.read.json("/datasets/yelp/parquet/user.parquet")
end = timer()
print(end-start)

print('Normal Filter')
start=timer()
bs.filter(bs.categories.rlike('Mexican'))
end=timer()
print(end-start)

print(bs2)
print('Parquet Filter')
start=timer()
bs2.filter(bs2.categories.rlike('Mexican'))
end=timer()
print(end-start)
