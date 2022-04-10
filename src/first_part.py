import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import countDistinct, col


conf = SparkConf()
conf.set("spark.executor.memory", "2G")
conf.set("spark.executor.instances", "4")

spark = SparkSession.builder \
	            .appName('template-application') \
		    .config(conf=conf) \
		    .getOrCreate()


bs = spark.read.json("/datasets/yelp/business.json")
rs = spark.read.json ("/datasets/yelp/review.json")
us = spark.read.json("/datasets/yelp/user.json")

# SPECIFIC DATAFRAME QUERIES
## Question 1 - Total number of reviews
print('Q1 - Total number of reviews')
bs.agg({'review_count': 'sum'}).show()
print('Q1 DONE')

## Question 2 - filter some businesses
print('Q2 - businesses with 5 stars that have been reviewed by at least 1000 people')
filtered = bs.filter(bs.stars == 5).filter(bs.review_count >= 1000).select("name","stars","review_count")
filtered.show()
print('Q2 DONE')

## Question 3 - filter users that wrote more than 1000 reviews
print('Q3 - influencers (people who wrote more than 1000 reviews)')
influencers = us.filter(us.review_count > 1000).select("user_id")
influencers.show()
print('Q3 DONE')

## Question 4 - find businesses that have been reviewed by more than 5 influencers
print('Q4 - businesses that have been reviewed by more than 5 distinct influencers')
#Filter only the reviews made by the influencers
reviews_by_influencers = rs.join(influencers, ["user_id"], 'leftsemi')
#Find the distinct counts of influencer users per business_id
gr = reviews_by_influencers.groupBy('business_id').agg(countDistinct('user_id').alias('influencer_count'))
#Take only the businesses that have more than 5 reviews from influencers
grFiltered = gr.filter(gr.influencer_count > 5)
#Find the corresponding business names
business_names = bs.join(grFiltered, bs.business_id == grFiltered.business_id, "inner").select("name")
business_names.show()
print('Q4 DONE')

# Question 5 - Find an ordered list of users based on the average star count given in all their reviews
print("Q5 - ordered list of users based on the average star count given in all their reviews")
avgStars = rs.groupBy('user_id').avg('stars')
user_names = us.join(avgStars, us.user_id == avgStars.user_id, "inner").orderBy('avg(stars)',ascending=False).select("name") #, "avg(stars)", "average_stars")
user_names.show()
print("Q5 DONE")
