import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import countDistinct, col, explode, split
from functools import reduce

conf = SparkConf()
conf.set("spark.executor.memory", "2G")
conf.set("spark.executor.instances", "4")

spark = SparkSession.builder \
	            .appName('template-application') \
		    .config(conf=conf) \
		    .getOrCreate()

bs = spark.read.json("/datasets/yelp/business.json")
rs = spark.read.json ("/datasets/yelp/review.json")
restaurants = bs[bs.categories.contains('Restaurants')] 
restaurants = restaurants.withColumnRenamed('stars', 'business_stars')

non_europe = ['South American', 'African','Mexican', 'Argentine', 'Bolivian', 'Chilean', 'Ecuadorian', 'Peruvian', 'Brazilian', \
	      'Uruguayan', 'Colombian', 'Paraguayan', 'Venezielean', 'Asian', 'Japanese', 'Chinese', 'Mongolian' , \
	      'Turkish', 'Korean','Indian','Pakistani','Bangladeshi','Thai','Singaporean', \
	      'Vietnamese', 'Indonesian', 'Cambodian', 'Burmese', 'Malaysian', 'Filipino', \
	      'Arab', 'Iranian','Taiwanese', 'Russian', 'Latin American']

safe_cuisines = ['European', 'French', 'Italian', 'German', 'Danish', 'Greek', '^American', 'Austrian']

# Join reviews and business information together
res_bus = restaurants.join(rs, "business_id")

# Explode columns to get single categories
res_bus_explode = res_bus.withColumn('single_categories', explode(split(col('categories'),', ')))

non_europe = res_bus_explode.filter(res_bus_explode.single_categories.rlike("|".join(non_europe)))
europe_like = res_bus_explode.filter(res_bus_explode.single_categories.rlike("|".join(safe_cuisines)))

non_europe_corr = non_europe.withColumn('AuthComment', (non_europe.text.rlike('([Ll]egitimate)|([Aa]uthentic[a-z]*)').cast('integer')))
europe_corr = europe_like.withColumn('AuthComment', (europe_like.text.rlike('([Ll]egitimate)|([Aa]uthentic[a-z]*)').cast('integer')))

print('CORRELATION FOR NON_EUROPE')
print(non_europe_corr.stat.corr('AuthComment', 'stars'))
print('CORRELATION FOR EUROPE')
print(europe_corr.stat.corr('AuthComment', 'stars'))

