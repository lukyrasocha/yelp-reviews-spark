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

negative_words = ['dirty','kitsch','cheap','rude','simple', 'filthy', 'greasy','low-price','low-cost']

non_europe = ['South American', 'African','Mexican', 'Argentine', 'Bolivian', 'Chilean', 'Ecuadorian', 'Peruvian', 'Brazilian', \
	      'Uruguayan', 'Colombian', 'Paraguayan', 'Venezielean', 'Asian', 'Japanese', 'Chinese', 'Mongolian' , \
	      'Turkish', 'Korean','Indian','Pakistani','Bangladeshi','Thai','Singaporean', \
	      'Vietnamese', 'Indonesian', 'Cambodian', 'Burmese', 'Malaysian', 'Filipino', \
	      'Arab', 'Iranian','Taiwanese', 'Russian', 'Latin American']

safe_cuisines = ['European', 'French', 'Italian', 'Danish', '^American', 'Austrian']

# Join reviews and business information together
res_bus = restaurants.join(rs, "business_id")

# Explode columns to get single categories
res_bus_explode = res_bus.withColumn('single_categories', explode(split(col('categories'),', ')))

non_europe = res_bus_explode.filter(res_bus_explode.single_categories.rlike("|".join(non_europe)))
europe_like = res_bus_explode.filter(res_bus_explode.single_categories.rlike("|".join(safe_cuisines)))

#europe_like.select('single_categories').show()
#non_europe.select('single_categories').show()

# Average starts per cuisine
avg_stars_europe = europe_like.groupBy('single_categories').avg("stars")
avg_stars_non_europe = non_europe.groupBy('single_categories').avg("stars")

europe_like = europe_like.withColumn("price_range", europe_like.attributes.RestaurantsPriceRange2.cast('float'))
non_europe = non_europe.withColumn("price_range", non_europe.attributes.RestaurantsPriceRange2.cast('float'))

print('============== GET AVERAGE PRICE RANGE PER CUISINE ================')
europe_like.groupBy('single_categories').avg("price_range").show()
non_europe.groupBy('single_categories').avg("price_range").show()

# Get counts of reviews for each cuisine respectively 
europe_counts = europe_like.groupBy('single_categories').count().withColumnRenamed("count", "review_count")
non_europe_counts = non_europe.groupBy('single_categories').count().withColumnRenamed("count", "review_count")

# Get only the reviews that contain authentic language
europe_like_auth = europe_like.filter(europe_like.text.rlike('([Ll]egitimate)|([Aa]uthentic[a-z]*)'))
non_europe_auth = non_europe.filter(non_europe.text.rlike('([Ll]egitimate)|([Aa]uthentic[a-z]*)'))

# Number of authentic reviews per each cuisine
europe_like_auth_count = europe_like_auth.groupBy('single_categories').count().withColumnRenamed("count", "auth_count")
non_europe_auth_count = non_europe_auth.groupBy('single_categories').count().withColumnRenamed("count", "auth_count")

# Join the count of all reviews and the counts of only authentic reviews for each single cuisine
europe_counts = europe_like_auth_count.join(europe_counts, "single_categories")
non_europe_counts = non_europe_auth_count.join(non_europe_counts, "single_categories")

# Find the ratio between the authentic reviews and all reviews for each single cuisine
europe_counts = europe_counts.withColumn('ratio (%)', (europe_counts.auth_count/europe_counts.review_count)*100)
non_europe_counts = non_europe_counts.withColumn('ratio (%)', (non_europe_counts.auth_count/non_europe_counts.review_count)*100)

# Do authentic reviews contain negative words?
europe_auth_negative = europe_like_auth.filter(europe_like_auth.text.rlike("|".join(negative_words)))
non_europe_auth_negative = non_europe_auth.filter(non_europe_auth.text.rlike("|".join(negative_words)))

europe_auth_negative_count = europe_auth_negative.groupBy('single_categories').count().withColumnRenamed("count", "negative_count")
non_europe_auth_negative_count = non_europe_auth_negative.groupBy('single_categories').count().withColumnRenamed("count", "negative_count")


europe_counts = europe_counts.join(europe_auth_negative_count, "single_categories")
non_europe_counts = non_europe_counts.join(non_europe_auth_negative_count, "single_categories")

# Europe with auth count, negative count, all reviews count, ratio of negative, ratio of authentic
print('=========== Europe/Non-Europe with count of authentic reviews, all reviews, negative reviews and relevant ratios ==========')
europe_counts = europe_counts.withColumn('ratio of negative auth (%)', (europe_counts.negative_count/europe_counts.auth_count)*100)
non_europe_counts = non_europe_counts.withColumn('ratio of negative auth (%)', (non_europe_counts.negative_count/non_europe_counts.auth_count)*100)
print('-=============-')
print('|===EUROPE====|')
print('-=============-')
europe_counts.show()
print('-=================-')
print('|===NON-EUROPE====|')
print('-=================-')
non_europe_counts.show()

# Is there correlation between authentic reviews and negative star reviews?
print('========= CORRELATION ==========')
all_together_europe = europe_counts.join(avg_stars_europe, 'single_categories')
all_together_non_europe = non_europe_counts.join(avg_stars_non_europe, 'single_categories')

print('-=============-')
print('|===EUROPE====|')
print('-=============-')
print(f"Ratio: {all_together_europe.stat.corr('ratio (%)', 'avg(stars)')}")
print('-=================-')
print('|===NON-EUROPE====|')
print('-=================-')
print(f"Ratio: {all_together_non_europe.stat.corr('ratio (%)', 'avg(stars)')}")



