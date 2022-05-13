import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import countDistinct, col, explode, split

conf = SparkConf()
conf.set("spark.executor.memory", "2G")
conf.set("spark.executor.instances", "4")

spark = SparkSession.builder \
	            .appName('template-application') \
		    .config(conf=conf) \
		    .getOrCreate()

bs = spark.read.json("/datasets/yelp/business.json")
rs = spark.read.json ("/datasets/yelp/review.json")

# What is the percentage of reviews containing a variant of the word 'authentic'?
print('-----What is the percentage of reviews containing a variant of the word "authentic"------')
restaurants = bs[bs.categories.contains('Restaurants')]
res_bus = restaurants.join(rs, "business_id")

# Find reviews that have a variant of the word "authentic"
#res_bus_auth = res_bus.filter(res_bus.text.rlike('[Aa]uthentic[a-z]*'))
#res_bus_auth_percentage = (res_bus_auth.count()/res_bus.count())*100

#print(f"Percentage of authentic reviews out of all restaurants reviews {res_bus_auth_percentage} %")

#How many reviews contain the string "legitimate" grouped by businesses type (type of cuisine)?

# Explode columns - since 'categories' is a long string of multiple categories for each row
# we first split all the categories to get a list of categories for each row
# then we explode it, what it does is that it basically creates a row for each of the items in the list
# For example if we have a row('James', ['Python', 'Java']) we will get
# row('James', 'Python'), row('James', 'Java')

print('======= How many reviews contain "legitimate" grouped by restaurant categories=======')
#res_bus_exploded = res_bus.withColumn('single_categories', explode(split(col('categories'), ', ')))
#Group them by the single categories and count how many of each single category are there
#res_bus_grouped = res_bus_exploded.groupBy("single_categories").count()
#res_bus_grouped = res_bus_grouped.withColumnRenamed("count", "all_count")


# Now lets do the same but only with the ones containing the word 'legitimate'

# Reviews containing the word 'legitimate'
#res_bus_legitimate = res_bus.filter(res_bus.text.contains("legitimate"))

#rs_bus_legitimate = rs_legitimate.join(bs, "business_id")
#res_bus_legit_exploded = res_bus_legitimate.withColumn('single_categories', explode(split(col('categories'), ', ')))
#res_bus_legit_grouped = res_bus_legit_exploded.groupBy("single_categories").count()
#res_bus_legit_grouped = res_bus_legit_grouped.withColumnRenamed("count", "legit_count")

# Normalize by the count - so you know what percentage of the category contains the word legitimate
#legit_ratio = res_bus_legit_grouped.join(res_bus_grouped, "single_categories")
#legit_ratio = legit_ratio.withColumn('ratio', legit_ratio.legit_count/legit_ratio.all_count)
#legit_ratio.sort('all_count', ascending=False).show()

# How many reviews contain the string "legitimate" groupedby restaurant categories?
#cuisines = ['Chinese', 'Japanese', 'Korean', 'Mexican', 'Eastern European', 'American', 'French', 'Italian', 'Greek', 'Indian']
#legit_ratio.filter(legit_ratio.single_categories.isin(cuisines)).sort('ratio', ascending=False).show()

# Is there a difference in the amount of authenticity language used in the different areas?
print('========== Difference in the amount of authenticty language used in different areas ========')
res_bus = res_bus.withColumn("auth_lang", res_bus.text.rlike('([Ll]egitimate)|([Aa]uthentic[a-z]*)'))

#The cube function “takes a list of columns and applies aggregate expressions to all possible combinations of the grouping columns”
res_bus_cube = res_bus.cube("state", "city", "auth_lang").count().orderBy("count", ascending=False)
res_bus_cube_states = res_bus_cube.filter(res_bus_cube.auth_lang.isNull()).filter(res_bus_cube.city.isNull()).withColumnRenamed("count", "reviews_count").drop("city","auth_lang")
res_bus_cube_true = res_bus_cube.filter(res_bus_cube["auth_lang"] == True).filter(res_bus_cube.city.isNull()).withColumnRenamed("count", "auth_count").withColumnRenamed("state","state2").drop("city","auth_lang")

together = res_bus_cube_states.join(res_bus_cube_true,res_bus_cube_states.state ==  res_bus_cube_true.state2,"inner").drop('state2')
together = together.withColumn('ratio', (together.auth_count/together.reviews_count)*100).sort('ratio',ascending=False)
together.show()
