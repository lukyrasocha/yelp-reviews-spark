You can view this markdown on this url: https://github.com/lukyrasocha/yelp-reviews-spark/blob/main/Report.md

# Structure
The assignment consists of two sections: In the first I will create specific queries using Spark DataFrames, while in the second I will answer high-level questions by querying the data and reasoning the results.

- 3.1 Specific Queries
- 3.2 Authenticity Study
  - 3.2.1 Data Exploration (Answer questions from the assignment)
  - 3.2.2 Hypothesis Testing

# 3.1 Specific Queries

Load the data

```python
bs = spark.read.json("/datasets/yelp/business.json")
rs = spark.read.json ("/datasets/yelp/review.json")
us = spark.read.json("/datasets/yelp/user.json")
```

## Query 1 - Total number of reviews
```python
bs.agg({'review_count': 'sum'}).show()
```
Here each individual `business_id` has associated a `review_count` attribute, so we simply sum all of them to get the total number of reviews.

## Query 2 - Filter some businesses
```python
filtered = bs.filter(bs.stars == 5).filter(bs.review_count >= 1000).select("name","stars","review_count")
```
Here we first take only the businesses that have 5 stars and out of them we take only the ones that have been reviewed more or equal to 1000 times, then we select the specific columns of interest.

## Query 3 - Filter users that wrote more than 1000 reviews (influencers)
```python
influencers = us.filter(us.review_count > 1000).select("user_id")
```
## Query 4 - Find businesses that have been reviewed by more than 5 influencers
```python
#Filter only the reviews made by the influencers
reviews_by_influencers = rs.join(influencers, ["user_id"], 'leftsemi')

#Find the distinct counts of influencer users per business_id
gr = reviews_by_influencers.groupBy('business_id').agg(countDistinct('user_id').alias('influencer_count'))

#Take only the businesses that have more than 5 reviews from influencers
grFiltered = gr.filter(gr.influencer_count > 5)

#Find the corresponding business names
business_names = bs.join(grFiltered, bs.business_id == grFiltered.business_id, "inner").select("name")
business_names.show()
```
## Query 5 - Find an ordered list of users based on the average star count given in all their reviews
```python
avgStars = rs.groupBy('user_id') \
             .avg('stars')
             
user_names = us.join(avgStars, us.user_id == avgStars.user_id, "inner") \
               .orderBy('avg(stars)',ascending=False) \
               .select("name") 
```

# 3.2 Authenticity Study
## 3.2.1 Data Exploration
### What is the percentage of reviews containing a variant of the word 'authentic'?

Since the study is concerning only restaurants (cuisines) I first filtered only the businesses which contain `Restaurant` in their category list. (Here comes my first assumption _I assume that all cuisines have 'Restaurant' in their category list_ (some Businesses will not have 'Restaurant' associated with them but only for example 'Indian' which means that I might lose some of the relevant data)).

```python
restaurants = bs[bs.categories.contains('Restaurants')]
# Join restaurant businesses with reviews so I can filter them based on the review text
res_bus = restaurants.join(rs, "business_id")

# Find reviews that have a variant of the word "authentic"
res_bus_auth = res_bus.filter(res_bus.text.rlike('[Aa]uthentic[a-z]*'))
res_bus_auth_percentage = (res_bus_auth.count()/res_bus.count())*100

print(f"Percentage of authentic reviews out of all restaurants reviews {res_bus_auth_percentage} %")
```
I used a regular expression for the word 'authentic' to find all the reviews that contain any form of such word.
Since the [article](https://ny.eater.com/2019/1/18/18183973/authenticity-yelp-reviews-white-supremacy-trap) studies reviews that contain authenticity language, this query assures me, that in my data set, there are reviews that satisfy that condition and thus those reviews can be analysed further. 

The answer for this query was `2.58 %` which doesn't seem like a lot, but given the fact that we have around `6.5 million` reviews it still serves like a good data set.

### How many reviews contain "legitimate" grouped by restaurant categories?
```python
res_bus_exploded = res_bus.withColumn('single_categories', explode(split(col('categories'), ', ')))
#Group them by the single categories and count how many of each single category are there
res_bus_grouped = res_bus_exploded.groupBy("single_categories").count()
res_bus_grouped = res_bus_grouped.withColumnRenamed("count", "all_count")

# Now lets do the same but only with the ones containing the word 'legitimate'

# Reviews containing the word 'legitimate'
res_bus_legitimate = res_bus.filter(res_bus.text.contains("legitimate"))

res_bus_legit_exploded = res_bus_legitimate.withColumn('single_categories', explode(split(col('categories'), ', ')))
res_bus_legit_grouped = res_bus_legit_exploded.groupBy("single_categories").count()
res_bus_legit_grouped = res_bus_legit_grouped.withColumnRenamed("count", "legit_count")

# Normalize by the count - so you know what percentage of the category contains the word legitimate
legit_ratio = res_bus_legit_grouped.join(res_bus_grouped, "single_categories")
legit_ratio = legit_ratio.withColumn('ratio', legit_ratio.legit_count/legit_ratio.all_count)
legit_ratio.sort('all_count', ascending=False).show()
```
-> How does `explode()` work -> lets say that we have `row('John', ['Java', 'Scala'])` -> if we explode the list we will get -> `row('John', 'Java'), row('John', 'Scala')` which helps to separate the string of categories into seperate individual categories (so I can for example group by the individual categories later).

This query already could give some insight into the fact, that some cuisines might be more prone to authentic language than others.

```
+--------------------+-----------+---------+--------------------+
|   single_categories|legit_count|all_count|               ratio|
+--------------------+-----------+---------+--------------------+
|         Restaurants|       2250|  4201684|5.354995758843359E-4|
|                Food|        625|  1133172|5.515491028722912E-4|
|           Nightlife|        574|  1009498|5.685994424951807E-4|
|                Bars|        554|   974747|5.683526084204414E-4|
|American (Traditi...|        323|   733103|4.405929316889987E-4|
|      American (New)|        360|   729264|4.936483907062463E-4|
|  Breakfast & Brunch|        309|   646334|4.780809921805135E-4|
|          Sandwiches|        238|   475626|5.003931660590464E-4|
|             Mexican|        207|   401693|5.153189127019888E-4|
|             Burgers|        180|   395129|4.555474288143872E-4|
|               Pizza|        217|   394428|5.501637814759601E-4|
|             Italian|        209|   392125|5.329933057060886E-4|
|             Seafood|        157|   343191|4.574712040816921E-4|
|            Japanese|        166|   309510|5.363316209492424E-4|
|Event Planning & ...|        170|   298962|5.686341407938133E-4|
|             Chinese|        136|   261527|5.200227892339988E-4|
|          Sushi Bars|        137|   254215|5.389139114529041E-4|
|               Salad|        108|   249166|4.334459757751860...|
|         Steakhouses|        116|   243536|4.763156165823533E-4|
|        Asian Fusion|        130|   240279|5.410377103284099E-4|
+--------------------+-----------+---------+--------------------+

+-----------------+-----------+---------+--------------------+
|single_categories|legit_count|all_count|               ratio|
+-----------------+-----------+---------+--------------------+
|           Korean|         59|    93732|6.294541885375326E-4|
|           Indian|         46|    79867|5.759575293926152E-4|
|         Japanese|        166|   309510|5.363316209492424E-4|
|          Italian|        209|   392125|5.329933057060886E-4|
|          Chinese|        136|   261527|5.200227892339988E-4|
|          Mexican|        207|   401693|5.153189127019888E-4|
|           French|         47|   103875|4.524669073405535...|
|            Greek|         34|    75500|4.503311258278146E-4|
+-----------------+-----------+---------+--------------------+
```
The result is sorted and also an additional column `ratio` is added, that is because more reviews should generally mean more reviews of all different types (including reviews with authentic language). So to compare the different categories I used the ratio instead. 

In the second table I filtered some specific cuisines of interest where we can see that for example 'Mexican cuisine' (which was one of the main focuses of the article) has larger fraction of reviews containing 'legitimate' than 'French cuisine'.

### Is there a difference in the amount of authenticity languiage used in the different areas?

```python
res_bus = res_bus.withColumn("auth_lang", res_bus.text.rlike('([Ll]egitimate)|([Aa]uthentic[a-z]*)'))

#The cube function “takes a list of columns and applies aggregate expressions to all possible combinations of the grouping columns”
res_bus_cube = res_bus.cube("state", "city", "auth_lang").count().orderBy("count", ascending=False)
res_bus_cube.show()
```
This query returns a table in which we see all the possible combinations of state, city and false/true for authenticity language. One could go deeper and use this query to analyse if there is a certain region where people tend to use authentic language more but since that was not the focus of the article I didn't analyse this further. (also the data set itself does not have any information about the origin of the users who gave the reviews so conclusion about this would also be more difficult). To make any area-comparisson viable one would must also normalize by for example the number of restaurants in the area, the popularity of the restaurants etc.

## 3.2.2 Hypothesis testing
