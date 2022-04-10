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

The answer for this query was `2.58 %` which doesn't seem like a lot (compared to the article where he had `7 %`), but given the fact that we have around `6.5 million` reviews it still serves like a good data set.

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
The hypothesis proposed in the article is that authenticity language is used to describe different characteristics for different cuisines (and by extension, makes it harder for non-white restaurant owners to enter the higher-end restaurant market). I will attempt to identify a difference in the relationship between authenticity language and typically negative words (dirty, cheap, rude)  in some of non-european cuisine compared to some of more european like cuisines. Doing so I will try to confirm the findings of the article that 85 % of Yelp reviewers connotes “authentic” with characteristics such as dirt floors, plastic stools, and other patrons who are non-white when reviewing non-European restaurants. 

The researcher in the article narrowed his data collection to reviews from New York, and focused his field even further by picking from Zagat’s top ten most popular cuisines in New York City: Mexican, Thai, Japanese, Chinese, French, Italian, Korean, Indian, Mediterranean and Soul food. I expanded this list by also looking into more south american cuisines (Argentine etc.) and more asian cuisines (Burmese, Vietnamese etc.) 

I initially looked into the average price range for these different cuisines, to see if the negative stereotypes are also reflected here. Because if non-european restaurants want to be seen as `authentic` for western society it means, that their prices should be generally lower than euopean cuisines, which leaves owners in trap of not being able to increase their prices to the same level as european cuisines charge.

### Queries
```
-===========================-
|===EUROPE-LIKE-CUISINES====|
-===========================-
+--------------------+------------------+                                                                                                  │
|   single_categories|  avg(price_range)|                                                                                                  │
+--------------------+------------------+                                                                                                  │
|     Modern European| 2.219686626357194|                                                                                                  │
|            Austrian| 2.141304347826087|                                                                                                  │
|             Italian| 2.052350917726905|                                                                                                  │
|              French|2.5191470380031613|                                                                                                  │
|American (Traditi...| 1.913789836466243|                                                                                                  │
|    Eastern European|               2.0|                                                                                                  │
|      American (New)|2.0853369370906725|                                                                                                  │
+--------------------+------------------+
-=================-
|===NON-EUROPE====|
-=================-
+-------------------+------------------+
|  single_categories|  avg(price_range)|
+-------------------+------------------+
|            Mexican|1.6148022661351538|
|            Turkish|1.8679124536029694|
|               Thai|1.8210390892635422|
|             Indian| 1.840243855778183|
|            Chinese|1.6969313680224414|
|      South African| 1.904635761589404|
|         Indonesian| 1.944831959416614|
|            African|  2.05050787438263|
|          Mongolian|1.9854402192531688|
|          Taiwanese| 1.494803827313098|
|          Argentine| 2.043364681295716|
|           Peruvian|1.6706760680215678|
|           Japanese| 2.013904873063017|
|     Latin American|1.8560295324036096|
|           Filipino|1.4862918859041816|
|         Vietnamese|1.4355559046635846|
|          Pakistani|1.8790112297048291|
|            Burmese|1.4759152215799614|
|New Mexican Cuisine|2.0041539462489366|
|    Persian/Iranian|1.8462052081014912|
+-------------------+------------------+
```

Comparing the two tables above, we can see that european cuisines are on average more expensive that non-european cuisines, which confirms the negative stereotype of what is considered authentic.

As a next test I examined how much people tend to use authentic language (words such as `authentic, legitimate`) in the individual cuisines, and how many of those reviews contain negative stereotypes, biases and racism. Here I made a strong assumption that a negative use of authentic language occurs whenever the review with an authentic language also contains generally negative words such as: `dirty, kitch, cheap, simple, rude...`. (This method is obviously very simple, since to discover such reviews, one must also know the context in which the negative words are used).

Here are the results:

```
-=============-
|===EUROPE====|
-=============-
+--------------------+----------+------------+------------------+--------------+--------------------------+
|   single_categories|auth_count|review_count|         ratio (%)|negative_count|ratio of negative auth (%)|
+--------------------+----------+------------+------------------+--------------+--------------------------+
|     Modern European|       412|       15546|2.6501994082078992|            47|        11.407766990291263|
|            Austrian|        31|         563| 5.506216696269982|             5|        16.129032258064516|
|             Italian|     11731|      392125| 2.991648071405802|          1093|         9.317193760122752|
|              French|      1916|      103875|1.8445246690734056|           165|          8.61169102296451|
|American (Traditi...|      4027|      733103|0.5493088965670581|           471|        11.696051651353365|
|    Eastern European|        10|          89|11.235955056179774|             3|                      30.0|
|      American (New)|      3991|      729264|0.5472640909190636|           421|         10.54873465296918|
+--------------------+----------+------------+------------------+--------------+--------------------------+

-=================-
|===NON-EUROPE====|
-=================-
+-------------------+----------+------------+------------------+--------------+--------------------------+
|  single_categories|auth_count|review_count|         ratio (%)|negative_count|ratio of negative auth (%)|
+-------------------+----------+------------+------------------+--------------+--------------------------+
|            Mexican|     25832|      401693| 6.430781716385399|          2719|        10.525704552493032|
|            Turkish|       469|        7888| 5.945740365111562|            28|         5.970149253731343|
|               Thai|      9012|      134965| 6.677286703960286|           782|         8.677319130048824|
|             Indian|      5724|       79867|  7.16691499618115|           489|         8.542976939203355|
|            Chinese|     14493|      261527| 5.541684032623782|          1989|          13.7238666942662|
|         Indonesian|       182|        1621|11.227637260950031|            12|         6.593406593406594|
|            African|       276|       10875|2.5379310344827584|            22|         7.971014492753622|
|          Mongolian|        79|        5876|1.3444520081688223|             8|        10.126582278481013|
|          Taiwanese|      1582|       22432| 7.052425106990014|           195|        12.326169405815424|
|          Argentine|       142|        3891|3.6494474428167565|             5|        3.5211267605633805|
|           Peruvian|       573|        7364| 7.781097229766432|            35|         6.108202443280978|
|           Japanese|      9343|      309510| 3.018642370198055|          1118|         11.96617788718827|
|     Latin American|      3879|       54212| 7.155242381760496|           330|         8.507347254447023|
|           Filipino|       820|       14815|5.5349308133648325|            83|        10.121951219512196|
|         Vietnamese|      5056|       90091| 5.612103317756491|           604|         11.94620253164557|
|          Pakistani|      2168|       29198|7.4251661072676205|           218|        10.055350553505535|
|            Burmese|        62|         544|11.397058823529411|             9|        14.516129032258066|
|New Mexican Cuisine|       723|       20053|3.6054455692415095|            70|         9.681881051175658|
|    Persian/Iranian|       511|        9138| 5.592033267673451|            38|         7.436399217221135|
|        Bangladeshi|        68|         643| 10.57542768273717|             6|         8.823529411764707|
+-------------------+----------+------------+------------------+--------------+--------------------------+
```
Looking at the ratio of the usage of authentic language, we can see that on average people use non-european restaurants are in the lead. That makes sense, since I believe that western society is more prone to look for `authenticity` in places that are foreign to them, which can also be very problematic. Furthermore looking into the ratio of how many of those authentic reviews were also negative 

The article also states that according to the research the restaurants most impacted by this difference serve Mexican and Chinese food which I can slighlty confirm based on the results.

For example lets consider an example: `This austrian restaurant was soo authentic, really made me feel like I was in the middle of Vienna. Plus their coffee was very cheap compared to the usual prices in the city`. We can see that this review is definitely positive, but since it contains the word `authentic` and also `cheap` we count it as a negative example of usage of `authenticity language`. Whereas review such as `This mexican restuarant with its cheap plastic tables reminded me of times when I visited Mexico for holidays`. We can see that even though, these two examples are basically in the same category one reflects the negative biased stereotypes that people have, whereas the other doesn't. So there are definitely a lot of flaws in this method. The method also wouldn't catch a review for a French restaurant such as: `Old elegance at its best! Yes, the ambiance is lovely with all the fresh flowers` which also talks about authenticity but without directly stating the word `authentic`. (and we can imagine, that there are thousands of examples where people talk about south-american or south-asian cuisines using negative authentic language but not stating the words directly).
