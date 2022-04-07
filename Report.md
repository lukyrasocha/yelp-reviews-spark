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
res_bus = restaurants.join(rs, "business_id")

# Find reviews that have a variant of the word "authentic"
res_bus_auth = res_bus.filter(res_bus.text.rlike('[Aa]uthentic[a-z]*'))
res_bus_auth_percentage = (res_bus_auth.count()/res_bus.count())*100

print(f"Percentage of authentic reviews out of all restaurants reviews {res_bus_auth_percentage} %")
```


