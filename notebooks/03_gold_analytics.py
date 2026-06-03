# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gold_movies;

# COMMAND ----------

# MAGIC %md
# MAGIC ## # **Problems 1 & 2**

# COMMAND ----------

from pyspark.sql.functions import year, count, desc

ratings_df = spark.table("silver_movies.ratings")

# 1. Show the aggregated number of ratings per year
ratings_per_year_df = ratings_df.groupBy(year("ingested_date").alias("rating_year")) \
    .agg(count("*").alias("total_ratings")) \
    .orderBy("rating_year")

ratings_per_year_df.write.format("delta").mode("overwrite").saveAsTable("gold_movies.ratings_per_year")

# 2. Show the rating levels distribution
ratings_levels_df = ratings_df.groupBy("rating").agg(count("*").alias("rating_count")).orderBy(desc("rating"))

ratings_levels_df.write.format("delta").mode("overwrite").saveAsTable("gold_movies.rating_levels_distribution")

# COMMAND ----------

# MAGIC %md **Problem 3:**
# MAGIC **Show the 18 movies that are tagged but not rated.**

# COMMAND ----------

# Step 1: Get the unique movies that have been tagged
tagged_movies_df = spark.table("silver_movies.tags").select("movie_id").distinct()

# Step 2: Get the unique movies that have been rated
rated_movies_df = spark.table("silver_movies.ratings").select("movie_id").distinct()

# Step 3: Find movies in tags but NOT in ratings using "left_anti"
tagged_not_rated_df = tagged_movies_df.join(
    rated_movies_df, 
    on="movie_id", 
    how="left_anti"
)

# Step 4: Bring in the actual movie titles so we can see what they are!
movies_metadata_df = spark.table("silver_movies.movies")

final_18_movies_df = tagged_not_rated_df.join(
    movies_metadata_df, 
    on="movie_id", 
    how="inner"
).select("movie_id", "title")

# Save to Gold
final_18_movies_df.write.format("delta").mode("overwrite").saveAsTable("gold_movies.tagged_not_rated")

# COMMAND ----------

# MAGIC %md **Problem 4:**
# MAGIC **Focusing on the rated untagged movies with more than 30 user ratings, show the top 10 movies in terms of average rating and number of ratings.**

# COMMAND ----------

from pyspark.sql.functions import count, avg, desc

tagged_movies_df = spark.table("silver_movies.tags").select("movie_id").distinct()
ratings_df = spark.table("silver_movies.ratings")

rated_untagged_df = ratings_df.join(tagged_movies_df, on="movie_id", how="left_anti")

movie_stats_df = rated_untagged_df.groupBy("movie_id").agg(
    count("*").alias("number_of_ratings"),
    avg("rating").alias("average_rating")
)
popular_un_tagged_df = movie_stats_df.filter("number_of_ratings > 30")

movies_metadata_df = spark.table("silver_movies.movies")
final_top_10 = popular_un_tagged_df.join(movies_metadata_df, on="movie_id", how="inner") \
    .orderBy(desc("average_rating"), desc("number_of_ratings")) \
    .limit(10) \
    .select("title", "average_rating", "number_of_ratings")

# Save to Gold
final_top_10.write.format("delta").mode("overwrite").saveAsTable("gold_movies.top_10_rated_untagged")

# COMMAND ----------

# MAGIC %md **Problem 5:**
# MAGIC **What is the average number of tags per movie in tags? And the average number of tags per user? How does it compare with the average number of tags a user assigns to a movie?**

# COMMAND ----------

from pyspark.sql.functions import count, avg, lit

# A. Count tags per movie
movie_counts = spark.table("silver_movies.tags") \
    .groupBy("movie_id") \
    .agg(count("*").alias("tag_count"))

# B. Count tags per user
user_counts = spark.table("silver_movies.tags") \
    .groupBy("user_id") \
    .agg(count("*").alias("tag_count"))

# C. Count tags per user-movie combo
pair_counts = spark.table("silver_movies.tags") \
    .groupBy("user_id", "movie_id") \
    .agg(count("*").alias("tag_count"))

# Calculate the averages and add labels
res1 = movie_counts.agg(avg("tag_count").alias("Value")) \
    .select(lit("Avg Tags per Movie").alias("Metric"), "Value")

res2 = user_counts.agg(avg("tag_count").alias("Value")) \
    .select(lit("Avg Tags per User").alias("Metric"), "Value")

res3 = pair_counts.agg(avg("tag_count").alias("Value")) \
    .select(lit("Avg Tags per User per Movie").alias("Metric"), "Value")

# Stack them together
final_summary_df = res1.union(res2).union(res3)

# Save to Gold
final_summary_df.write.format("delta").mode("overwrite").saveAsTable("gold_movies.tag_density_summary")

# COMMAND ----------

# MAGIC %md **Problem 6:**
# MAGIC **Identify the users that tagged movies without rating them.**

# COMMAND ----------

# Loaded base tables explicitly to ensure standalone compatibility
tags_df = spark.table("silver_movies.tags")
ratings_df = spark.table("silver_movies.ratings")

# Step 1: Perform the left-anti join to find users in tags but NOT in ratings
untagged_users_df = tags_df.join(ratings_df, on="user_id", how="left_anti")

# Step 2: Clean up the output to show just a unique list of user IDs
final_users_df = untagged_users_df.select("user_id").distinct()

# Save to Gold
final_users_df.write.format("delta").mode("overwrite").saveAsTable("gold_movies.users_tagged_not_rated")

# COMMAND ----------

# MAGIC %md **Problem 7:**
# MAGIC **What is the predominant (frequency based) genre per rating level?**

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, split, explode, count, desc, col 

movies_df = spark.table("silver_movies.movies") 
ratings_df = spark.table("silver_movies.ratings")

split_movies_df = movies_df.select("movie_id", "title", split("genres", r"\|").alias("genres_array"))
exploded_movies_df = split_movies_df.select("movie_id", "title", explode("genres_array").alias("individual_genre"))

genre_with_ratings_df = ratings_df.join(exploded_movies_df, on="movie_id", how="inner")
genre_counts_df = genre_with_ratings_df.groupBy("rating", "individual_genre").agg(count("*").alias("count"))

window_spec = Window.partitionBy("rating").orderBy(desc("count"))

final_df = genre_counts_df.withColumn("row_number", row_number().over(window_spec)) \
    .filter("row_number == 1") \
    .select("rating", col("individual_genre").alias("predominant_genre"), "count")

# Save to Gold
final_df.write.format("delta").mode("overwrite").saveAsTable("gold_movies.predominant_genre_per_rating")

# COMMAND ----------

# MAGIC %md **Problem 8:**
# MAGIC **What is the predominant tag per genre and the most tagged genres?**

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, count, desc, col 

tagged_df = spark.table("silver_movies.tags")

tag_genre_joined_df = exploded_movies_df.join(tagged_df, on="movie_id", how="inner")
tag_counts_df = tag_genre_joined_df.groupBy("individual_genre", "tag").agg(count("*").alias("count"))

window_spec = Window.partitionBy("individual_genre").orderBy(desc("count"))

predominant_tag_per_genre = tag_counts_df.withColumn("row_number", row_number().over(window_spec)) \
    .filter("row_number == 1") \
    .select(col("individual_genre").alias("genre"), col("tag").alias("predominant_tag"), "count")

# Save Part 1 to Gold
predominant_tag_per_genre.write.format("delta").mode("overwrite").saveAsTable("gold_movies.predominant_tag_per_genre")

most_tagged_genres_df = tag_genre_joined_df.groupBy("individual_genre") \
    .agg(count("*").alias("total_tags")) \
    .orderBy(desc("total_tags"))

# Save Part 2 to Gold
most_tagged_genres_df.write.format("delta").mode("overwrite").saveAsTable("gold_movies.most_tagged_genres")

# COMMAND ----------

# MAGIC %md **Problem 9:**
# MAGIC **What are the most predominant (popularity based) movies?**

# COMMAND ----------

from pyspark.sql.functions import count, desc

movies_df = spark.table("silver_movies.movies")
ratings_df = spark.table("silver_movies.ratings")

joined_df = movies_df.join(ratings_df, on="movie_id", how="inner")
ratings_per_movie_df = joined_df.groupBy("movie_id","title").agg(count("*").alias("count")).orderBy(desc("count"))

# Save to Gold
ratings_per_movie_df.write.format("delta").mode("overwrite").saveAsTable("gold_movies.popular_movies")

# COMMAND ----------

# MAGIC %md **Problem 10:**
# MAGIC **Top 10 movies in terms of average rating (provided more than 30 users reviewed them)**

# COMMAND ----------

from pyspark.sql.functions import count, desc, avg

movies_df = spark.table("silver_movies.movies")
ratings_df = spark.table("silver_movies.ratings")

joined_df = ratings_df.join(movies_df, on="movie_id", how="inner")

aggregated_df = joined_df.groupBy("movie_id","title").agg(avg("rating").alias("average_ratings"),count("*").alias("total_reviewers"))
filtered_df = aggregated_df.filter("total_reviewers>30")

final_df = filtered_df.select("movie_id","title","average_ratings").orderBy(desc("average_ratings")).limit(10)

# Save to Gold
final_df.write.format("delta").mode("overwrite").saveAsTable("gold_movies.top_rated_movies")