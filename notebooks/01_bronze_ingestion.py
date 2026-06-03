# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS bronze_movies;

# COMMAND ----------

# 1. BRONZE - MOVIES
raw_movies_path = "abfss://movies@devdhanushmadesa.dfs.core.windows.net/raw_movies/"

bronze_movies_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(raw_movies_path)

bronze_movies_df.write \
    .mode('overwrite') \
    .format('delta') \
    .saveAsTable('bronze_movies.raw_movies')

# COMMAND ----------

# 2. BRONZE - RATINGS
raw_ratings_path = "abfss://movies@devdhanushmadesa.dfs.core.windows.net/raw_ratings/"

bronze_ratings_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(raw_ratings_path)

bronze_ratings_df.write \
    .mode('overwrite') \
    .format('delta') \
    .saveAsTable('bronze_movies.raw_ratings')

# COMMAND ----------

# 3. BRONZE - TAGS
raw_tags_path = "abfss://movies@devdhanushmadesa.dfs.core.windows.net/raw_tags/"

bronze_tags_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(raw_tags_path)

bronze_tags_df.write \
    .mode('overwrite') \
    .format('delta') \
    .saveAsTable('bronze_movies.raw_tags')

print("All 3 Raw datasets successfully copied and archived into the Bronze Database!")

# COMMAND ----------

