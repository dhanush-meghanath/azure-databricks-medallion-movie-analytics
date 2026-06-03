# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS silver_movies;

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import input_file_name, current_date

# STEP 1: DEFINE THE BLUEPRINT (Schema Definition)
movies_schema = StructType([
    StructField("movieId", IntegerType(), True), 
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True)
])

# STEP 2: INGESTION (Using .schema() here!)
# NOTE: Make sure the storage_account_name variable is defined in your workspace!
raw_movies_path = f"abfss://movies@devdhanushmadesa.dfs.core.windows.net/raw_movies/"

raw_movies_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(movies_schema) \
    .load(raw_movies_path)

# STEP 3: TRANSFORMATIONS & METADATA ENRICHMENT
transformed_movies_df = raw_movies_df \
    .withColumnRenamed('movieId', 'movie_id') \
    .withColumn('ingested_date', current_date()) \
    .withColumn('file_name', input_file_name()) \
    .dropDuplicates(['movie_id'])  # Kept exactly yours, just updated to the renamed column name


# STEP 4: WRITE TO SILVER LAYER
transformed_movies_df.write \
    .mode('overwrite') \
    .format('delta') \
    .saveAsTable('silver_movies.movies')

print("🎉 Movies Silver table successfully built and saved with strict schema enforcement!")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType
from pyspark.sql.functions import input_file_name, current_date


# STEP 1: DEFINE THE BLUEPRINT (Schema Definition)
ratings_schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])


# STEP 2: INGESTION (Using .schema() here!)
raw_ratings_path = "abfss://movies@devdhanushmadesa.dfs.core.windows.net/raw_ratings/"
raw_ratings_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(ratings_schema) \
    .load(raw_ratings_path)


# STEP 3: TRANSFORMATIONS & METADATA ENRICHMENT (Dropping Timestamp)
transformed_ratings_df = raw_ratings_df \
    .withColumnRenamed('userId', 'user_id') \
    .withColumnRenamed('movieId', 'movie_id') \
    .withColumn('ingested_date', current_date()) \
    .withColumn('file_name', input_file_name()) \
    .dropDuplicates(['user_id', 'movie_id']) \
    .drop('timestamp')


# STEP 4: WRITE TO SILVER LAYER
transformed_ratings_df.write \
    .mode('overwrite') \
    .format('delta') \
    .option("overwriteSchema", "true") \
    .saveAsTable('silver_movies.ratings')

print("🎉 Ratings Silver table successfully built and saved without the timestamp column!")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType
from pyspark.sql.functions import input_file_name, current_date

# STEP 1: DEFINE THE BLUEPRINT (Schema Definition)
tags_schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("tag", StringType(), True),
    StructField("timestamp", LongType(), True)
])

# STEP 2: INGESTION (Using .schema() here!)
raw_tags_path = "abfss://movies@devdhanushmadesa.dfs.core.windows.net/raw_tags/"
raw_tags_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(tags_schema) \
    .load(raw_tags_path)

# STEP 3: TRANSFORMATIONS & METADATA ENRICHMENT (Dropping Timestamp)
transformed_tags_df = raw_tags_df \
    .withColumnRenamed('userId', 'user_id') \
    .withColumnRenamed('movieId', 'movie_id') \
    .withColumn('ingested_date', current_date()) \
    .withColumn('file_name', input_file_name()) \
    .dropDuplicates(['user_id', 'movie_id', 'tag']) \
    .drop('timestamp')


# STEP 4: WRITE TO SILVER LAYER
transformed_tags_df.write \
    .mode('overwrite') \
    .format('delta') \
    .option("overwriteSchema", "true") \
    .saveAsTable('silver_movies.tags')

print("🎉 Tags Silver table successfully built and saved without the timestamp column!")