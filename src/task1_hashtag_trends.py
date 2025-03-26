from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load posts.csv into a DataFrame
posts_df = spark.read.option("header", True).csv("input/posts.csv")

# Explode the hashtags column to create separate rows for each hashtag
hashtags_df = posts_df.select(explode(split(col("Hashtags"), ",")).alias("Hashtag"))

# Count occurrences of each hashtag
hashtag_counts = hashtags_df.groupBy("Hashtag").count().orderBy(col("count").desc())

# Show top 10 hashtags
hashtag_counts.show(10)

# Save the output as a CSV file
hashtag_counts.coalesce(1).write.option("header", True).csv("output/hashtag_trends.csv")

print("✅ Hashtag trends analysis completed! Results saved in 'output/hashtag_trends.csv'")

# Stop Spark session
spark.stop()
