from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when, round

# Initialize Spark session
spark = SparkSession.builder.appName("SentimentVsEngagement").getOrCreate()

# Load dataset
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)

# Categorize sentiment into Positive, Neutral, and Negative
sentiment_df = posts_df.withColumn(
    "Sentiment",
    when(col("SentimentScore") > 0.2, "Positive")
    .when(col("SentimentScore") < -0.2, "Negative")
    .otherwise("Neutral")
)

# Calculate the average Likes and Retweets, rounded to 1 decimal place
sentiment_engagement = sentiment_df.groupBy("Sentiment").agg(
    round(avg("Likes"), 1).alias("Avg Likes"),
    round(avg("Retweets"), 1).alias("Avg Retweets")
)

# Show results
sentiment_engagement.show()

# Save output to CSV
sentiment_engagement.coalesce(1).write.csv("output/sentiment_vs_engagement", header=True, mode="overwrite")

print("✅ Task 3: Sentiment vs Engagement analysis completed!")
