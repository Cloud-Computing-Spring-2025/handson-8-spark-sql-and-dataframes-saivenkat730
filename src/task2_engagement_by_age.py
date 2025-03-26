from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round

# Initialize Spark session
spark = SparkSession.builder.appName("EngagementByAgeGroup").getOrCreate()

# Load datasets
posts_df = spark.read.csv("input/posts.csv", header=True, inferSchema=True)
users_df = spark.read.csv("input/users.csv", header=True, inferSchema=True)

# Join posts and users on UserID
joined_df = posts_df.join(users_df, "UserID")

# Group by AgeGroup and calculate average Likes and Retweets, rounding to 1 decimal place
engagement_df = joined_df.groupBy("AgeGroup").agg(
    round(avg("Likes"), 1).alias("Avg_Likes"),
    round(avg("Retweets"), 1).alias("Avg_Retweets")
)

# Sort by engagement
engagement_df = engagement_df.orderBy(col("Avg_Likes").desc())

# Show results
engagement_df.show()

# Save output to CSV
engagement_df.coalesce(1).write.csv("output/engagement_by_age", header=True, mode="overwrite")

print("✅ Task 2: Engagement by Age Group analysis completed!")
