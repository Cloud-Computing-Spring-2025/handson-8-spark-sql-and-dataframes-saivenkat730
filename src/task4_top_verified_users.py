from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# Initialize Spark session
spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

# Load datasets
users_df = spark.read.csv("input/users.csv", header=True, inferSchema=True)
posts_df = spark.read.csv("input/posts.csv", header=True, inferSchema=True)

# Filter only verified users
verified_users_df = users_df.filter(col("Verified") == True)

# Compute total reach (Likes + Retweets) for each user
posts_df = posts_df.withColumn("TotalReach", col("Likes") + col("Retweets"))

# Join verified users with their post engagement
verified_engagement_df = verified_users_df.join(posts_df, "UserID", "inner")

# Aggregate total reach per verified user
top_verified_users_df = verified_engagement_df.groupBy("Username").agg(
    sum("TotalReach").alias("Total_Reach")
)

# Get top 5 verified users by total reach
top_verified_users_df = top_verified_users_df.orderBy(col("Total_Reach").desc()).limit(5)

# Show results
top_verified_users_df.show()

# Save output to CSV
top_verified_users_df.coalesce(1).write.csv("output/top_verified_users", header=True, mode="overwrite")

print("✅ Task 4: Top Verified Users analysis completed!")
