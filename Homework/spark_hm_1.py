from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc

# Creating SparkSession
spark = SparkSession.builder \
    .appName("Spark Fundamentals Week") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

# Loading data
match_details = spark.read.csv("match_details.csv", header=True, inferSchema=True)
matches = spark.read.csv("matches.csv", header=True, inferSchema=True)
medals_matches_players = spark.read.csv("medals_matches_players.csv", header=True, inferSchema=True)
medals = spark.read.csv("medals.csv", header=True, inferSchema=True)

# Marking small tables for broadcast join
medals_broadcast = medals.hint("broadcast")
matches_broadcast = matches.hint("broadcast")

# Bucketing large tables (16 buckets)
match_details.write.bucketBy(16, "match_id").sortBy("match_id").saveAsTable("bucketed_match_details")
matches.write.bucketBy(16, "match_id").sortBy("match_id").saveAsTable("bucketed_matches")
medals_matches_players.write.bucketBy(16, "match_id").sortBy("match_id").saveAsTable("bucketed_medals_matches_players")

# Reading back bucketed tables
bucketed_match_details = spark.read.table("bucketed_match_details")
bucketed_matches = spark.read.table("bucketed_matches")
bucketed_medals_matches_players = spark.read.table("bucketed_medals_matches_players")

# Joining tables
joined_data = bucketed_match_details \
    .join(matches_broadcast, "match_id", "inner") \
    .join(bucketed_medals_matches_players, ["match_id", "player_id"], "inner") \
    .join(medals_broadcast, "medal_id", "inner")

# Query 1: Which player has the highest average kills per game?
player_kills_avg = joined_data \
    .groupBy("player_id") \
    .agg(avg("kills").alias("avg_kills")) \
    .orderBy(desc("avg_kills"))

# Query 2: Which playlist is played the most?
most_played_playlist = joined_data \
    .groupBy("playlist") \
    .agg(count("match_id").alias("match_count")) \
    .orderBy(desc("match_count"))

# Query 3: Which map is played the most?
most_played_map = joined_data \
    .groupBy("map") \
    .agg(count("match_id").alias("match_count")) \
    .orderBy(desc("match_count"))

# Query 4: Which map has the most Killing Spree medals?
killing_spree_map = joined_data \
    .filter(col("medal_name") == "Killing Spree") \
    .groupBy("map") \
    .agg(count("medal_id").alias("killing_spree_count")) \
    .orderBy(desc("killing_spree_count"))

# Optimization with Partitioning and Sorting
partitioned_by_playlist = joined_data.repartition(4, "playlist").sortWithinPartitions("playlist")
partitioned_by_map = joined_data.repartition(4, "map").sortWithinPartitions("map")
partitioned_default = joined_data.repartition(4)

# Writing partitioned data to Parquet
partitioned_by_playlist.write.mode("overwrite").parquet("output/partitioned_by_playlist")
partitioned_by_map.write.mode("overwrite").parquet("output/partitioned_by_map")
partitioned_default.write.mode("overwrite").parquet("output/partitioned_default")

# Displaying results
print("Player with the most average kills per game:")
player_kills_avg.show(10)

print("Most played playlist:")
most_played_playlist.show(10)

print("Most played map:")
most_played_map.show(10)

print("Maps with the most Killing Spree medals:")
killing_spree_map.show(10)

# Stopping Spark session
spark.stop()
