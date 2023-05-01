#Grace Meredith
#CS433 HW2 Q1
#1 May 2023

#Find top 10 mentions used in training set

#Library to find running spark session from any working directory
import findspark
findspark.init("/opt/spark")

import pyspark
import pyspark.sql.functions
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import col, explode, split, desc

sc =SparkContext()
spark = SparkSession.builder.appName("DataFrame").getOrCreate()

#Read training_set_tweets and split data into columns
df = spark.read.text("training_set_tweets.txt")
split_cols = pyspark.sql.functions.split(df['value'], "\t")
df1 = df.withColumn('userID', split_cols.getItem(0)) \
    .withColumn('tweetID', split_cols.getItem(1)) \
    .withColumn('tweet', split_cols.getItem(2)) \
    .withColumn('date', split_cols.getItem(3))

#split "tweets" column into columns of words
tweets = df1.select(explode(split(col("tweet"), " ")).alias("mention"))

#filter by words that start with "@" to indicate a mention
mentions = tweets.filter(col("mention").startswith("@")).groupBy("mention")

#count repeated mentions and sort it from max-min, only save the top ten
top_ten = mentions.count().sort(desc("count")).limit(11) #I limited to 11, because the 'top' mention is just an @ on its own. I removed this top result when copying it to the text file.
top_ten.show()
